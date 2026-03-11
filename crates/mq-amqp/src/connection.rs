//! AMQP 1.0 connection and session management.
//!
//! Implements the AMQP 1.0 connection lifecycle:
//! 1. Client connects, optionally sends SASL header for authentication
//! 2. SASL negotiation (PLAIN/ANONYMOUS)
//! 3. Client sends AMQP header
//! 4. Server sends Open, client sends Open
//! 5. Sessions created via Begin/End
//! 6. Links created via Attach/Detach within sessions
//!
//! Connection → Session → Link hierarchy with credit-based flow control.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Buf, Bytes, BytesMut};
use smallvec::SmallVec;
use tracing::{debug, info, warn};

use crate::codec::{self, CodecError};
use crate::link::{AmqpLink, LinkError};
use crate::types::*;

// =============================================================================
// Connection State
// =============================================================================

/// Connection lifecycle phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPhase {
    /// Waiting for client to send a protocol header (SASL or AMQP).
    AwaitingHeader,
    /// SASL negotiation in progress.
    SaslNegotiating,
    /// Waiting for client AMQP header (after SASL or directly).
    AwaitingAmqpHeader,
    /// Server has sent Open; waiting for client Open.
    AwaitingOpen,
    /// Connection is open and operational.
    Open,
    /// Connection is closing (Close sent, awaiting Close).
    Closing,
    /// Connection is closed.
    Closed,
}

/// Authentication state.
#[derive(Debug, Clone, Default)]
pub struct AuthState {
    pub mechanism: WireString,
    pub username: WireString,
    pub authenticated: bool,
}

// =============================================================================
// Session
// =============================================================================

/// An AMQP 1.0 session (created via Begin).
#[derive(Debug)]
pub struct AmqpSession {
    /// Local channel number.
    pub local_channel: u16,
    /// Remote channel number (from the peer's Begin).
    pub remote_channel: Option<u16>,
    /// Next outgoing transfer ID (monotonically increasing).
    pub next_outgoing_id: u32,
    /// Next expected incoming transfer ID.
    pub next_incoming_id: u32,
    /// Incoming window size.
    pub incoming_window: u32,
    /// Outgoing window size.
    pub outgoing_window: u32,
    /// Handle max for this session.
    pub handle_max: u32,
    /// Links by handle. SmallVec for typical 1-8 links per session.
    pub links: SmallVec<[(u32, AmqpLink); 8]>,
    /// Next available link handle.
    next_handle: u32,
}

impl AmqpSession {
    pub fn new(local_channel: u16) -> Self {
        Self {
            local_channel,
            remote_channel: None,
            next_outgoing_id: 0,
            next_incoming_id: 0,
            incoming_window: 2048,
            outgoing_window: 2048,
            handle_max: u32::MAX,
            links: SmallVec::new(),
            next_handle: 0,
        }
    }

    /// Allocate a new link handle.
    pub fn alloc_handle(&mut self) -> u32 {
        let h = self.next_handle;
        self.next_handle += 1;
        h
    }

    /// Record an outgoing transfer and return the delivery ID.
    pub fn next_delivery_id(&mut self) -> u32 {
        let id = self.next_outgoing_id;
        self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);
        id
    }

    /// Find a link by handle.
    #[inline]
    fn find_link(&self, handle: u32) -> Option<usize> {
        self.links.iter().position(|(h, _)| *h == handle)
    }

    /// Get a mutable reference to a link by handle.
    #[inline]
    fn get_link_mut(&mut self, handle: u32) -> Option<&mut AmqpLink> {
        self.links
            .iter_mut()
            .find(|(h, _)| *h == handle)
            .map(|(_, l)| l)
    }
}

// =============================================================================
// AmqpConnection
// =============================================================================

/// Global connection ID counter.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// An AMQP 1.0 TCP connection.
///
/// Manages protocol header negotiation, SASL authentication, Open/Close
/// lifecycle, and session multiplexing.
pub struct AmqpConnection {
    /// Unique connection ID.
    pub id: u64,
    /// Current lifecycle phase.
    pub phase: ConnectionPhase,
    /// Authentication state.
    pub auth: AuthState,
    /// Our container ID.
    pub container_id: WireString,
    /// Peer's container ID.
    pub peer_container_id: Option<WireString>,
    /// Negotiated max frame size.
    pub max_frame_size: u32,
    /// Negotiated channel max.
    pub channel_max: u16,
    /// Idle timeout in milliseconds (0 = disabled).
    pub idle_timeout: u32,
    /// Sessions indexed by local channel number. SmallVec for typical 1-4 sessions.
    pub sessions: SmallVec<[(u16, AmqpSession); 4]>,
    /// Map from remote channel to local channel.
    remote_to_local_channel: SmallVec<[(u16, u16); 4]>,
    /// Next local channel to allocate.
    next_channel: u16,
    /// Read buffer for incoming data.
    pub read_buf: BytesMut,
    /// Write buffer for outgoing frames.
    pub write_buf: BytesMut,
}

impl AmqpConnection {
    /// Create a new connection in the initial state.
    pub fn new() -> Self {
        let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
            phase: ConnectionPhase::AwaitingHeader,
            auth: AuthState::default(),
            container_id: WireString::from_string(format!("bisque-mq-{}", id.wrapping_add(1))),
            peer_container_id: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            channel_max: DEFAULT_CHANNEL_MAX,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            sessions: SmallVec::new(),
            remote_to_local_channel: SmallVec::new(),
            next_channel: 0,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
        }
    }

    /// Feed incoming bytes from the TCP stream.
    #[inline]
    pub fn feed_data(&mut self, data: &[u8]) {
        self.read_buf.extend_from_slice(data);
    }

    /// Take the outgoing write buffer contents as frozen Bytes (zero-copy).
    /// Reuses the existing allocation by splitting.
    #[inline]
    pub fn take_write_bytes(&mut self) -> Bytes {
        self.write_buf.split().freeze()
    }

    /// Check if there is data pending to be written.
    #[inline]
    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.is_empty()
    }

    /// Process buffered data.
    ///
    /// Returns `Ok(true)` if the connection should continue,
    /// `Ok(false)` if the connection should be closed.
    pub fn process(&mut self) -> Result<bool, ConnectionError> {
        loop {
            match self.phase {
                ConnectionPhase::AwaitingHeader => {
                    if !self.process_header()? {
                        return Ok(true);
                    }
                }
                ConnectionPhase::SaslNegotiating => {
                    if !self.process_sasl()? {
                        return Ok(true);
                    }
                }
                ConnectionPhase::AwaitingAmqpHeader => {
                    if !self.process_amqp_header()? {
                        return Ok(true);
                    }
                }
                ConnectionPhase::AwaitingOpen
                | ConnectionPhase::Open
                | ConnectionPhase::Closing => {
                    if !self.process_frame()? {
                        return Ok(self.phase != ConnectionPhase::Closed);
                    }
                }
                ConnectionPhase::Closed => return Ok(false),
            }
        }
    }

    // =========================================================================
    // Protocol Header Processing
    // =========================================================================

    /// Process the initial 8-byte protocol header.
    fn process_header(&mut self) -> Result<bool, ConnectionError> {
        if self.read_buf.len() < 8 {
            return Ok(false);
        }

        let header: [u8; 8] = self.read_buf[..8].try_into().unwrap();

        if header[..4] != *b"AMQP" {
            self.write_buf.extend_from_slice(&AMQP_HEADER);
            self.phase = ConnectionPhase::Closed;
            return Err(ConnectionError::ProtocolMismatch(header));
        }

        let protocol_id = header[4];
        self.read_buf.advance(8);

        match protocol_id {
            3 => {
                // SASL negotiation requested.
                debug!(conn = self.id, "SASL header received");
                self.send_sasl_mechanisms();
                self.phase = ConnectionPhase::SaslNegotiating;
                Ok(true)
            }
            0 => {
                // Direct AMQP (no SASL).
                debug!(conn = self.id, "AMQP header received (no SASL)");
                self.auth.authenticated = true; // anonymous
                self.auth.mechanism = WireString::from_static("ANONYMOUS");
                self.write_buf.extend_from_slice(&AMQP_HEADER);
                self.send_open();
                self.phase = ConnectionPhase::AwaitingOpen;
                Ok(true)
            }
            _ => {
                self.write_buf.extend_from_slice(&AMQP_HEADER);
                self.phase = ConnectionPhase::Closed;
                Err(ConnectionError::ProtocolMismatch(header))
            }
        }
    }

    /// Process the AMQP header after SASL completes.
    fn process_amqp_header(&mut self) -> Result<bool, ConnectionError> {
        if self.read_buf.len() < 8 {
            return Ok(false);
        }

        let header: [u8; 8] = self.read_buf[..8].try_into().unwrap();
        if header != AMQP_HEADER {
            self.write_buf.extend_from_slice(&AMQP_HEADER);
            self.phase = ConnectionPhase::Closed;
            return Err(ConnectionError::ProtocolMismatch(header));
        }

        self.read_buf.advance(8);
        debug!(conn = self.id, "AMQP header received (post-SASL)");

        self.write_buf.extend_from_slice(&AMQP_HEADER);
        self.send_open();
        self.phase = ConnectionPhase::AwaitingOpen;
        Ok(true)
    }

    // =========================================================================
    // SASL
    // =========================================================================

    fn send_sasl_mechanisms(&mut self) {
        // Encode directly into write_buf — no intermediate BytesMut allocation.
        codec::encode_framed_sasl_mechanisms(&mut self.write_buf, &["PLAIN", "ANONYMOUS"]);
        debug!(conn = self.id, "sent sasl-mechanisms");
    }

    fn process_sasl(&mut self) -> Result<bool, ConnectionError> {
        if self.read_buf.len() < 8 {
            return Ok(false);
        }

        // Peek at frame size before consuming — avoids freeze+copy-back on Incomplete.
        let size = u32::from_be_bytes([
            self.read_buf[0],
            self.read_buf[1],
            self.read_buf[2],
            self.read_buf[3],
        ]) as usize;
        if size > self.read_buf.len() {
            return Ok(false); // incomplete frame, wait for more data
        }

        // Split exactly the frame bytes and freeze — no copy-back needed.
        let frame_bytes = self.read_buf.split_to(size).freeze();
        let (frame, _) = codec::decode_frame(&frame_bytes)?;

        match frame.body {
            FrameBody::Sasl(SaslPerformative::Init(init)) => {
                self.auth.mechanism = init.mechanism.clone();

                if init.mechanism == "PLAIN" {
                    if let Some(ref response) = init.initial_response {
                        self.parse_sasl_plain(response.clone());
                    }
                    self.auth.authenticated = true;
                } else if init.mechanism == "ANONYMOUS" {
                    self.auth.authenticated = true;
                    self.auth.username = WireString::from_static("anonymous");
                } else {
                    // Unknown mechanism — reject. Encode directly, no intermediate alloc.
                    codec::encode_framed_sasl_outcome(&mut self.write_buf, SaslCode::Auth);
                    self.phase = ConnectionPhase::Closed;
                    return Err(ConnectionError::AuthenticationFailed);
                }

                info!(
                    conn = self.id,
                    user = %self.auth.username,
                    mechanism = %self.auth.mechanism,
                    "SASL authentication succeeded"
                );

                // Send sasl-outcome(ok) directly into write_buf — no intermediate alloc.
                codec::encode_framed_sasl_outcome(&mut self.write_buf, SaslCode::Ok);

                self.phase = ConnectionPhase::AwaitingAmqpHeader;
                Ok(true)
            }
            _ => {
                warn!(conn = self.id, "unexpected SASL frame");
                Err(ConnectionError::UnexpectedFrame("expected sasl-init"))
            }
        }
    }

    fn parse_sasl_plain(&mut self, response: Bytes) {
        // SASL PLAIN format: [authzid]\0[authcid]\0[password]
        // Find the positions of null bytes without allocating a Vec
        let mut first_null = None;
        let mut second_null = None;
        for (i, &b) in response.iter().enumerate() {
            if b == 0 {
                if first_null.is_none() {
                    first_null = Some(i);
                } else {
                    second_null = Some(i);
                    break;
                }
            }
        }
        // Zero-copy: slice directly from the Bytes buffer
        if let (Some(n1), Some(n2)) = (first_null, second_null) {
            // authcid is between first and second null
            let username_bytes = response.slice(n1 + 1..n2);
            if std::str::from_utf8(&username_bytes).is_ok() {
                self.auth.username = WireString::from_utf8_unchecked(username_bytes);
            }
        } else if let Some(n1) = first_null {
            // Fallback: just one null separator
            let username_bytes = response.slice(..n1);
            if std::str::from_utf8(&username_bytes).is_ok() {
                self.auth.username = WireString::from_utf8_unchecked(username_bytes);
            }
        }
    }

    // =========================================================================
    // Connection Open/Close
    // =========================================================================

    fn send_open(&mut self) {
        let mut caps = SmallVec::new();
        caps.push(WireString::from_static("ANONYMOUS-RELAY"));
        let open = Open {
            container_id: self.container_id.clone(),
            hostname: None,
            max_frame_size: self.max_frame_size,
            channel_max: self.channel_max,
            idle_timeout: if self.idle_timeout > 0 {
                Some(self.idle_timeout)
            } else {
                None
            },
            offered_capabilities: caps,
            ..Default::default()
        };
        codec::encode_framed_performative(
            &mut self.write_buf,
            0,
            FRAME_TYPE_AMQP,
            &Performative::Open(open),
        );
        debug!(conn = self.id, "sent Open");
    }

    /// Send a connection-level error and initiate close.
    pub fn send_connection_error(&mut self, condition: &'static str, description: &'static str) {
        let close = Close {
            error: Some(AmqpError::from_static(condition, description)),
        };
        codec::encode_framed_performative(
            &mut self.write_buf,
            0,
            FRAME_TYPE_AMQP,
            &Performative::Close(close),
        );
        self.phase = ConnectionPhase::Closing;
    }

    // =========================================================================
    // Frame Processing
    // =========================================================================

    fn process_frame(&mut self) -> Result<bool, ConnectionError> {
        if self.read_buf.len() < 8 {
            return Ok(false);
        }

        // Peek at frame size before consuming — avoids freeze+copy-back on Incomplete.
        let size = u32::from_be_bytes([
            self.read_buf[0],
            self.read_buf[1],
            self.read_buf[2],
            self.read_buf[3],
        ]) as usize;
        if size > self.read_buf.len() {
            return Ok(false); // incomplete frame, wait for more data
        }

        // Split exactly the frame bytes and freeze — no copy-back needed.
        let frame_bytes = self.read_buf.split_to(size).freeze();
        let (frame, _) = codec::decode_frame(&frame_bytes)?;

        match frame.body {
            FrameBody::Empty => {
                // Heartbeat — respond with empty frame.
                codec::encode_empty_frame(&mut self.write_buf);
                Ok(true)
            }
            FrameBody::Amqp(perf) => self.dispatch_performative(frame.channel, perf),
            FrameBody::Sasl(_) => {
                warn!(conn = self.id, "unexpected SASL frame after negotiation");
                Err(ConnectionError::UnexpectedFrame("SASL frame in AMQP phase"))
            }
        }
    }

    fn dispatch_performative(
        &mut self,
        channel: u16,
        perf: Performative,
    ) -> Result<bool, ConnectionError> {
        match perf {
            Performative::Open(open) => self.handle_open(open),
            Performative::Close(close) => self.handle_close(close),
            Performative::Begin(begin) => self.handle_begin(channel, begin),
            Performative::End(end) => self.handle_end(channel, end),
            // Session-level performatives: dispatch to session.
            Performative::Attach(attach) => {
                self.handle_session_perf(channel, |s, wb| s.handle_attach(attach, wb))
            }
            Performative::Detach(detach) => {
                self.handle_session_perf(channel, |s, wb| s.handle_detach(detach, wb))
            }
            Performative::Flow(flow) => {
                self.handle_session_perf(channel, |s, wb| s.handle_flow(flow, wb))
            }
            Performative::Transfer(transfer) => {
                self.handle_session_perf(channel, |s, wb| s.handle_transfer(transfer, wb))
            }
            Performative::Disposition(disposition) => {
                self.handle_session_perf(channel, |s, wb| s.handle_disposition(disposition, wb))
            }
        }
    }

    /// Dispatch a performative to the session on the given channel.
    fn handle_session_perf<F>(&mut self, channel: u16, f: F) -> Result<bool, ConnectionError>
    where
        F: FnOnce(&mut AmqpSession, &mut BytesMut) -> Result<(), LinkError>,
    {
        let local_ch = self
            .remote_to_local_channel
            .iter()
            .find(|(r, _)| *r == channel)
            .map(|(_, l)| *l)
            .unwrap_or(channel);
        let session = self
            .sessions
            .iter_mut()
            .find(|(ch, _)| *ch == local_ch)
            .map(|(_, s)| s)
            .ok_or(ConnectionError::UnknownSession(channel))?;
        f(session, &mut self.write_buf).map_err(ConnectionError::Link)?;
        Ok(true)
    }

    // =========================================================================
    // Open / Close handlers
    // =========================================================================

    fn handle_open(&mut self, open: Open) -> Result<bool, ConnectionError> {
        if self.phase != ConnectionPhase::AwaitingOpen {
            return Err(ConnectionError::UnexpectedPhase {
                expected: ConnectionPhase::AwaitingOpen,
                got: self.phase,
            });
        }

        self.peer_container_id = Some(open.container_id.clone());

        // Negotiate parameters: take the minimum.
        if open.max_frame_size > 0 && open.max_frame_size < self.max_frame_size {
            self.max_frame_size = open.max_frame_size;
        }
        if open.channel_max < self.channel_max {
            self.channel_max = open.channel_max;
        }
        if let Some(timeout) = open.idle_timeout {
            if self.idle_timeout == 0 || (timeout > 0 && timeout < self.idle_timeout) {
                self.idle_timeout = timeout;
            }
        }

        self.phase = ConnectionPhase::Open;
        info!(
            conn = self.id,
            peer = %open.container_id,
            max_frame_size = self.max_frame_size,
            channel_max = self.channel_max,
            "connection opened"
        );
        Ok(true)
    }

    fn handle_close(&mut self, close: Close) -> Result<bool, ConnectionError> {
        if let Some(ref err) = close.error {
            info!(
                conn = self.id,
                condition = %err.condition,
                description = ?err.description,
                "peer closed connection with error"
            );
        } else {
            info!(conn = self.id, "peer closed connection");
        }

        // Send Close response directly into write buffer.
        let resp = Close { error: None };
        codec::encode_framed_performative(
            &mut self.write_buf,
            0,
            FRAME_TYPE_AMQP,
            &Performative::Close(resp),
        );

        self.phase = ConnectionPhase::Closed;
        Ok(false)
    }

    // =========================================================================
    // Session Begin / End
    // =========================================================================

    fn handle_begin(&mut self, remote_channel: u16, begin: Begin) -> Result<bool, ConnectionError> {
        if self.phase != ConnectionPhase::Open {
            return Err(ConnectionError::NotOpen);
        }

        let local_channel = self.alloc_channel();
        if local_channel > self.channel_max {
            return Err(ConnectionError::SessionLimitExceeded {
                channel: local_channel,
                max: self.channel_max,
            });
        }

        let mut session = AmqpSession::new(local_channel);
        session.remote_channel = Some(remote_channel);
        session.next_incoming_id = begin.next_outgoing_id;
        session.incoming_window = begin.outgoing_window;
        if begin.handle_max < session.handle_max {
            session.handle_max = begin.handle_max;
        }

        // Send Begin response directly into write buffer.
        let resp = Begin {
            remote_channel: Some(remote_channel),
            next_outgoing_id: session.next_outgoing_id,
            incoming_window: session.incoming_window,
            outgoing_window: session.outgoing_window,
            handle_max: session.handle_max,
            ..Default::default()
        };
        codec::encode_framed_performative(
            &mut self.write_buf,
            local_channel,
            FRAME_TYPE_AMQP,
            &Performative::Begin(resp),
        );

        self.sessions.push((local_channel, session));
        self.remote_to_local_channel
            .push((remote_channel, local_channel));

        debug!(
            conn = self.id,
            local_channel, remote_channel, "session begun"
        );
        Ok(true)
    }

    fn handle_end(&mut self, channel: u16, end: End) -> Result<bool, ConnectionError> {
        let local_ch = self
            .remote_to_local_channel
            .iter()
            .find(|(r, _)| *r == channel)
            .map(|(_, l)| *l)
            .unwrap_or(channel);

        if let Some(idx) = self.sessions.iter().position(|(ch, _)| *ch == local_ch) {
            let (_, session) = self.sessions.remove(idx);
            if let Some(remote) = session.remote_channel {
                if let Some(pos) = self
                    .remote_to_local_channel
                    .iter()
                    .position(|(r, _)| *r == remote)
                {
                    self.remote_to_local_channel.remove(pos);
                }
            }

            if let Some(ref err) = end.error {
                warn!(
                    conn = self.id,
                    channel = local_ch,
                    condition = %err.condition,
                    "session ended with error"
                );
            }

            // Send End response.
            let resp = End { error: None };
            codec::encode_framed_performative(
                &mut self.write_buf,
                local_ch,
                FRAME_TYPE_AMQP,
                &Performative::End(resp),
            );

            debug!(conn = self.id, channel = local_ch, "session ended");
        } else {
            warn!(conn = self.id, channel, "end for unknown session");
        }

        Ok(true)
    }

    /// Allocate the next available channel number.
    fn alloc_channel(&mut self) -> u16 {
        let ch = self.next_channel;
        self.next_channel += 1;
        ch
    }
}

// =============================================================================
// Session-level performative handlers
// =============================================================================

impl AmqpSession {
    pub fn handle_attach(
        &mut self,
        attach: Attach,
        write_buf: &mut BytesMut,
    ) -> Result<(), LinkError> {
        let handle = attach.handle;

        if handle > self.handle_max {
            return Err(LinkError::HandleExceedsMax {
                handle,
                max: self.handle_max,
            });
        }

        // Create a link. The peer's role is the opposite of our role.
        let our_role = match attach.role {
            Role::Sender => Role::Receiver,
            Role::Receiver => Role::Sender,
        };

        let address = match our_role {
            Role::Receiver => attach.target.as_ref().and_then(|t| t.address.clone()),
            Role::Sender => attach.source.as_ref().and_then(|s| s.address.clone()),
        };

        let link = AmqpLink::new(handle, attach.name.clone(), our_role, address);

        // Send Attach response directly into write buffer.
        let resp = Attach {
            name: attach.name,
            handle,
            role: our_role,
            snd_settle_mode: attach.snd_settle_mode,
            rcv_settle_mode: attach.rcv_settle_mode,
            source: attach.source,
            target: attach.target,
            initial_delivery_count: if our_role == Role::Sender {
                Some(0)
            } else {
                None
            },
            ..Default::default()
        };
        codec::encode_framed_performative(
            write_buf,
            self.local_channel,
            FRAME_TYPE_AMQP,
            &Performative::Attach(resp),
        );

        self.links.push((handle, link));
        debug!(
            channel = self.local_channel,
            handle,
            role = ?our_role,
            "link attached"
        );
        Ok(())
    }

    pub fn handle_detach(
        &mut self,
        detach: Detach,
        write_buf: &mut BytesMut,
    ) -> Result<(), LinkError> {
        let handle = detach.handle;

        if let Some(idx) = self.find_link(handle) {
            self.links.remove(idx);

            // Send Detach response.
            let resp = Detach {
                handle,
                closed: detach.closed,
                error: None,
            };
            codec::encode_framed_performative(
                write_buf,
                self.local_channel,
                FRAME_TYPE_AMQP,
                &Performative::Detach(resp),
            );
            debug!(channel = self.local_channel, handle, "link detached");
        } else {
            warn!(
                channel = self.local_channel,
                handle, "detach for unknown handle"
            );
        }
        Ok(())
    }

    pub fn handle_flow(&mut self, flow: Flow, write_buf: &mut BytesMut) -> Result<(), LinkError> {
        // Update session-level flow state.
        if let Some(id) = flow.next_incoming_id {
            self.next_incoming_id = id;
        }
        self.outgoing_window = flow.incoming_window;

        // If this is a link-level flow, update the link.
        if let Some(handle) = flow.handle {
            let ch = self.local_channel;
            if let Some(link) = self.get_link_mut(handle) {
                if let Some(credit) = flow.link_credit {
                    link.link_credit = credit;
                }
                if let Some(count) = flow.delivery_count {
                    link.delivery_count = count;
                }
                link.drain = flow.drain;
                debug!(
                    channel = ch,
                    handle,
                    credit = link.link_credit,
                    "flow updated for link"
                );
            }
        }

        // If echo requested, send our flow back.
        if flow.echo {
            let resp = Flow {
                next_incoming_id: Some(self.next_incoming_id),
                incoming_window: self.incoming_window,
                next_outgoing_id: self.next_outgoing_id,
                outgoing_window: self.outgoing_window,
                echo: false,
                ..Default::default()
            };
            codec::encode_framed_performative(
                write_buf,
                self.local_channel,
                FRAME_TYPE_AMQP,
                &Performative::Flow(resp),
            );
        }

        Ok(())
    }

    pub fn handle_transfer(
        &mut self,
        transfer: Transfer,
        write_buf: &mut BytesMut,
    ) -> Result<(), LinkError> {
        let handle = transfer.handle;

        // Read fields before borrowing self mutably
        let settled = transfer.settled.unwrap_or(false);
        let delivery_id = transfer.delivery_id.unwrap_or(self.next_incoming_id);
        let payload_len = transfer.payload.len();

        let link = self
            .get_link_mut(handle)
            .ok_or(LinkError::UnknownHandle(handle))?;

        link.on_transfer_received(&transfer);

        debug!(
            channel = self.local_channel,
            handle, delivery_id, settled, payload_len, "transfer received"
        );

        // For now, immediately accept all messages (auto-settle).
        if !settled {
            let disposition = Disposition {
                role: Role::Receiver,
                first: delivery_id,
                last: None,
                settled: true,
                state: Some(DeliveryState::Accepted),
                batchable: false,
            };
            codec::encode_framed_performative(
                write_buf,
                self.local_channel,
                FRAME_TYPE_AMQP,
                &Performative::Disposition(disposition),
            );
        }

        self.next_incoming_id = self.next_incoming_id.wrapping_add(1);
        Ok(())
    }

    pub fn handle_disposition(
        &mut self,
        disposition: Disposition,
        _write_buf: &mut BytesMut,
    ) -> Result<(), LinkError> {
        let last = disposition.last.unwrap_or(disposition.first);
        debug!(
            channel = self.local_channel,
            role = ?disposition.role,
            first = disposition.first,
            last,
            settled = disposition.settled,
            "disposition received"
        );
        Ok(())
    }
}

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("protocol mismatch: got {0:?}")]
    ProtocolMismatch([u8; 8]),

    #[error("codec error: {0}")]
    Codec(#[from] CodecError),

    #[error("link error: {0}")]
    Link(#[from] LinkError),

    #[error("unexpected phase: expected {expected:?}, got {got:?}")]
    UnexpectedPhase {
        expected: ConnectionPhase,
        got: ConnectionPhase,
    },

    #[error("unexpected frame: {0}")]
    UnexpectedFrame(&'static str),

    #[error("connection not open")]
    NotOpen,

    #[error("authentication failed")]
    AuthenticationFailed,

    #[error("unknown session channel {0}")]
    UnknownSession(u16),

    #[error("session channel {channel} exceeds max {max}")]
    SessionLimitExceeded { channel: u16, max: u16 },
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_new() {
        let conn = AmqpConnection::new();
        assert_eq!(conn.phase, ConnectionPhase::AwaitingHeader);
        assert!(conn.sessions.is_empty());
        assert!(!conn.auth.authenticated);
    }

    #[test]
    fn test_protocol_header_mismatch() {
        let mut conn = AmqpConnection::new();
        conn.feed_data(b"HTTP/1.1");
        let result = conn.process();
        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectionError::ProtocolMismatch(header) => {
                assert_eq!(&header, b"HTTP/1.1");
            }
            e => panic!("unexpected error: {e:?}"),
        }
    }

    #[test]
    fn test_amqp_header_accepted() {
        let mut conn = AmqpConnection::new();
        conn.feed_data(&AMQP_HEADER);
        let result = conn.process();
        assert!(result.is_ok());
        assert_eq!(conn.phase, ConnectionPhase::AwaitingOpen);
        assert!(conn.auth.authenticated);
        // Should have written AMQP header + Open frame.
        assert!(!conn.write_buf.is_empty());
    }

    #[test]
    fn test_sasl_header_starts_negotiation() {
        let mut conn = AmqpConnection::new();
        conn.feed_data(&SASL_HEADER);
        let result = conn.process();
        assert!(result.is_ok());
        assert_eq!(conn.phase, ConnectionPhase::SaslNegotiating);
        // Should have sent sasl-mechanisms frame.
        assert!(!conn.write_buf.is_empty());
    }

    #[test]
    fn test_sasl_plain_parsing() {
        let mut conn = AmqpConnection::new();
        conn.parse_sasl_plain(Bytes::from_static(b"\x00guest\x00secret"));
        assert_eq!(&*conn.auth.username, "guest");
    }

    #[test]
    fn test_sasl_plain_no_leading_null() {
        let mut conn = AmqpConnection::new();
        conn.parse_sasl_plain(Bytes::from_static(b"user\x00pass"));
        assert_eq!(&*conn.auth.username, "user");
    }

    #[test]
    fn test_full_amqp_handshake() {
        let mut conn = AmqpConnection::new();

        // Step 1: Send AMQP header (no SASL).
        conn.feed_data(&AMQP_HEADER);
        conn.process().unwrap();
        assert_eq!(conn.phase, ConnectionPhase::AwaitingOpen);
        conn.take_write_bytes(); // consume header + Open

        // Step 2: Send client Open.
        let open = Open {
            container_id: WireString::from("test-client"),
            max_frame_size: 32768,
            channel_max: 128,
            ..Default::default()
        };
        let mut frame_buf = BytesMut::new();
        codec::encode_framed_performative(
            &mut frame_buf,
            0,
            FRAME_TYPE_AMQP,
            &Performative::Open(open),
        );
        conn.feed_data(&frame_buf);
        conn.process().unwrap();
        assert_eq!(conn.phase, ConnectionPhase::Open);
        assert_eq!(conn.peer_container_id.as_deref(), Some("test-client"));
        assert_eq!(conn.max_frame_size, 32768);
        assert_eq!(conn.channel_max, 128);
    }

    #[test]
    fn test_heartbeat_response() {
        let mut conn = AmqpConnection::new();
        conn.phase = ConnectionPhase::Open;

        let mut frame_buf = BytesMut::new();
        codec::encode_empty_frame(&mut frame_buf);
        conn.feed_data(&frame_buf);
        conn.process().unwrap();

        // Should have echoed a heartbeat.
        let write = conn.take_write_bytes();
        assert!(!write.is_empty());
        let (frame, _) = codec::decode_frame(&write).unwrap();
        assert!(matches!(frame.body, FrameBody::Empty));
    }

    #[test]
    fn test_session_begin_end() {
        let mut conn = AmqpConnection::new();
        conn.phase = ConnectionPhase::Open;

        // Send Begin.
        let begin = Begin {
            remote_channel: None,
            next_outgoing_id: 0,
            incoming_window: 1024,
            outgoing_window: 1024,
            ..Default::default()
        };
        let mut frame_buf = BytesMut::new();
        codec::encode_framed_performative(
            &mut frame_buf,
            0,
            FRAME_TYPE_AMQP,
            &Performative::Begin(begin),
        );
        conn.feed_data(&frame_buf);
        conn.process().unwrap();

        assert_eq!(conn.sessions.len(), 1);
        let write = conn.take_write_bytes();
        assert!(!write.is_empty());

        // Send End.
        let end = End { error: None };
        let mut frame_buf = BytesMut::new();
        codec::encode_framed_performative(
            &mut frame_buf,
            0,
            FRAME_TYPE_AMQP,
            &Performative::End(end),
        );
        conn.feed_data(&frame_buf);
        conn.process().unwrap();

        assert_eq!(conn.sessions.len(), 0);
    }

    #[test]
    fn test_connection_close() {
        let mut conn = AmqpConnection::new();
        conn.phase = ConnectionPhase::Open;

        let close = Close { error: None };
        let mut frame_buf = BytesMut::new();
        codec::encode_framed_performative(
            &mut frame_buf,
            0,
            FRAME_TYPE_AMQP,
            &Performative::Close(close),
        );
        conn.feed_data(&frame_buf);
        let result = conn.process().unwrap();
        assert!(!result);
        assert_eq!(conn.phase, ConnectionPhase::Closed);
    }

    #[test]
    fn test_session_alloc_handle() {
        let mut session = AmqpSession::new(0);
        assert_eq!(session.alloc_handle(), 0);
        assert_eq!(session.alloc_handle(), 1);
        assert_eq!(session.alloc_handle(), 2);
    }

    #[test]
    fn test_session_delivery_id() {
        let mut session = AmqpSession::new(0);
        assert_eq!(session.next_delivery_id(), 0);
        assert_eq!(session.next_delivery_id(), 1);
        assert_eq!(session.next_delivery_id(), 2);
    }
}
