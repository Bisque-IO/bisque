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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;
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
    pub mechanism: String,
    pub username: String,
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
    /// Links by handle.
    pub links: HashMap<u32, AmqpLink>,
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
            links: HashMap::new(),
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
    pub container_id: String,
    /// Peer's container ID.
    pub peer_container_id: Option<String>,
    /// Negotiated max frame size.
    pub max_frame_size: u32,
    /// Negotiated channel max.
    pub channel_max: u16,
    /// Idle timeout in milliseconds (0 = disabled).
    pub idle_timeout: u32,
    /// Sessions indexed by local channel number.
    pub sessions: HashMap<u16, AmqpSession>,
    /// Map from remote channel to local channel.
    remote_to_local_channel: HashMap<u16, u16>,
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
        Self {
            id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            phase: ConnectionPhase::AwaitingHeader,
            auth: AuthState::default(),
            container_id: format!("bisque-mq-{}", NEXT_CONNECTION_ID.load(Ordering::Relaxed)),
            peer_container_id: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            channel_max: DEFAULT_CHANNEL_MAX,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            sessions: HashMap::new(),
            remote_to_local_channel: HashMap::new(),
            next_channel: 0,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
        }
    }

    /// Feed incoming bytes from the TCP stream.
    pub fn feed_data(&mut self, data: &[u8]) {
        self.read_buf.extend_from_slice(data);
    }

    /// Take the outgoing write buffer.
    pub fn take_write_buf(&mut self) -> BytesMut {
        std::mem::replace(&mut self.write_buf, BytesMut::with_capacity(4096))
    }

    /// Check if there is data pending to be written.
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
                self.auth.mechanism = "ANONYMOUS".to_string();
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
        let mut payload = BytesMut::new();
        codec::encode_sasl_mechanisms(&mut payload, &["PLAIN", "ANONYMOUS"]);
        codec::encode_frame(&mut self.write_buf, 0, FRAME_TYPE_SASL, &payload);
        debug!(conn = self.id, "sent sasl-mechanisms");
    }

    fn process_sasl(&mut self) -> Result<bool, ConnectionError> {
        if self.read_buf.len() < 8 {
            return Ok(false);
        }

        let (frame, consumed) = match codec::decode_frame(&self.read_buf) {
            Ok(r) => r,
            Err(CodecError::Incomplete) => return Ok(false),
            Err(e) => return Err(ConnectionError::Codec(e)),
        };
        self.read_buf.advance(consumed);

        match frame.body {
            FrameBody::Sasl(SaslPerformative::Init(init)) => {
                self.auth.mechanism = init.mechanism.clone();

                if init.mechanism == "PLAIN" {
                    if let Some(ref response) = init.initial_response {
                        self.parse_sasl_plain(response);
                    }
                    self.auth.authenticated = true;
                } else if init.mechanism == "ANONYMOUS" {
                    self.auth.authenticated = true;
                    self.auth.username = "anonymous".to_string();
                } else {
                    // Unknown mechanism — reject.
                    let mut payload = BytesMut::new();
                    codec::encode_sasl_outcome(&mut payload, SaslCode::Auth);
                    codec::encode_frame(&mut self.write_buf, 0, FRAME_TYPE_SASL, &payload);
                    self.phase = ConnectionPhase::Closed;
                    return Err(ConnectionError::AuthenticationFailed);
                }

                info!(
                    conn = self.id,
                    user = %self.auth.username,
                    mechanism = %self.auth.mechanism,
                    "SASL authentication succeeded"
                );

                // Send sasl-outcome(ok).
                let mut payload = BytesMut::new();
                codec::encode_sasl_outcome(&mut payload, SaslCode::Ok);
                codec::encode_frame(&mut self.write_buf, 0, FRAME_TYPE_SASL, &payload);

                self.phase = ConnectionPhase::AwaitingAmqpHeader;
                Ok(true)
            }
            _ => {
                warn!(conn = self.id, "unexpected SASL frame");
                Err(ConnectionError::UnexpectedFrame("expected sasl-init"))
            }
        }
    }

    fn parse_sasl_plain(&mut self, response: &[u8]) {
        let parts: Vec<&[u8]> = response.split(|&b| b == 0).collect();
        if parts.len() >= 3 {
            self.auth.username = String::from_utf8_lossy(parts[1]).to_string();
        } else if parts.len() == 2 {
            self.auth.username = String::from_utf8_lossy(parts[0]).to_string();
        }
    }

    // =========================================================================
    // Connection Open/Close
    // =========================================================================

    fn send_open(&mut self) {
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
            offered_capabilities: vec!["ANONYMOUS-RELAY".to_string()],
            ..Default::default()
        };
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Open(open));
        codec::encode_frame(&mut self.write_buf, 0, FRAME_TYPE_AMQP, &payload);
        debug!(conn = self.id, "sent Open");
    }

    /// Send a connection-level error and initiate close.
    pub fn send_connection_error(&mut self, condition: &str, description: &str) {
        let close = Close {
            error: Some(AmqpError::new(condition, description)),
        };
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Close(close));
        codec::encode_frame(&mut self.write_buf, 0, FRAME_TYPE_AMQP, &payload);
        self.phase = ConnectionPhase::Closing;
    }

    // =========================================================================
    // Frame Processing
    // =========================================================================

    fn process_frame(&mut self) -> Result<bool, ConnectionError> {
        if self.read_buf.len() < 8 {
            return Ok(false);
        }

        let (frame, consumed) = match codec::decode_frame(&self.read_buf) {
            Ok(r) => r,
            Err(CodecError::Incomplete) => return Ok(false),
            Err(e) => return Err(ConnectionError::Codec(e)),
        };
        self.read_buf.advance(consumed);

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
            .get(&channel)
            .copied()
            .unwrap_or(channel);
        let session = self
            .sessions
            .get_mut(&local_ch)
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

        // Send Close response.
        let resp = Close { error: None };
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Close(resp));
        codec::encode_frame(&mut self.write_buf, 0, FRAME_TYPE_AMQP, &payload);

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

        // Send Begin response.
        let resp = Begin {
            remote_channel: Some(remote_channel),
            next_outgoing_id: session.next_outgoing_id,
            incoming_window: session.incoming_window,
            outgoing_window: session.outgoing_window,
            handle_max: session.handle_max,
            ..Default::default()
        };
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Begin(resp));
        codec::encode_frame(
            &mut self.write_buf,
            local_channel,
            FRAME_TYPE_AMQP,
            &payload,
        );

        self.sessions.insert(local_channel, session);
        self.remote_to_local_channel
            .insert(remote_channel, local_channel);

        debug!(
            conn = self.id,
            local_channel, remote_channel, "session begun"
        );
        Ok(true)
    }

    fn handle_end(&mut self, channel: u16, end: End) -> Result<bool, ConnectionError> {
        let local_ch = self
            .remote_to_local_channel
            .get(&channel)
            .copied()
            .unwrap_or(channel);

        if let Some(session) = self.sessions.remove(&local_ch) {
            if let Some(remote) = session.remote_channel {
                self.remote_to_local_channel.remove(&remote);
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
            let mut payload = BytesMut::new();
            codec::encode_performative(&mut payload, &Performative::End(resp));
            codec::encode_frame(&mut self.write_buf, local_ch, FRAME_TYPE_AMQP, &payload);

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

use bytes::Buf;

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

        // Send Attach response.
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
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Attach(resp));
        codec::encode_frame(write_buf, self.local_channel, FRAME_TYPE_AMQP, &payload);

        self.links.insert(handle, link);
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

        if let Some(_link) = self.links.remove(&handle) {
            // Send Detach response.
            let resp = Detach {
                handle,
                closed: detach.closed,
                error: None,
            };
            let mut payload = BytesMut::new();
            codec::encode_performative(&mut payload, &Performative::Detach(resp));
            codec::encode_frame(write_buf, self.local_channel, FRAME_TYPE_AMQP, &payload);
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
            if let Some(link) = self.links.get_mut(&handle) {
                if let Some(credit) = flow.link_credit {
                    link.link_credit = credit;
                }
                if let Some(count) = flow.delivery_count {
                    link.delivery_count = count;
                }
                link.drain = flow.drain;

                debug!(
                    channel = self.local_channel,
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
            let mut payload = BytesMut::new();
            codec::encode_performative(&mut payload, &Performative::Flow(resp));
            codec::encode_frame(write_buf, self.local_channel, FRAME_TYPE_AMQP, &payload);
        }

        Ok(())
    }

    pub fn handle_transfer(
        &mut self,
        transfer: Transfer,
        write_buf: &mut BytesMut,
    ) -> Result<(), LinkError> {
        let handle = transfer.handle;

        let link = self
            .links
            .get_mut(&handle)
            .ok_or(LinkError::UnknownHandle(handle))?;

        // Track delivery for unsettled transfers.
        let settled = transfer.settled.unwrap_or(false);
        let delivery_id = transfer.delivery_id.unwrap_or(self.next_incoming_id);

        link.on_transfer_received(&transfer);

        debug!(
            channel = self.local_channel,
            handle,
            delivery_id,
            settled,
            payload_len = transfer.payload.len(),
            "transfer received"
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
            let mut payload = BytesMut::new();
            codec::encode_performative(&mut payload, &Performative::Disposition(disposition));
            codec::encode_frame(write_buf, self.local_channel, FRAME_TYPE_AMQP, &payload);
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
        // TODO: Update unsettled delivery map when integrated with MqWriteBatcher.
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
        conn.parse_sasl_plain(b"\x00guest\x00secret");
        assert_eq!(conn.auth.username, "guest");
    }

    #[test]
    fn test_sasl_plain_no_leading_null() {
        let mut conn = AmqpConnection::new();
        conn.parse_sasl_plain(b"user\x00pass");
        assert_eq!(conn.auth.username, "user");
    }

    #[test]
    fn test_full_amqp_handshake() {
        let mut conn = AmqpConnection::new();

        // Step 1: Send AMQP header (no SASL).
        conn.feed_data(&AMQP_HEADER);
        conn.process().unwrap();
        assert_eq!(conn.phase, ConnectionPhase::AwaitingOpen);
        conn.take_write_buf(); // consume header + Open

        // Step 2: Send client Open.
        let open = Open {
            container_id: "test-client".to_string(),
            max_frame_size: 32768,
            channel_max: 128,
            ..Default::default()
        };
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Open(open));
        let mut frame_buf = BytesMut::new();
        codec::encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);
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
        let write = conn.take_write_buf();
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
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Begin(begin));
        let mut frame_buf = BytesMut::new();
        codec::encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);
        conn.feed_data(&frame_buf);
        conn.process().unwrap();

        assert_eq!(conn.sessions.len(), 1);
        let write = conn.take_write_buf();
        assert!(!write.is_empty());

        // Send End.
        let end = End { error: None };
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::End(end));
        let mut frame_buf = BytesMut::new();
        codec::encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);
        conn.feed_data(&frame_buf);
        conn.process().unwrap();

        assert_eq!(conn.sessions.len(), 0);
    }

    #[test]
    fn test_connection_close() {
        let mut conn = AmqpConnection::new();
        conn.phase = ConnectionPhase::Open;

        let close = Close { error: None };
        let mut payload = BytesMut::new();
        codec::encode_performative(&mut payload, &Performative::Close(close));
        let mut frame_buf = BytesMut::new();
        codec::encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);
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
