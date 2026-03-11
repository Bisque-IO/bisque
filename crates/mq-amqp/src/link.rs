//! AMQP 1.0 link management.
//!
//! A link is a unidirectional route between two nodes (source and target).
//! Links are attached within a session via Attach/Detach performatives.
//!
//! - **Sender link** (our role = sender): We send messages to the peer.
//!   The peer grants us credit via Flow frames.
//! - **Receiver link** (our role = receiver): We receive messages from the peer.
//!   We grant the peer credit via Flow frames.

use std::collections::{HashSet, VecDeque};

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use crate::broker::BrokerMessage;
use crate::types::*;

// =============================================================================
// Link State
// =============================================================================

/// Link lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    /// Attach sent/received, waiting for peer Attach.
    Attaching,
    /// Link is active.
    Active,
    /// Detach initiated.
    Detaching,
    /// Link is detached.
    Detached,
}

/// An AMQP 1.0 link (sender or receiver).
#[derive(Debug)]
pub struct AmqpLink {
    /// Link handle (session-local).
    pub handle: u32,
    /// Link name (unique within connection).
    pub name: WireString,
    /// Our role on this link.
    pub role: Role,
    /// Address this link is associated with (source or target).
    pub address: Option<WireString>,
    /// Link state.
    pub state: LinkState,
    /// Credit available (for sender: credits granted by receiver; for receiver: credits we granted).
    pub link_credit: u32,
    /// Delivery count (for sender: deliveries sent; for receiver: deliveries received).
    pub delivery_count: u32,
    /// Whether drain mode is active.
    pub drain: bool,
    /// Sender settle mode.
    pub snd_settle_mode: SndSettleMode,
    /// Receiver settle mode.
    pub rcv_settle_mode: RcvSettleMode,
    /// Unsettled deliveries. SmallVec for typical small counts.
    pub unsettled: SmallVec<[UnsettledDelivery; 8]>,
    /// Partial delivery being assembled from multi-frame transfer.
    pub partial_delivery: Option<PartialDelivery>,
    /// Maximum message size for this link (0 = no limit).
    pub max_message_size: u64,
    /// Whether this link uses a dynamic terminus (auto-created).
    pub dynamic: bool,
    /// Distribution mode for sender-role links.
    pub distribution_mode: Option<WireString>,
    /// Terminus durability level (0=none, 1=configuration, 2=unsettled-state).
    pub terminus_durable: u32,
    /// Terminus expiry policy.
    pub expiry_policy: Option<WireString>,
    /// Available message count (for sender links, reported in FLOW).
    pub available: u32,
    /// Whether this link supports resumption (has durable unsettled state).
    pub resumable: bool,
    /// Pending batchable disposition count.
    pub batchable_count: u32,
    /// Minimum pending batchable delivery ID.
    pub batchable_min: u32,
    /// Maximum pending batchable delivery ID.
    pub batchable_max: u32,
    /// Maximum batch size before flushing batchable dispositions.
    pub batch_size: u32,

    // === C2: Broker integration fields ===
    /// Broker consumer ID for this link (assigned on register_consumer).
    pub consumer_id: Option<u64>,
    /// Resolved broker entity type ("topic" or "queue").
    pub entity_type: Option<&'static str>,
    /// Resolved broker entity ID.
    pub entity_id: Option<u64>,

    // === C5: Outbound delivery fields ===
    /// Outbound message queue for sender-role links (messages from broker).
    pub outbound_queue: VecDeque<BrokerMessage>,
    /// Next delivery tag counter for outbound transfers.
    pub next_delivery_tag: u64,

    // === N3: Source filter support ===
    /// Parsed source filter from ATTACH (key=symbol, value=described filter).
    pub source_filter: Option<AmqpValue>,

    // === N11: Link properties ===
    /// Raw link properties from ATTACH.
    pub link_properties: Option<AmqpValue>,
    /// Whether this is a shared subscription link.
    pub shared: bool,
    /// Paired link name (for request-response pattern).
    pub paired: Option<WireString>,

    // === N12: Received delivery state ===
    /// Current section number during multi-frame reassembly.
    pub section_number: u32,
    /// Current section offset during multi-frame reassembly.
    pub section_offset: u64,
}

/// An unsettled delivery tracked by the link.
#[derive(Debug, Clone)]
pub struct UnsettledDelivery {
    pub delivery_id: u32,
    pub delivery_tag: Bytes,
    pub settled: bool,
}

/// Partial delivery being accumulated during multi-frame transfer.
#[derive(Debug)]
pub struct PartialDelivery {
    pub delivery_id: Option<u32>,
    pub delivery_tag: Option<Bytes>,
    pub message_format: Option<u32>,
    pub settled: Option<bool>,
    pub payload_buf: BytesMut,
}

/// Maximum partial delivery buffer size (16 MB).
pub const MAX_PARTIAL_DELIVERY_SIZE: usize = 16 * 1024 * 1024;

impl AmqpLink {
    /// Create a new link.
    pub fn new(handle: u32, name: WireString, role: Role, address: Option<WireString>) -> Self {
        Self {
            handle,
            name,
            role,
            address,
            state: LinkState::Active,
            link_credit: 0,
            delivery_count: 0,
            drain: false,
            snd_settle_mode: SndSettleMode::Mixed,
            rcv_settle_mode: RcvSettleMode::First,
            unsettled: SmallVec::new(),
            partial_delivery: None,
            max_message_size: 0,
            dynamic: false,
            distribution_mode: None,
            terminus_durable: 0,
            expiry_policy: None,
            available: 0,
            resumable: false,
            batchable_count: 0,
            batchable_min: u32::MAX,
            batchable_max: 0,
            batch_size: 64,
            consumer_id: None,
            entity_type: None,
            entity_id: None,
            outbound_queue: VecDeque::new(),
            next_delivery_tag: 0,
            source_filter: None,
            link_properties: None,
            shared: false,
            paired: None,
            section_number: 0,
            section_offset: 0,
        }
    }

    /// Check if we have credit to send (sender role).
    #[inline]
    pub fn has_credit(&self) -> bool {
        self.role == Role::Sender && self.link_credit > 0
    }

    /// Consume one credit for an outgoing transfer (sender role).
    #[inline]
    pub fn consume_credit(&mut self) {
        if self.link_credit > 0 {
            self.link_credit -= 1;
            self.delivery_count = self.delivery_count.wrapping_add(1);
        }
    }

    /// Record an incoming transfer (receiver role).
    pub fn on_transfer_received(&mut self, transfer: &Transfer) {
        self.delivery_count = self.delivery_count.wrapping_add(1);
        if self.link_credit > 0 {
            self.link_credit -= 1;
        }

        // Track unsettled delivery if not pre-settled.
        if !transfer.settled.unwrap_or(false) {
            if let Some(id) = transfer.delivery_id {
                self.unsettled.push(UnsettledDelivery {
                    delivery_id: id,
                    delivery_tag: transfer.delivery_tag.clone().unwrap_or_default(),
                    settled: false,
                });
            }
        }
    }

    /// Handle a multi-frame transfer. Returns `Some(complete_payload)` when the
    /// last frame is received (`more=false`), or `None` if still accumulating.
    /// Returns `Err` on abort or size violation.
    pub fn accumulate_transfer(&mut self, transfer: &Transfer) -> Result<Option<Bytes>, LinkError> {
        // Handle aborted transfer
        if transfer.aborted {
            if self.partial_delivery.take().is_some() {
                // Credit was already consumed, delivery discarded
            }
            return Ok(None);
        }

        if transfer.more {
            // Continuation frame — accumulate
            let partial = self
                .partial_delivery
                .get_or_insert_with(|| PartialDelivery {
                    delivery_id: transfer.delivery_id,
                    delivery_tag: transfer.delivery_tag.clone(),
                    message_format: transfer.message_format,
                    settled: transfer.settled,
                    payload_buf: BytesMut::with_capacity(transfer.payload.len() * 2),
                });

            // Check max message size
            let new_size = partial.payload_buf.len() + transfer.payload.len();
            if self.max_message_size > 0 && new_size as u64 > self.max_message_size {
                self.partial_delivery = None;
                return Err(LinkError::MessageSizeExceeded {
                    size: new_size as u64,
                    max: self.max_message_size,
                });
            }
            if new_size > MAX_PARTIAL_DELIVERY_SIZE {
                self.partial_delivery = None;
                return Err(LinkError::MessageSizeExceeded {
                    size: new_size as u64,
                    max: MAX_PARTIAL_DELIVERY_SIZE as u64,
                });
            }

            partial.payload_buf.extend_from_slice(&transfer.payload);
            Ok(None)
        } else if let Some(mut partial) = self.partial_delivery.take() {
            // Final frame of multi-frame transfer
            partial.payload_buf.extend_from_slice(&transfer.payload);

            // Check max message size
            let total = partial.payload_buf.len();
            if self.max_message_size > 0 && total as u64 > self.max_message_size {
                return Err(LinkError::MessageSizeExceeded {
                    size: total as u64,
                    max: self.max_message_size,
                });
            }

            Ok(Some(partial.payload_buf.freeze()))
        } else {
            // Single-frame transfer (no partial in progress, more=false)
            // Check max message size
            if self.max_message_size > 0 && transfer.payload.len() as u64 > self.max_message_size {
                return Err(LinkError::MessageSizeExceeded {
                    size: transfer.payload.len() as u64,
                    max: self.max_message_size,
                });
            }
            Ok(Some(transfer.payload.clone()))
        }
    }

    /// Settle deliveries in the given range.
    pub fn settle_range(&mut self, first: u32, last: u32) {
        self.unsettled
            .retain(|d| !(d.delivery_id >= first && d.delivery_id <= last));
    }

    /// Add a delivery to the batchable pending set.
    /// Returns `true` if the batch is full and should be flushed.
    #[inline]
    pub fn add_batchable(&mut self, delivery_id: u32) -> bool {
        if self.batchable_count == 0 {
            self.batchable_min = delivery_id;
            self.batchable_max = delivery_id;
        } else {
            self.batchable_min = self.batchable_min.min(delivery_id);
            self.batchable_max = self.batchable_max.max(delivery_id);
        }
        self.batchable_count += 1;
        self.batchable_count >= self.batch_size
    }

    /// Take pending batchable delivery IDs for disposition.
    /// Returns (first, last) range if any pending, or None.
    #[inline]
    pub fn flush_batchable(&mut self) -> Option<(u32, u32)> {
        if self.batchable_count == 0 {
            return None;
        }
        let first = self.batchable_min;
        let last = self.batchable_max;
        self.batchable_count = 0;
        self.batchable_min = u32::MAX;
        self.batchable_max = 0;
        Some((first, last))
    }

    /// Check if link is resumable (durable terminus with unsettled state).
    pub fn is_resumable(&self) -> bool {
        self.resumable && self.terminus_durable >= 2
    }

    /// Enqueue outbound messages from the broker (C5).
    pub fn enqueue_outbound(&mut self, messages: Vec<BrokerMessage>) {
        self.available = self.available.saturating_sub(messages.len() as u32);
        self.outbound_queue.extend(messages);
    }

    /// Dequeue the next outbound message if credit is available (C5).
    pub fn dequeue_outbound(&mut self) -> Option<BrokerMessage> {
        if self.role == Role::Sender && self.link_credit > 0 {
            self.outbound_queue.pop_front()
        } else {
            None
        }
    }

    /// Generate the next delivery tag for outbound transfers (C5).
    pub fn next_tag(&mut self) -> Bytes {
        let tag = self.next_delivery_tag;
        self.next_delivery_tag += 1;
        Bytes::copy_from_slice(&tag.to_be_bytes())
    }

    /// Check if this link has outbound messages and credit to send (C5).
    pub fn has_outbound_ready(&self) -> bool {
        self.role == Role::Sender && self.link_credit > 0 && !self.outbound_queue.is_empty()
    }

    /// Handle drain mode: advance delivery_count, zero credit (C5).
    pub fn drain_complete(&mut self) {
        self.delivery_count = self.delivery_count.wrapping_add(self.link_credit);
        self.link_credit = 0;
        self.drain = false;
    }

    /// Update received delivery state tracking (N12).
    pub fn update_received_state(&mut self, payload_len: usize) {
        self.section_offset += payload_len as u64;
    }

    /// Get the current received delivery state for resume (N12).
    pub fn received_state(&self) -> DeliveryState {
        DeliveryState::Received {
            section_number: self.section_number,
            section_offset: self.section_offset,
        }
    }

    /// Reset received state for a new delivery (N12).
    pub fn reset_received_state(&mut self) {
        self.section_number = 0;
        self.section_offset = 0;
    }

    /// Reconcile peer's unsettled map with our local unsettled state (N6).
    /// Returns deliveries that need to be resent (peer has them unsettled but we don't).
    pub fn reconcile_unsettled(&mut self, peer_unsettled: &AmqpValue) -> SmallVec<[u32; 8]> {
        let mut needs_resend = SmallVec::new();

        if let AmqpValue::Map(entries) = peer_unsettled {
            // Build O(1) lookup set of peer delivery IDs.
            let peer_ids: HashSet<u32> = entries
                .iter()
                .filter_map(|(k, _)| {
                    if let AmqpValue::Uint(id) = k {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();

            // Build O(1) lookup set of our delivery IDs.
            let our_ids: HashSet<u32> = self.unsettled.iter().map(|d| d.delivery_id).collect();

            // Peer has it but we don't → needs resend.
            for &id in &peer_ids {
                if !our_ids.contains(&id) {
                    needs_resend.push(id);
                }
            }

            // Remove from our unsettled any deliveries the peer doesn't have.
            self.unsettled.retain(|d| peer_ids.contains(&d.delivery_id));
        }

        needs_resend
    }

    /// Check if a source filter matches a message's properties (N3).
    /// Returns true if no filter is set or if the filter matches.
    pub fn matches_filter(&self, _message: &crate::types::AmqpMessage) -> bool {
        match &self.source_filter {
            None => true, // No filter = match all
            Some(AmqpValue::Map(filters)) => {
                // Basic filter support: check for selector-filter
                for (_key, _value) in filters {
                    // Filters are described types; basic implementation accepts all
                    // Full SQL selector parsing would go here
                }
                true
            }
            Some(_) => true,
        }
    }

    /// Get the address, resolving the bisque-mq entity type.
    pub fn resolved_address(&self) -> Option<ResolvedAddress> {
        self.address
            .as_ref()
            .and_then(|addr| ResolvedAddress::parse(addr))
    }
}

// =============================================================================
// Address Resolution
// =============================================================================

/// A resolved bisque-mq address from an AMQP 1.0 link address string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedAddress {
    /// topic/{name}
    Topic(WireString),
    /// queue/{name}
    Queue(WireString),
    /// exchange/{name}
    Exchange(WireString),
    /// Bare name (auto-detect).
    Auto(WireString),
}

impl ResolvedAddress {
    /// Parse an AMQP address string into a resolved address.
    pub fn parse(address: &str) -> Option<Self> {
        if address.is_empty() {
            return None;
        }
        if let Some(name) = address.strip_prefix("topic/") {
            Some(Self::Topic(WireString::from(name)))
        } else if let Some(name) = address.strip_prefix("queue/") {
            Some(Self::Queue(WireString::from(name)))
        } else if let Some(name) = address.strip_prefix("exchange/") {
            Some(Self::Exchange(WireString::from(name)))
        } else {
            Some(Self::Auto(WireString::from(address)))
        }
    }

    /// Get the entity name (without prefix).
    pub fn name(&self) -> &str {
        match self {
            Self::Topic(n) | Self::Queue(n) | Self::Exchange(n) | Self::Auto(n) => n,
        }
    }
}

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("unknown handle: {0}")]
    UnknownHandle(u32),

    #[error("handle {handle} exceeds max {max}")]
    HandleExceedsMax { handle: u32, max: u32 },

    #[error("no credit available")]
    NoCredit,

    #[error("link not in expected state")]
    InvalidState,

    #[error("message size {size} exceeds max {max}")]
    MessageSizeExceeded { size: u64, max: u64 },

    #[error("frame size {size} exceeds negotiated max {max}")]
    FrameSizeExceeded { size: u32, max: u32 },

    #[error("session window violation")]
    WindowViolation,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_link_new() {
        let link = AmqpLink::new(
            0,
            WireString::from("link-0"),
            Role::Sender,
            Some(WireString::from("topic/events")),
        );
        assert_eq!(link.handle, 0);
        assert_eq!(&*link.name, "link-0");
        assert_eq!(link.role, Role::Sender);
        assert_eq!(link.state, LinkState::Active);
        assert_eq!(link.link_credit, 0);
        assert!(!link.has_credit());
    }

    #[test]
    fn test_link_credit() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        link.link_credit = 10;
        assert!(link.has_credit());

        link.consume_credit();
        assert_eq!(link.link_credit, 9);
        assert_eq!(link.delivery_count, 1);
    }

    #[test]
    fn test_link_on_transfer() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.link_credit = 5;

        let transfer = Transfer {
            handle: 0,
            delivery_id: Some(0),
            delivery_tag: Some(Bytes::from_static(b"tag-0")),
            settled: Some(false),
            ..Default::default()
        };
        link.on_transfer_received(&transfer);

        assert_eq!(link.delivery_count, 1);
        assert_eq!(link.link_credit, 4);
        assert_eq!(link.unsettled.len(), 1);
    }

    #[test]
    fn test_link_settle_range() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 0,
            delivery_tag: Bytes::from_static(b"a"),
            settled: false,
        });
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 1,
            delivery_tag: Bytes::from_static(b"b"),
            settled: false,
        });
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 2,
            delivery_tag: Bytes::from_static(b"c"),
            settled: false,
        });

        link.settle_range(0, 1);
        assert_eq!(link.unsettled.len(), 1);
        assert_eq!(link.unsettled[0].delivery_id, 2);
    }

    #[test]
    fn test_resolved_address_parse() {
        assert_eq!(
            ResolvedAddress::parse("topic/events"),
            Some(ResolvedAddress::Topic(WireString::from("events")))
        );
        assert_eq!(
            ResolvedAddress::parse("queue/tasks"),
            Some(ResolvedAddress::Queue(WireString::from("tasks")))
        );
        assert_eq!(
            ResolvedAddress::parse("exchange/logs"),
            Some(ResolvedAddress::Exchange(WireString::from("logs")))
        );
        assert_eq!(
            ResolvedAddress::parse("my-queue"),
            Some(ResolvedAddress::Auto(WireString::from("my-queue")))
        );
        assert_eq!(ResolvedAddress::parse(""), None);
    }

    #[test]
    fn test_resolved_address_name() {
        let addr = ResolvedAddress::Topic(WireString::from("events"));
        assert_eq!(addr.name(), "events");

        let addr = ResolvedAddress::Auto(WireString::from("my-queue"));
        assert_eq!(addr.name(), "my-queue");
    }

    #[test]
    fn test_link_resolved_address() {
        let link = AmqpLink::new(
            0,
            WireString::from("link-0"),
            Role::Sender,
            Some(WireString::from("queue/tasks")),
        );
        assert_eq!(
            link.resolved_address(),
            Some(ResolvedAddress::Queue(WireString::from("tasks")))
        );

        let link = AmqpLink::new(0, WireString::from("link-1"), Role::Sender, None);
        assert_eq!(link.resolved_address(), None);
    }

    #[test]
    fn test_multi_frame_transfer() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.link_credit = 5;

        // First frame: more=true
        let t1 = Transfer {
            handle: 0,
            delivery_id: Some(0),
            delivery_tag: Some(Bytes::from_static(b"tag-0")),
            more: true,
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t1).unwrap();
        assert!(result.is_none()); // still accumulating

        // Second frame: more=true
        let t2 = Transfer {
            handle: 0,
            more: true,
            payload: Bytes::from_static(b" world"),
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t2).unwrap();
        assert!(result.is_none());

        // Final frame: more=false
        let t3 = Transfer {
            handle: 0,
            more: false,
            payload: Bytes::from_static(b"!"),
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t3).unwrap();
        assert!(result.is_some());
        // The first frame's payload was "hello" (empty in this test since no payload on t1)
        // but t2 + t3 payloads should be concatenated
    }

    #[test]
    fn test_aborted_transfer() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.link_credit = 5;

        // Start multi-frame
        let t1 = Transfer {
            handle: 0,
            delivery_id: Some(0),
            more: true,
            payload: Bytes::from_static(b"partial"),
            ..Default::default()
        };
        link.accumulate_transfer(&t1).unwrap();
        assert!(link.partial_delivery.is_some());

        // Abort
        let t2 = Transfer {
            handle: 0,
            aborted: true,
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t2).unwrap();
        assert!(result.is_none());
        assert!(link.partial_delivery.is_none());
    }

    #[test]
    fn test_max_message_size_exceeded() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.link_credit = 5;
        link.max_message_size = 10; // 10 bytes max

        let t = Transfer {
            handle: 0,
            delivery_id: Some(0),
            payload: Bytes::from_static(b"this is way too long for the limit"),
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t);
        assert!(matches!(result, Err(LinkError::MessageSizeExceeded { .. })));
    }

    #[test]
    fn test_max_message_size_exceeded_multi_frame() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.link_credit = 5;
        link.max_message_size = 10;

        // First frame fits
        let t1 = Transfer {
            handle: 0,
            delivery_id: Some(0),
            more: true,
            payload: Bytes::from_static(b"12345"),
            ..Default::default()
        };
        assert!(link.accumulate_transfer(&t1).unwrap().is_none());

        // Second frame pushes over limit
        let t2 = Transfer {
            handle: 0,
            more: true,
            payload: Bytes::from_static(b"678901"),
            ..Default::default()
        };
        assert!(matches!(
            link.accumulate_transfer(&t2),
            Err(LinkError::MessageSizeExceeded { .. })
        ));
    }

    #[test]
    fn test_receiver_no_credit() {
        let link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        assert!(!link.has_credit());
    }

    #[test]
    fn test_batchable_add_and_flush() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.batch_size = 4;

        // Add 3 — not full yet
        assert!(!link.add_batchable(10));
        assert!(!link.add_batchable(11));
        assert!(!link.add_batchable(12));
        assert_eq!(link.batchable_count, 3);

        // 4th triggers flush
        assert!(link.add_batchable(13));

        let (first, last) = link.flush_batchable().unwrap();
        assert_eq!(first, 10);
        assert_eq!(last, 13);
        assert_eq!(link.batchable_count, 0);

        // Flush on empty returns None
        assert!(link.flush_batchable().is_none());
    }

    #[test]
    fn test_batchable_flush_tracks_min_max() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.add_batchable(20);
        link.add_batchable(10);
        link.add_batchable(15);
        let (first, last) = link.flush_batchable().unwrap();
        assert_eq!(first, 10);
        assert_eq!(last, 20);
    }

    #[test]
    fn test_link_resumable() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        // Not resumable by default
        assert!(!link.is_resumable());

        // Resumable flag alone not enough — need durable >= 2
        link.resumable = true;
        link.terminus_durable = 1;
        assert!(!link.is_resumable());

        // Both set
        link.terminus_durable = 2;
        assert!(link.is_resumable());
    }

    #[test]
    fn test_pre_settled_transfer_not_tracked() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.link_credit = 5;

        let transfer = Transfer {
            handle: 0,
            delivery_id: Some(0),
            delivery_tag: Some(Bytes::from_static(b"tag-0")),
            settled: Some(true), // pre-settled
            ..Default::default()
        };
        link.on_transfer_received(&transfer);

        assert_eq!(link.delivery_count, 1);
        assert_eq!(link.link_credit, 4);
        assert_eq!(link.unsettled.len(), 0); // not tracked
    }

    // === C5: Outbound delivery tests ===

    #[test]
    fn test_outbound_queue_enqueue_dequeue() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        link.link_credit = 5;

        let msgs = vec![
            BrokerMessage {
                message_id: 1,
                payload: Bytes::from_static(b"msg-1"),
                attempt: 1,
            },
            BrokerMessage {
                message_id: 2,
                payload: Bytes::from_static(b"msg-2"),
                attempt: 1,
            },
        ];
        link.enqueue_outbound(msgs);
        assert_eq!(link.outbound_queue.len(), 2);
        assert!(link.has_outbound_ready());

        let msg = link.dequeue_outbound().unwrap();
        assert_eq!(msg.message_id, 1);
        assert_eq!(link.outbound_queue.len(), 1);
    }

    #[test]
    fn test_outbound_dequeue_no_credit() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        link.link_credit = 0;
        link.outbound_queue.push_back(BrokerMessage {
            message_id: 1,
            payload: Bytes::from_static(b"msg"),
            attempt: 1,
        });
        assert!(!link.has_outbound_ready());
        assert!(link.dequeue_outbound().is_none());
    }

    #[test]
    fn test_outbound_dequeue_wrong_role() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        link.link_credit = 5;
        link.outbound_queue.push_back(BrokerMessage {
            message_id: 1,
            payload: Bytes::from_static(b"msg"),
            attempt: 1,
        });
        assert!(!link.has_outbound_ready());
        assert!(link.dequeue_outbound().is_none());
    }

    #[test]
    fn test_delivery_tag_generation() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        let tag0 = link.next_tag();
        let tag1 = link.next_tag();
        assert_ne!(tag0, tag1);
        assert_eq!(link.next_delivery_tag, 2);
    }

    #[test]
    fn test_drain_complete() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        link.link_credit = 10;
        link.delivery_count = 5;
        link.drain = true;

        link.drain_complete();
        assert_eq!(link.link_credit, 0);
        assert_eq!(link.delivery_count, 15); // 5 + 10
        assert!(!link.drain);
    }

    // === N6: Resume & recovery tests ===

    #[test]
    fn test_reconcile_unsettled_removes_missing() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        // We have deliveries 0, 1, 2
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 0,
            delivery_tag: Bytes::from_static(b"a"),
            settled: false,
        });
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 1,
            delivery_tag: Bytes::from_static(b"b"),
            settled: false,
        });
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 2,
            delivery_tag: Bytes::from_static(b"c"),
            settled: false,
        });

        // Peer only has 1 and 2 — 0 should be removed from ours
        let peer_unsettled = AmqpValue::Map(vec![
            (AmqpValue::Uint(1), AmqpValue::Null),
            (AmqpValue::Uint(2), AmqpValue::Null),
        ]);
        let needs_resend = link.reconcile_unsettled(&peer_unsettled);
        assert!(needs_resend.is_empty()); // peer has them, we have them
        assert_eq!(link.unsettled.len(), 2); // delivery 0 removed
        assert_eq!(link.unsettled[0].delivery_id, 1);
        assert_eq!(link.unsettled[1].delivery_id, 2);
    }

    #[test]
    fn test_reconcile_unsettled_identifies_resends() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 1,
            delivery_tag: Bytes::from_static(b"b"),
            settled: false,
        });

        // Peer has delivery 1 and 5 — we don't have 5
        let peer_unsettled = AmqpValue::Map(vec![
            (AmqpValue::Uint(1), AmqpValue::Null),
            (AmqpValue::Uint(5), AmqpValue::Null),
        ]);
        let needs_resend = link.reconcile_unsettled(&peer_unsettled);
        assert_eq!(needs_resend.as_slice(), &[5]);
    }

    // === N12: Received delivery state tests ===

    #[test]
    fn test_received_state_tracking() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Receiver, None);
        assert_eq!(link.section_number, 0);
        assert_eq!(link.section_offset, 0);

        link.update_received_state(100);
        assert_eq!(link.section_offset, 100);

        link.update_received_state(200);
        assert_eq!(link.section_offset, 300);

        let state = link.received_state();
        assert_eq!(
            state,
            DeliveryState::Received {
                section_number: 0,
                section_offset: 300,
            }
        );

        link.reset_received_state();
        assert_eq!(link.section_number, 0);
        assert_eq!(link.section_offset, 0);
    }

    // === N3: Source filter tests ===

    #[test]
    fn test_matches_filter_no_filter() {
        let link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        let msg = crate::types::AmqpMessage::default();
        assert!(link.matches_filter(&msg));
    }

    #[test]
    fn test_matches_filter_with_empty_map() {
        let mut link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        link.source_filter = Some(AmqpValue::Map(Vec::new()));
        let msg = crate::types::AmqpMessage::default();
        assert!(link.matches_filter(&msg));
    }

    // === N11: Link properties tests ===

    #[test]
    fn test_link_properties_default() {
        let link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        assert!(link.link_properties.is_none());
        assert!(!link.shared);
        assert!(link.paired.is_none());
    }

    // === C2: Broker integration field tests ===

    #[test]
    fn test_broker_fields_default() {
        let link = AmqpLink::new(0, WireString::from("link-0"), Role::Sender, None);
        assert!(link.consumer_id.is_none());
        assert!(link.entity_type.is_none());
        assert!(link.entity_id.is_none());
    }

    // =================================================================
    // Gap 1: LinkState transitions
    // =================================================================

    #[test]
    fn test_link_state_values() {
        // New link starts in Active (per constructor).
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        assert_eq!(link.state, LinkState::Active);

        // Manually transition through states.
        link.state = LinkState::Detaching;
        assert_eq!(link.state, LinkState::Detaching);

        link.state = LinkState::Detached;
        assert_eq!(link.state, LinkState::Detached);

        link.state = LinkState::Attaching;
        assert_eq!(link.state, LinkState::Attaching);

        // All four variants are distinct.
        assert_ne!(LinkState::Attaching, LinkState::Active);
        assert_ne!(LinkState::Active, LinkState::Detaching);
        assert_ne!(LinkState::Detaching, LinkState::Detached);
    }

    // =================================================================
    // Gap 2: Multi-frame transfer - empty payload in continuation
    // =================================================================

    #[test]
    fn test_multi_frame_empty_continuation_payload() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.link_credit = 5;

        // First frame with data.
        let t1 = Transfer {
            handle: 0,
            delivery_id: Some(0),
            delivery_tag: Some(Bytes::from_static(b"tag")),
            more: true,
            payload: Bytes::from_static(b"hello"),
            ..Default::default()
        };
        assert!(link.accumulate_transfer(&t1).unwrap().is_none());

        // Continuation frame with empty payload.
        let t2 = Transfer {
            handle: 0,
            more: true,
            payload: Bytes::new(),
            ..Default::default()
        };
        assert!(link.accumulate_transfer(&t2).unwrap().is_none());

        // Final frame with data.
        let t3 = Transfer {
            handle: 0,
            more: false,
            payload: Bytes::from_static(b" world"),
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t3).unwrap().unwrap();
        assert_eq!(&result[..], b"hello world");
    }

    // =================================================================
    // Gap 3: Multiple consecutive aborted transfers
    // =================================================================

    #[test]
    fn test_multiple_consecutive_aborted_transfers() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.link_credit = 10;

        for i in 0..3u32 {
            // Start a partial delivery.
            let t_start = Transfer {
                handle: 0,
                delivery_id: Some(i),
                more: true,
                payload: Bytes::from_static(b"partial"),
                ..Default::default()
            };
            link.accumulate_transfer(&t_start).unwrap();
            assert!(link.partial_delivery.is_some());

            // Abort it.
            let t_abort = Transfer {
                handle: 0,
                aborted: true,
                ..Default::default()
            };
            let result = link.accumulate_transfer(&t_abort).unwrap();
            assert!(result.is_none());
            assert!(link.partial_delivery.is_none());
        }

        // After all aborts, a normal single-frame transfer should succeed.
        let t_ok = Transfer {
            handle: 0,
            delivery_id: Some(100),
            payload: Bytes::from_static(b"ok"),
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t_ok).unwrap().unwrap();
        assert_eq!(&result[..], b"ok");
    }

    // =================================================================
    // Gap 4: Batchable min/max with non-sequential delivery IDs
    // =================================================================

    #[test]
    fn test_batchable_non_sequential_ids() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.batch_size = 100; // large so we don't auto-flush

        link.add_batchable(10);
        link.add_batchable(3);
        link.add_batchable(7);

        let (first, last) = link.flush_batchable().unwrap();
        assert_eq!(first, 3);
        assert_eq!(last, 10);
    }

    // =================================================================
    // Gap 5: Batchable flush when empty
    // =================================================================

    #[test]
    fn test_batchable_flush_empty() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        assert!(link.flush_batchable().is_none());
    }

    // =================================================================
    // Gap 6: Credit wrapping at near-u32::MAX delivery_count
    // =================================================================

    #[test]
    fn test_consume_credit_wrapping_delivery_count() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.delivery_count = u32::MAX - 1;
        link.link_credit = 3;

        link.consume_credit();
        assert_eq!(link.delivery_count, u32::MAX);
        assert_eq!(link.link_credit, 2);

        link.consume_credit();
        assert_eq!(link.delivery_count, 0); // wrapped
        assert_eq!(link.link_credit, 1);

        link.consume_credit();
        assert_eq!(link.delivery_count, 1);
        assert_eq!(link.link_credit, 0);
    }

    // =================================================================
    // Gap 7: Pre-settled transfer tracking (already exists but verify thoroughly)
    // =================================================================

    #[test]
    fn test_pre_settled_transfer_with_none_settled() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.link_credit = 5;

        // settled=None is treated as not-settled, so it SHOULD be tracked.
        let transfer = Transfer {
            handle: 0,
            delivery_id: Some(42),
            delivery_tag: Some(Bytes::from_static(b"tag")),
            settled: None,
            ..Default::default()
        };
        link.on_transfer_received(&transfer);
        assert_eq!(link.unsettled.len(), 1);
        assert_eq!(link.unsettled[0].delivery_id, 42);
    }

    // =================================================================
    // Gap 8: settle_range with empty range (first > last)
    // =================================================================

    #[test]
    fn test_settle_range_empty_range() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 5,
            delivery_tag: Bytes::from_static(b"a"),
            settled: false,
        });
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 10,
            delivery_tag: Bytes::from_static(b"b"),
            settled: false,
        });

        // first > last: the condition (id >= 10 && id <= 5) is always false.
        link.settle_range(10, 5);
        assert_eq!(link.unsettled.len(), 2); // nothing removed
    }

    // =================================================================
    // Gap 9: settle_range with single element (first == last)
    // =================================================================

    #[test]
    fn test_settle_range_single_element() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        for id in 0..5u32 {
            link.unsettled.push(UnsettledDelivery {
                delivery_id: id,
                delivery_tag: Bytes::copy_from_slice(&id.to_be_bytes()),
                settled: false,
            });
        }

        link.settle_range(2, 2);
        assert_eq!(link.unsettled.len(), 4);
        assert!(link.unsettled.iter().all(|d| d.delivery_id != 2));
    }

    // =================================================================
    // Gap 10: Outbound queue - enqueue multiple, dequeue respects credit
    // =================================================================

    #[test]
    fn test_outbound_dequeue_respects_credit_limit() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.link_credit = 2;

        let msgs: Vec<BrokerMessage> = (0..5)
            .map(|i| BrokerMessage {
                message_id: i,
                payload: Bytes::from(format!("msg-{}", i)),
                attempt: 1,
            })
            .collect();
        link.enqueue_outbound(msgs);
        assert_eq!(link.outbound_queue.len(), 5);

        // Dequeue should work while credit > 0.
        let m0 = link.dequeue_outbound().unwrap();
        assert_eq!(m0.message_id, 0);

        let m1 = link.dequeue_outbound().unwrap();
        assert_eq!(m1.message_id, 1);

        // Credit not consumed by dequeue_outbound itself; the caller does that.
        // But if we manually set credit to 0, dequeue should fail.
        link.link_credit = 0;
        assert!(link.dequeue_outbound().is_none());
        assert_eq!(link.outbound_queue.len(), 3); // 3 remain
    }

    // =================================================================
    // Gap 11: next_tag monotonically increasing
    // =================================================================

    #[test]
    fn test_next_tag_monotonically_increasing() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        let mut prev = 0u64;
        for i in 0..100u64 {
            let tag = link.next_tag();
            let val = u64::from_be_bytes(tag[..].try_into().unwrap());
            assert_eq!(val, i);
            if i > 0 {
                assert!(val > prev);
            }
            prev = val;
        }
        assert_eq!(link.next_delivery_tag, 100);
    }

    // =================================================================
    // Gap 12: drain_complete when already drained (no credit)
    // =================================================================

    #[test]
    fn test_drain_complete_already_drained() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.link_credit = 0;
        link.delivery_count = 42;
        link.drain = true;

        link.drain_complete();
        // delivery_count wrapping_add(0) = unchanged
        assert_eq!(link.delivery_count, 42);
        assert_eq!(link.link_credit, 0);
        assert!(!link.drain);
    }

    // =================================================================
    // Gap 13: reconcile_unsettled with empty peer list
    // =================================================================

    #[test]
    fn test_reconcile_unsettled_empty_peer() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 1,
            delivery_tag: Bytes::from_static(b"a"),
            settled: false,
        });
        link.unsettled.push(UnsettledDelivery {
            delivery_id: 2,
            delivery_tag: Bytes::from_static(b"b"),
            settled: false,
        });

        // Empty peer map: peer has nothing.
        let peer = AmqpValue::Map(vec![]);
        let needs_resend = link.reconcile_unsettled(&peer);

        // Peer has nothing unsettled => nothing to resend.
        assert!(needs_resend.is_empty());
        // Our unsettled entries not in peer set => removed.
        assert!(link.unsettled.is_empty());
    }

    // =================================================================
    // Gap 14: reconcile_unsettled with empty our list
    // =================================================================

    #[test]
    fn test_reconcile_unsettled_empty_ours() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        // We have nothing unsettled.

        // Peer has deliveries 3 and 4.
        let peer = AmqpValue::Map(vec![
            (AmqpValue::Uint(3), AmqpValue::Null),
            (AmqpValue::Uint(4), AmqpValue::Null),
        ]);
        let needs_resend = link.reconcile_unsettled(&peer);

        // Peer has them but we don't => needs resend.
        assert_eq!(needs_resend.len(), 2);
        assert!(needs_resend.contains(&3));
        assert!(needs_resend.contains(&4));
    }

    // =================================================================
    // Gap 15: matches_filter with Map containing entries
    // =================================================================

    #[test]
    fn test_matches_filter_with_populated_map() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.source_filter = Some(AmqpValue::Map(vec![(
            AmqpValue::Symbol(WireString::from("jms-selector")),
            AmqpValue::String(WireString::from("color = 'red'")),
        )]));
        let msg = crate::types::AmqpMessage::default();
        // Stub implementation always returns true.
        assert!(link.matches_filter(&msg));
    }

    // =================================================================
    // Gap 16: ResolvedAddress::parse with various formats
    // =================================================================

    #[test]
    fn test_resolved_address_parse_queue() {
        let addr = ResolvedAddress::parse("queue/my-queue").unwrap();
        assert_eq!(addr, ResolvedAddress::Queue(WireString::from("my-queue")));
        assert_eq!(addr.name(), "my-queue");
    }

    #[test]
    fn test_resolved_address_parse_topic() {
        let addr = ResolvedAddress::parse("topic/my-topic").unwrap();
        assert_eq!(addr, ResolvedAddress::Topic(WireString::from("my-topic")));
        assert_eq!(addr.name(), "my-topic");
    }

    #[test]
    fn test_resolved_address_parse_malformed() {
        // No prefix match => Auto variant.
        let addr = ResolvedAddress::parse("something-random").unwrap();
        assert_eq!(
            addr,
            ResolvedAddress::Auto(WireString::from("something-random"))
        );

        // Partial prefix without slash is still Auto.
        let addr = ResolvedAddress::parse("topicfoo").unwrap();
        assert_eq!(addr, ResolvedAddress::Auto(WireString::from("topicfoo")));

        // Just a slash.
        let addr = ResolvedAddress::parse("/").unwrap();
        assert_eq!(addr, ResolvedAddress::Auto(WireString::from("/")));

        // Empty string returns None.
        assert!(ResolvedAddress::parse("").is_none());
    }

    // =================================================================
    // Gap 17: max_message_size = 0 means no limit
    // =================================================================

    #[test]
    fn test_max_message_size_zero_no_limit() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.max_message_size = 0; // no limit

        // Single-frame with large payload should succeed.
        let large_payload = Bytes::from(vec![0xAB; 1024 * 1024]); // 1 MB
        let t = Transfer {
            handle: 0,
            delivery_id: Some(0),
            payload: large_payload.clone(),
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t).unwrap().unwrap();
        assert_eq!(result.len(), 1024 * 1024);

        // Multi-frame with large payload should also succeed.
        let t1 = Transfer {
            handle: 0,
            delivery_id: Some(1),
            more: true,
            payload: large_payload.clone(),
            ..Default::default()
        };
        assert!(link.accumulate_transfer(&t1).unwrap().is_none());

        let t2 = Transfer {
            handle: 0,
            more: false,
            payload: large_payload,
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t2).unwrap().unwrap();
        assert_eq!(result.len(), 2 * 1024 * 1024);
    }

    // =================================================================
    // Gap 18: available count tracking (saturating_sub)
    // =================================================================

    #[test]
    fn test_available_saturating_sub() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.available = 3;

        // Enqueue 2 messages: available goes from 3 to 1.
        link.enqueue_outbound(vec![
            BrokerMessage {
                message_id: 1,
                payload: Bytes::from_static(b"a"),
                attempt: 1,
            },
            BrokerMessage {
                message_id: 2,
                payload: Bytes::from_static(b"b"),
                attempt: 1,
            },
        ]);
        assert_eq!(link.available, 1);

        // Enqueue 5 messages when available is 1: saturates to 0.
        link.enqueue_outbound(vec![
            BrokerMessage {
                message_id: 3,
                payload: Bytes::from_static(b"c"),
                attempt: 1,
            },
            BrokerMessage {
                message_id: 4,
                payload: Bytes::from_static(b"d"),
                attempt: 1,
            },
            BrokerMessage {
                message_id: 5,
                payload: Bytes::from_static(b"e"),
                attempt: 1,
            },
            BrokerMessage {
                message_id: 6,
                payload: Bytes::from_static(b"f"),
                attempt: 1,
            },
            BrokerMessage {
                message_id: 7,
                payload: Bytes::from_static(b"g"),
                attempt: 1,
            },
        ]);
        assert_eq!(link.available, 0);
    }

    // =================================================================
    // Gap 19: Link with receiver role vs sender role differences
    // =================================================================

    #[test]
    fn test_receiver_vs_sender_has_credit() {
        // Receiver with credit: has_credit returns false (only true for Sender).
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.link_credit = 10;
        assert!(!link.has_credit());

        // Sender with credit: has_credit returns true.
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.link_credit = 10;
        assert!(link.has_credit());
    }

    #[test]
    fn test_receiver_cannot_dequeue_outbound() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.link_credit = 10;
        link.outbound_queue.push_back(BrokerMessage {
            message_id: 1,
            payload: Bytes::from_static(b"m"),
            attempt: 1,
        });
        assert!(!link.has_outbound_ready());
        assert!(link.dequeue_outbound().is_none());
    }

    #[test]
    fn test_sender_has_outbound_ready() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Sender, None);
        link.link_credit = 1;
        // No messages yet.
        assert!(!link.has_outbound_ready());

        link.outbound_queue.push_back(BrokerMessage {
            message_id: 1,
            payload: Bytes::from_static(b"m"),
            attempt: 1,
        });
        assert!(link.has_outbound_ready());
    }

    // =================================================================
    // Gap 20: accumulate_transfer with delivery_id in continuation
    // =================================================================

    #[test]
    fn test_accumulate_transfer_ignores_delivery_id_in_continuation() {
        let mut link = AmqpLink::new(0, WireString::from("l"), Role::Receiver, None);
        link.link_credit = 5;

        // First frame establishes delivery_id = 42.
        let t1 = Transfer {
            handle: 0,
            delivery_id: Some(42),
            delivery_tag: Some(Bytes::from_static(b"tag")),
            more: true,
            payload: Bytes::from_static(b"part1"),
            ..Default::default()
        };
        link.accumulate_transfer(&t1).unwrap();

        // Continuation frame has a different delivery_id (should be ignored per spec).
        let t2 = Transfer {
            handle: 0,
            delivery_id: Some(999), // per spec this should be ignored
            more: true,
            payload: Bytes::from_static(b"part2"),
            ..Default::default()
        };
        link.accumulate_transfer(&t2).unwrap();

        // Verify the partial delivery retains the original delivery_id from t1.
        assert!(link.partial_delivery.is_some());
        let partial = link.partial_delivery.as_ref().unwrap();
        assert_eq!(partial.delivery_id, Some(42));

        // Final frame.
        let t3 = Transfer {
            handle: 0,
            more: false,
            payload: Bytes::from_static(b"part3"),
            ..Default::default()
        };
        let result = link.accumulate_transfer(&t3).unwrap().unwrap();
        assert_eq!(&result[..], b"part1part2part3");
    }
}
