//! AMQP 1.0 link management.
//!
//! A link is a unidirectional route between two nodes (source and target).
//! Links are attached within a session via Attach/Detach performatives.
//!
//! - **Sender link** (our role = sender): We send messages to the peer.
//!   The peer grants us credit via Flow frames.
//! - **Receiver link** (our role = receiver): We receive messages from the peer.
//!   We grant the peer credit via Flow frames.

use bytes::Bytes;

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
    pub name: String,
    /// Our role on this link.
    pub role: Role,
    /// Address this link is associated with (source or target).
    pub address: Option<String>,
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
    /// Unsettled deliveries: delivery_id → delivery_tag.
    pub unsettled: Vec<UnsettledDelivery>,
}

/// An unsettled delivery tracked by the link.
#[derive(Debug, Clone)]
pub struct UnsettledDelivery {
    pub delivery_id: u32,
    pub delivery_tag: Bytes,
    pub settled: bool,
}

impl AmqpLink {
    /// Create a new link.
    pub fn new(handle: u32, name: String, role: Role, address: Option<String>) -> Self {
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
            unsettled: Vec::new(),
        }
    }

    /// Check if we have credit to send (sender role).
    pub fn has_credit(&self) -> bool {
        self.role == Role::Sender && self.link_credit > 0
    }

    /// Consume one credit for an outgoing transfer (sender role).
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

    /// Settle deliveries in the given range.
    pub fn settle_range(&mut self, first: u32, last: u32) {
        self.unsettled
            .retain(|d| !(d.delivery_id >= first && d.delivery_id <= last));
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
    Topic(String),
    /// queue/{name}
    Queue(String),
    /// exchange/{name}
    Exchange(String),
    /// Bare name (auto-detect).
    Auto(String),
}

impl ResolvedAddress {
    /// Parse an AMQP address string into a resolved address.
    pub fn parse(address: &str) -> Option<Self> {
        if address.is_empty() {
            return None;
        }
        if let Some(name) = address.strip_prefix("topic/") {
            Some(Self::Topic(name.to_string()))
        } else if let Some(name) = address.strip_prefix("queue/") {
            Some(Self::Queue(name.to_string()))
        } else if let Some(name) = address.strip_prefix("exchange/") {
            Some(Self::Exchange(name.to_string()))
        } else {
            Some(Self::Auto(address.to_string()))
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
            "link-0".into(),
            Role::Sender,
            Some("topic/events".into()),
        );
        assert_eq!(link.handle, 0);
        assert_eq!(link.name, "link-0");
        assert_eq!(link.role, Role::Sender);
        assert_eq!(link.state, LinkState::Active);
        assert_eq!(link.link_credit, 0);
        assert!(!link.has_credit());
    }

    #[test]
    fn test_link_credit() {
        let mut link = AmqpLink::new(0, "link-0".into(), Role::Sender, None);
        link.link_credit = 10;
        assert!(link.has_credit());

        link.consume_credit();
        assert_eq!(link.link_credit, 9);
        assert_eq!(link.delivery_count, 1);
    }

    #[test]
    fn test_link_on_transfer() {
        let mut link = AmqpLink::new(0, "link-0".into(), Role::Receiver, None);
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
        let mut link = AmqpLink::new(0, "link-0".into(), Role::Receiver, None);
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
            Some(ResolvedAddress::Topic("events".into()))
        );
        assert_eq!(
            ResolvedAddress::parse("queue/tasks"),
            Some(ResolvedAddress::Queue("tasks".into()))
        );
        assert_eq!(
            ResolvedAddress::parse("exchange/logs"),
            Some(ResolvedAddress::Exchange("logs".into()))
        );
        assert_eq!(
            ResolvedAddress::parse("my-queue"),
            Some(ResolvedAddress::Auto("my-queue".into()))
        );
        assert_eq!(ResolvedAddress::parse(""), None);
    }

    #[test]
    fn test_resolved_address_name() {
        let addr = ResolvedAddress::Topic("events".into());
        assert_eq!(addr.name(), "events");

        let addr = ResolvedAddress::Auto("my-queue".into());
        assert_eq!(addr.name(), "my-queue");
    }

    #[test]
    fn test_link_resolved_address() {
        let link = AmqpLink::new(0, "link-0".into(), Role::Sender, Some("queue/tasks".into()));
        assert_eq!(
            link.resolved_address(),
            Some(ResolvedAddress::Queue("tasks".into()))
        );

        let link = AmqpLink::new(0, "link-1".into(), Role::Sender, None);
        assert_eq!(link.resolved_address(), None);
    }

    #[test]
    fn test_receiver_no_credit() {
        let link = AmqpLink::new(0, "link-0".into(), Role::Receiver, None);
        assert!(!link.has_credit());
    }

    #[test]
    fn test_pre_settled_transfer_not_tracked() {
        let mut link = AmqpLink::new(0, "link-0".into(), Role::Receiver, None);
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
}
