/// Entity type constants matching the protocol.
pub const ENTITY_TYPE_TOPIC: u8 = 0;
pub const ENTITY_TYPE_QUEUE: u8 = 1;
pub const ENTITY_TYPE_ACTOR_NAMESPACE: u8 = 2;
pub const ENTITY_TYPE_JOB: u8 = 3;

/// Per-subscription state tracked by the consumer handler.
///
/// Each subscription maps to a single entity (topic, queue, actor namespace)
/// in a single MQ raft group.
pub struct SubscriptionState {
    /// Client-assigned subscription ID.
    pub sub_id: u32,
    /// Raft group this entity belongs to.
    pub group_id: u64,
    /// Entity type (0=Topic, 1=Queue, 2=ActorNamespace, 3=Job).
    pub entity_type: u8,
    /// CRC64-NVME hash of the entity name.
    pub name_hash: u64,
    /// Resolved entity ID from the engine.
    pub entity_id: u64,

    // -- Flow control --
    /// Max unacked messages for this subscription.
    pub max_in_flight: u32,
    /// Current number of unacked messages.
    pub in_flight_count: u32,
    /// Bytes currently in flight (for connection-wide byte_budget tracking).
    pub in_flight_bytes: u64,

    // -- Topic-specific cursor --
    /// Next offset to read (topics only).
    pub current_offset: u64,
    /// Known head of topic (updated on publish notify).
    pub head_index: u64,
    /// True when caught up to head (tail mode).
    pub is_tailing: bool,
}

impl SubscriptionState {
    pub fn new(
        sub_id: u32,
        group_id: u64,
        entity_type: u8,
        name_hash: u64,
        entity_id: u64,
        start_offset: u64,
        max_in_flight: u32,
    ) -> Self {
        Self {
            sub_id,
            group_id,
            entity_type,
            name_hash,
            entity_id,
            max_in_flight,
            in_flight_count: 0,
            in_flight_bytes: 0,
            current_offset: start_offset,
            head_index: 0,
            is_tailing: false,
        }
    }

    /// Returns true if this subscription has capacity for more messages.
    pub fn has_capacity(&self) -> bool {
        self.max_in_flight > 0 && self.in_flight_count < self.max_in_flight
    }

    /// Returns true if this subscription is paused (max_in_flight == 0).
    pub fn is_paused(&self) -> bool {
        self.max_in_flight == 0
    }

    /// Returns true if this subscription is ready for delivery.
    ///
    /// Ready means: has capacity AND has messages available.
    /// For topics: current_offset < head_index
    /// For queues/actors: always true if has capacity (server will try to deliver)
    pub fn is_ready(&self) -> bool {
        if !self.has_capacity() {
            return false;
        }
        match self.entity_type {
            ENTITY_TYPE_TOPIC => self.current_offset < self.head_index,
            _ => true,
        }
    }

    /// Record that a message was sent to the consumer.
    pub fn on_message_sent(&mut self, msg_bytes: u64) {
        self.in_flight_count += 1;
        self.in_flight_bytes += msg_bytes;
    }

    /// Record that messages were acknowledged.
    /// Returns the bytes freed.
    pub fn on_ack(&mut self, count: u32, bytes: u64) {
        self.in_flight_count = self.in_flight_count.saturating_sub(count);
        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(bytes);
    }

    /// Record that messages were negatively acknowledged.
    /// Same as ack for flow control purposes — the messages leave in-flight.
    pub fn on_nack(&mut self, count: u32, bytes: u64) {
        self.on_ack(count, bytes);
    }

    /// Update max_in_flight. Returns the new readiness state.
    pub fn set_max_in_flight(&mut self, new_max: u32) -> bool {
        self.max_in_flight = new_max;
        self.is_ready()
    }

    /// Update the known head index (for topic tailing).
    pub fn update_head_index(&mut self, head: u64) {
        if head > self.head_index {
            self.head_index = head;
            if self.is_tailing && self.current_offset < head {
                self.is_tailing = false;
            }
        }
    }

    /// Advance the topic cursor after delivering messages.
    pub fn advance_offset(&mut self, new_offset: u64) {
        self.current_offset = new_offset;
        if self.current_offset >= self.head_index {
            self.is_tailing = true;
        }
    }

    /// How many more messages can be delivered to this subscription.
    pub fn remaining_capacity(&self) -> u32 {
        if self.is_paused() {
            return 0;
        }
        self.max_in_flight.saturating_sub(self.in_flight_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_topic_sub(sub_id: u32, max_in_flight: u32) -> SubscriptionState {
        SubscriptionState::new(sub_id, 1, ENTITY_TYPE_TOPIC, 0xCAFE, 10, 0, max_in_flight)
    }

    fn make_queue_sub(sub_id: u32, max_in_flight: u32) -> SubscriptionState {
        SubscriptionState::new(sub_id, 1, ENTITY_TYPE_QUEUE, 0xBEEF, 20, 0, max_in_flight)
    }

    #[test]
    fn test_new_subscription_defaults() {
        let sub = make_topic_sub(1, 100);
        assert_eq!(sub.sub_id, 1);
        assert_eq!(sub.group_id, 1);
        assert_eq!(sub.entity_type, ENTITY_TYPE_TOPIC);
        assert_eq!(sub.max_in_flight, 100);
        assert_eq!(sub.in_flight_count, 0);
        assert_eq!(sub.in_flight_bytes, 0);
        assert_eq!(sub.current_offset, 0);
        assert_eq!(sub.head_index, 0);
        assert!(!sub.is_tailing);
    }

    #[test]
    fn test_has_capacity() {
        let mut sub = make_queue_sub(1, 3);
        assert!(sub.has_capacity());
        sub.on_message_sent(100);
        assert!(sub.has_capacity());
        sub.on_message_sent(100);
        assert!(sub.has_capacity());
        sub.on_message_sent(100);
        assert!(!sub.has_capacity()); // 3/3
    }

    #[test]
    fn test_paused() {
        let sub = make_queue_sub(1, 0);
        assert!(sub.is_paused());
        assert!(!sub.has_capacity());
        assert!(!sub.is_ready());
        assert_eq!(sub.remaining_capacity(), 0);
    }

    #[test]
    fn test_on_message_sent() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_message_sent(500);
        assert_eq!(sub.in_flight_count, 1);
        assert_eq!(sub.in_flight_bytes, 500);
        sub.on_message_sent(300);
        assert_eq!(sub.in_flight_count, 2);
        assert_eq!(sub.in_flight_bytes, 800);
    }

    #[test]
    fn test_on_ack() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_message_sent(500);
        sub.on_message_sent(300);
        sub.on_ack(1, 500);
        assert_eq!(sub.in_flight_count, 1);
        assert_eq!(sub.in_flight_bytes, 300);
    }

    #[test]
    fn test_on_ack_saturating() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_ack(5, 1000); // more than in flight
        assert_eq!(sub.in_flight_count, 0);
        assert_eq!(sub.in_flight_bytes, 0);
    }

    #[test]
    fn test_on_nack() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_message_sent(500);
        sub.on_nack(1, 500);
        assert_eq!(sub.in_flight_count, 0);
        assert_eq!(sub.in_flight_bytes, 0);
    }

    #[test]
    fn test_set_max_in_flight() {
        let mut sub = make_queue_sub(1, 10);
        assert!(sub.set_max_in_flight(5));
        assert_eq!(sub.max_in_flight, 5);
        assert!(!sub.set_max_in_flight(0)); // paused = not ready
        assert!(sub.is_paused());
    }

    #[test]
    fn test_remaining_capacity() {
        let mut sub = make_queue_sub(1, 5);
        assert_eq!(sub.remaining_capacity(), 5);
        sub.on_message_sent(100);
        assert_eq!(sub.remaining_capacity(), 4);
        sub.on_message_sent(100);
        sub.on_message_sent(100);
        sub.on_message_sent(100);
        sub.on_message_sent(100);
        assert_eq!(sub.remaining_capacity(), 0);
    }

    // -- Topic-specific tests --

    #[test]
    fn test_topic_readiness() {
        let mut sub = make_topic_sub(1, 10);
        // No messages available (head_index == 0, current_offset == 0)
        assert!(!sub.is_ready());

        // Messages available
        sub.update_head_index(100);
        assert!(sub.is_ready());

        // Fill capacity
        for _ in 0..10 {
            sub.on_message_sent(50);
        }
        assert!(!sub.is_ready());
    }

    #[test]
    fn test_queue_readiness() {
        let sub = make_queue_sub(1, 10);
        // Queues are always ready if they have capacity
        assert!(sub.is_ready());
    }

    #[test]
    fn test_update_head_index() {
        let mut sub = make_topic_sub(1, 10);
        sub.update_head_index(50);
        assert_eq!(sub.head_index, 50);

        // Cannot go backwards
        sub.update_head_index(30);
        assert_eq!(sub.head_index, 50);
    }

    #[test]
    fn test_advance_offset() {
        let mut sub = make_topic_sub(1, 10);
        sub.update_head_index(100);
        sub.advance_offset(50);
        assert_eq!(sub.current_offset, 50);
        assert!(!sub.is_tailing);

        // Catch up
        sub.advance_offset(100);
        assert!(sub.is_tailing);
    }

    #[test]
    fn test_tailing_becomes_catchup_on_new_publish() {
        let mut sub = make_topic_sub(1, 10);
        sub.update_head_index(100);
        sub.advance_offset(100);
        assert!(sub.is_tailing);

        // New publish
        sub.update_head_index(150);
        assert!(!sub.is_tailing);
        assert!(sub.is_ready());
    }

    #[test]
    fn test_start_offset() {
        let sub = SubscriptionState::new(1, 1, ENTITY_TYPE_TOPIC, 0xCAFE, 10, 42, 100);
        assert_eq!(sub.current_offset, 42);
    }

    // -- Additional edge case tests --

    #[test]
    fn test_actor_namespace_readiness() {
        let sub = SubscriptionState::new(1, 1, ENTITY_TYPE_ACTOR_NAMESPACE, 0xFACE, 30, 0, 10);
        assert!(sub.is_ready()); // actors always ready if has capacity
        assert!(!sub.is_paused());
        assert_eq!(sub.remaining_capacity(), 10);
    }

    #[test]
    fn test_job_readiness() {
        let sub = SubscriptionState::new(1, 1, ENTITY_TYPE_JOB, 0xF00D, 40, 0, 5);
        assert!(sub.is_ready());
    }

    #[test]
    fn test_max_in_flight_one() {
        let mut sub = make_queue_sub(1, 1);
        assert!(sub.has_capacity());
        assert_eq!(sub.remaining_capacity(), 1);
        sub.on_message_sent(100);
        assert!(!sub.has_capacity());
        assert_eq!(sub.remaining_capacity(), 0);
        assert!(!sub.is_ready());
        sub.on_ack(1, 100);
        assert!(sub.has_capacity());
        assert!(sub.is_ready());
    }

    #[test]
    fn test_max_in_flight_transition_zero_to_nonzero() {
        let mut sub = make_queue_sub(1, 0);
        assert!(sub.is_paused());
        assert!(!sub.is_ready());
        assert_eq!(sub.remaining_capacity(), 0);

        let ready = sub.set_max_in_flight(10);
        assert!(ready);
        assert!(!sub.is_paused());
        assert_eq!(sub.remaining_capacity(), 10);
    }

    #[test]
    fn test_max_in_flight_transition_nonzero_to_zero() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_message_sent(100);
        sub.on_message_sent(200);
        assert_eq!(sub.in_flight_count, 2);

        let ready = sub.set_max_in_flight(0);
        assert!(!ready);
        assert!(sub.is_paused());
        // in_flight_count unchanged
        assert_eq!(sub.in_flight_count, 2);
        assert_eq!(sub.in_flight_bytes, 300);
    }

    #[test]
    fn test_reduce_max_in_flight_below_current() {
        let mut sub = make_queue_sub(1, 10);
        for _ in 0..8 {
            sub.on_message_sent(50);
        }
        assert_eq!(sub.in_flight_count, 8);
        assert!(sub.has_capacity()); // 8 < 10

        // Reduce to 5 — now at capacity (8 >= 5)
        let ready = sub.set_max_in_flight(5);
        assert!(!ready); // not ready: 8 >= 5
        assert!(!sub.has_capacity());
        assert_eq!(sub.remaining_capacity(), 0);
    }

    #[test]
    fn test_on_ack_then_on_nack_interleaved() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_message_sent(100);
        sub.on_message_sent(200);
        sub.on_message_sent(300);
        assert_eq!(sub.in_flight_count, 3);
        assert_eq!(sub.in_flight_bytes, 600);

        sub.on_ack(1, 100);
        assert_eq!(sub.in_flight_count, 2);
        assert_eq!(sub.in_flight_bytes, 500);

        sub.on_nack(1, 200);
        assert_eq!(sub.in_flight_count, 1);
        assert_eq!(sub.in_flight_bytes, 300);
    }

    #[test]
    fn test_topic_head_index_zero_not_ready() {
        let sub = make_topic_sub(1, 10);
        assert_eq!(sub.head_index, 0);
        assert_eq!(sub.current_offset, 0);
        // offset == head_index => not ready
        assert!(!sub.is_ready());
    }

    #[test]
    fn test_topic_advance_offset_past_head_becomes_tailing() {
        let mut sub = make_topic_sub(1, 10);
        sub.update_head_index(50);
        sub.advance_offset(60); // past head
        assert!(sub.is_tailing);
        assert!(!sub.is_ready()); // offset >= head_index
    }

    #[test]
    fn test_topic_tailing_to_catchup_cycle() {
        let mut sub = make_topic_sub(1, 10);
        // Phase 1: catch up
        sub.update_head_index(100);
        assert!(sub.is_ready());
        sub.advance_offset(100);
        assert!(sub.is_tailing);
        assert!(!sub.is_ready());

        // Phase 2: new publish
        sub.update_head_index(200);
        assert!(!sub.is_tailing);
        assert!(sub.is_ready());

        // Phase 3: catch up again
        sub.advance_offset(200);
        assert!(sub.is_tailing);
        assert!(!sub.is_ready());

        // Phase 4: multiple publishes
        sub.update_head_index(300);
        sub.update_head_index(400);
        sub.update_head_index(500);
        assert_eq!(sub.head_index, 500);
        assert!(!sub.is_tailing);
        assert!(sub.is_ready());
    }

    #[test]
    fn test_topic_start_offset_ahead_of_head() {
        let mut sub = SubscriptionState::new(1, 1, ENTITY_TYPE_TOPIC, 0xCAFE, 10, 200, 100);
        assert_eq!(sub.current_offset, 200);
        // head_index is 0, offset 200 > 0, not ready
        assert!(!sub.is_ready());

        sub.update_head_index(100);
        // offset 200 > head 100, still not ready
        assert!(!sub.is_ready());

        sub.update_head_index(300);
        // offset 200 < head 300, now ready
        assert!(sub.is_ready());
    }

    #[test]
    fn test_on_message_sent_zero_bytes() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_message_sent(0);
        assert_eq!(sub.in_flight_count, 1);
        assert_eq!(sub.in_flight_bytes, 0);
    }

    #[test]
    fn test_on_ack_zero_count() {
        let mut sub = make_queue_sub(1, 10);
        sub.on_message_sent(100);
        sub.on_ack(0, 0);
        assert_eq!(sub.in_flight_count, 1);
        assert_eq!(sub.in_flight_bytes, 100);
    }

    #[test]
    fn test_large_in_flight_bytes() {
        let mut sub = make_queue_sub(1, u32::MAX);
        let big = u64::MAX / 4;
        sub.on_message_sent(big);
        sub.on_message_sent(big);
        assert_eq!(sub.in_flight_bytes, big * 2);
        sub.on_ack(1, big);
        assert_eq!(sub.in_flight_bytes, big);
    }

    #[test]
    fn test_remaining_capacity_at_max_u32() {
        let sub = SubscriptionState::new(1, 1, ENTITY_TYPE_QUEUE, 0, 1, 0, u32::MAX);
        assert_eq!(sub.remaining_capacity(), u32::MAX);
    }
}
