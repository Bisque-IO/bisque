use std::collections::HashMap;

use crate::scheduler::DeliveryScheduler;
use crate::subscription::SubscriptionState;

/// Consumer session state for a connected consumer.
///
/// Manages all subscriptions, flow control, and delivery scheduling
/// for a single consumer connection.
pub struct ConsumerSession {
    /// Durable consumer identity (persisted in raft state machine).
    pub consumer_id: u64,
    /// Session token returned from handshake (for reconnection).
    pub session_token: Vec<u8>,

    /// Active subscriptions keyed by client-assigned sub_id.
    subscriptions: HashMap<u32, SubscriptionState>,
    /// Fair round-robin scheduler for ready subscriptions.
    scheduler: DeliveryScheduler,

    /// Connection-wide byte budget remaining.
    pub byte_budget: u64,
    /// Total bytes across all subscriptions currently in flight.
    pub total_in_flight_bytes: u64,

    // Pre-initialized metrics handles.
    m_messages_sent: metrics::Counter,
    m_messages_acked: metrics::Counter,
    m_messages_nacked: metrics::Counter,
    m_bytes_sent: metrics::Counter,
}

impl ConsumerSession {
    pub fn new(consumer_id: u64, session_token: Vec<u8>) -> Self {
        let labels = [("consumer", consumer_id.to_string())];
        Self {
            consumer_id,
            session_token,
            subscriptions: HashMap::new(),
            scheduler: DeliveryScheduler::new(),
            byte_budget: u64::MAX, // unlimited until consumer sets it
            total_in_flight_bytes: 0,
            m_messages_sent: metrics::counter!("mq.consumer.messages_sent", &labels),
            m_messages_acked: metrics::counter!("mq.consumer.messages_acked", &labels),
            m_messages_nacked: metrics::counter!("mq.consumer.messages_nacked", &labels),
            m_bytes_sent: metrics::counter!("mq.consumer.bytes_sent", &labels),
        }
    }

    /// Add a subscription. If ready, adds it to the scheduler.
    pub fn add_subscription(&mut self, sub: SubscriptionState) {
        let sub_id = sub.sub_id;
        let ready = sub.is_ready();
        self.subscriptions.insert(sub_id, sub);
        if ready {
            self.scheduler.mark_ready(sub_id);
        }
    }

    /// Remove a subscription and clean up scheduler state.
    /// Returns the removed subscription if it existed.
    pub fn remove_subscription(&mut self, sub_id: u32) -> Option<SubscriptionState> {
        self.scheduler.remove(sub_id);
        let sub = self.subscriptions.remove(&sub_id)?;
        self.total_in_flight_bytes = self
            .total_in_flight_bytes
            .saturating_sub(sub.in_flight_bytes);
        Some(sub)
    }

    /// Get a subscription by ID.
    pub fn get_subscription(&self, sub_id: u32) -> Option<&SubscriptionState> {
        self.subscriptions.get(&sub_id)
    }

    /// Get a mutable reference to a subscription.
    pub fn get_subscription_mut(&mut self, sub_id: u32) -> Option<&mut SubscriptionState> {
        self.subscriptions.get_mut(&sub_id)
    }

    /// Number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Update max_in_flight for a subscription. Recalculates readiness.
    pub fn update_max_in_flight(&mut self, sub_id: u32, new_max: u32) -> bool {
        if let Some(sub) = self.subscriptions.get_mut(&sub_id) {
            let ready = sub.set_max_in_flight(new_max);
            if ready {
                self.scheduler.mark_ready(sub_id);
            } else {
                self.scheduler.mark_not_ready(sub_id);
            }
            true
        } else {
            false
        }
    }

    /// Update the connection-wide byte budget.
    pub fn update_byte_budget(&mut self, budget: u64) {
        self.byte_budget = budget;
    }

    /// Returns true if the session can send more messages.
    ///
    /// Requires: byte budget not exhausted AND at least one subscription is ready.
    pub fn can_send(&self) -> bool {
        self.total_in_flight_bytes < self.byte_budget && self.scheduler.has_ready()
    }

    /// Get the next subscription to deliver to (round-robin).
    pub fn next_delivery_sub(&mut self) -> Option<u32> {
        self.scheduler.next_ready()
    }

    /// Record that a message was sent on a subscription.
    pub fn on_message_sent(&mut self, sub_id: u32, msg_bytes: u64) {
        if let Some(sub) = self.subscriptions.get_mut(&sub_id) {
            sub.on_message_sent(msg_bytes);
            self.total_in_flight_bytes += msg_bytes;
            self.m_messages_sent.increment(1);
            self.m_bytes_sent.increment(msg_bytes);
            if !sub.is_ready() {
                self.scheduler.mark_not_ready(sub_id);
            }
        }
    }

    /// Record that messages were acknowledged.
    pub fn on_ack(&mut self, sub_id: u32, count: u32, bytes_freed: u64) {
        if let Some(sub) = self.subscriptions.get_mut(&sub_id) {
            sub.on_ack(count, bytes_freed);
            self.total_in_flight_bytes = self.total_in_flight_bytes.saturating_sub(bytes_freed);
            self.m_messages_acked.increment(count as u64);
            if sub.is_ready() {
                self.scheduler.mark_ready(sub_id);
            }
        }
    }

    /// Record that messages were negatively acknowledged.
    pub fn on_nack(&mut self, sub_id: u32, count: u32, bytes_freed: u64) {
        if let Some(sub) = self.subscriptions.get_mut(&sub_id) {
            sub.on_nack(count, bytes_freed);
            self.total_in_flight_bytes = self.total_in_flight_bytes.saturating_sub(bytes_freed);
            self.m_messages_nacked.increment(count as u64);
            if sub.is_ready() {
                self.scheduler.mark_ready(sub_id);
            }
        }
    }

    /// Update the readiness of a subscription (e.g. after topic head_index changes).
    pub fn update_readiness(&mut self, sub_id: u32) {
        if let Some(sub) = self.subscriptions.get(&sub_id) {
            if sub.is_ready() {
                self.scheduler.mark_ready(sub_id);
            } else {
                self.scheduler.mark_not_ready(sub_id);
            }
        }
    }

    /// Iterate over all subscriptions.
    pub fn subscriptions(&self) -> impl Iterator<Item = (&u32, &SubscriptionState)> {
        self.subscriptions.iter()
    }

    /// Collect all unique group_ids across subscriptions.
    pub fn group_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.subscriptions.values().map(|s| s.group_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::{ENTITY_TYPE_QUEUE, ENTITY_TYPE_TOPIC};

    fn make_session() -> ConsumerSession {
        ConsumerSession::new(42, b"token".to_vec())
    }

    fn topic_sub(sub_id: u32) -> SubscriptionState {
        let mut sub = SubscriptionState::new(sub_id, 1, ENTITY_TYPE_TOPIC, 0xCAFE, 10, 0, 100);
        sub.update_head_index(1000); // has messages
        sub
    }

    fn queue_sub(sub_id: u32) -> SubscriptionState {
        SubscriptionState::new(sub_id, 1, ENTITY_TYPE_QUEUE, 0xBEEF, 20, 0, 50)
    }

    #[test]
    fn test_new_session() {
        let session = make_session();
        assert_eq!(session.consumer_id, 42);
        assert_eq!(session.session_token, b"token");
        assert_eq!(session.subscription_count(), 0);
        assert_eq!(session.byte_budget, u64::MAX);
        assert_eq!(session.total_in_flight_bytes, 0);
        assert!(!session.can_send()); // no subs
    }

    #[test]
    fn test_add_subscription() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        assert_eq!(session.subscription_count(), 1);
        assert!(session.get_subscription(1).is_some());
        assert!(session.can_send());
    }

    #[test]
    fn test_remove_subscription() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.on_message_sent(1, 500);
        assert_eq!(session.total_in_flight_bytes, 500);

        let removed = session.remove_subscription(1);
        assert!(removed.is_some());
        assert_eq!(session.subscription_count(), 0);
        assert_eq!(session.total_in_flight_bytes, 0);
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut session = make_session();
        assert!(session.remove_subscription(999).is_none());
    }

    #[test]
    fn test_can_send_byte_budget() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.update_byte_budget(1000);
        assert!(session.can_send());

        // Exhaust byte budget
        session.on_message_sent(1, 1000);
        assert!(!session.can_send());

        // Free some bytes
        session.on_ack(1, 1, 500);
        assert!(session.can_send());
    }

    #[test]
    fn test_round_robin_delivery() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.add_subscription(queue_sub(2));
        session.add_subscription(queue_sub(3));

        assert_eq!(session.next_delivery_sub(), Some(1));
        assert_eq!(session.next_delivery_sub(), Some(2));
        assert_eq!(session.next_delivery_sub(), Some(3));
        assert_eq!(session.next_delivery_sub(), Some(1));
    }

    #[test]
    fn test_on_message_sent_removes_from_scheduler_when_full() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1)); // max_in_flight = 50
        for _ in 0..50 {
            session.on_message_sent(1, 10);
        }
        // Sub is at capacity, should be removed from scheduler
        assert!(!session.can_send());
    }

    #[test]
    fn test_on_ack_readds_to_scheduler() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1)); // max_in_flight = 50
        for _ in 0..50 {
            session.on_message_sent(1, 10);
        }
        assert!(!session.can_send());

        session.on_ack(1, 1, 10);
        assert!(session.can_send());
    }

    #[test]
    fn test_on_nack_readds_to_scheduler() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        for _ in 0..50 {
            session.on_message_sent(1, 10);
        }
        session.on_nack(1, 5, 50);
        assert!(session.can_send());
    }

    #[test]
    fn test_update_max_in_flight() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        assert!(session.can_send());

        // Pause subscription
        session.update_max_in_flight(1, 0);
        assert!(!session.can_send());

        // Resume
        session.update_max_in_flight(1, 10);
        assert!(session.can_send());
    }

    #[test]
    fn test_update_max_in_flight_unknown_sub() {
        let mut session = make_session();
        assert!(!session.update_max_in_flight(999, 10));
    }

    #[test]
    fn test_topic_sub_readiness_update() {
        let mut session = make_session();
        let mut sub = topic_sub(1);
        sub.advance_offset(1000); // caught up = tailing
        session.add_subscription(sub);
        // Topic caught up, not ready
        assert!(!session.can_send());

        // Simulate new publish notification
        if let Some(sub) = session.get_subscription_mut(1) {
            sub.update_head_index(1500);
        }
        session.update_readiness(1);
        assert!(session.can_send());
    }

    #[test]
    fn test_group_ids() {
        let mut session = make_session();
        session.add_subscription(SubscriptionState::new(
            1,
            10,
            ENTITY_TYPE_QUEUE,
            0,
            1,
            0,
            10,
        ));
        session.add_subscription(SubscriptionState::new(
            2,
            20,
            ENTITY_TYPE_QUEUE,
            0,
            2,
            0,
            10,
        ));
        session.add_subscription(SubscriptionState::new(
            3,
            10,
            ENTITY_TYPE_TOPIC,
            0,
            3,
            0,
            10,
        ));
        let ids = session.group_ids();
        assert_eq!(ids, vec![10, 20]);
    }

    #[test]
    fn test_byte_budget_unlimited_by_default() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        // Default byte_budget is u64::MAX, so it should always pass
        session.on_message_sent(1, u64::MAX / 2);
        assert!(session.can_send()); // still under budget
    }

    // -- Additional edge case and pacing tests --

    #[test]
    fn test_byte_budget_exact_boundary() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.update_byte_budget(1000);

        // Send exactly at budget
        session.on_message_sent(1, 1000);
        // total_in_flight_bytes == byte_budget, can_send checks <, so false
        assert!(!session.can_send());

        // Ack 1 byte
        session.on_ack(1, 1, 1);
        // 999 < 1000
        assert!(session.can_send());
    }

    #[test]
    fn test_byte_budget_one_byte_over() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.update_byte_budget(500);

        session.on_message_sent(1, 250);
        assert!(session.can_send()); // 250 < 500

        session.on_message_sent(1, 250);
        assert!(!session.can_send()); // 500 == 500, not <

        session.on_message_sent(1, 1);
        assert!(!session.can_send()); // 501 > 500
    }

    #[test]
    fn test_pacing_multiple_subs_shared_byte_budget() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.add_subscription(queue_sub(2));
        session.update_byte_budget(1000);

        // Sub 1 takes 600 bytes
        session.on_message_sent(1, 600);
        assert!(session.can_send()); // 600 < 1000, sub 2 still ready

        // Sub 2 takes 400 bytes — now at budget
        session.on_message_sent(2, 400);
        assert!(!session.can_send()); // 1000 == 1000

        // Ack on sub 1 frees bytes, allowing sub 2 to deliver
        session.on_ack(1, 1, 300);
        assert!(session.can_send()); // 700 < 1000
    }

    #[test]
    fn test_pacing_nack_frees_budget() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.update_byte_budget(500);

        session.on_message_sent(1, 500);
        assert!(!session.can_send());

        session.on_nack(1, 1, 500);
        assert!(session.can_send()); // nack freed all bytes
    }

    #[test]
    fn test_byte_budget_update_resumes_delivery() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.update_byte_budget(100);

        session.on_message_sent(1, 100);
        assert!(!session.can_send());

        // Increase budget
        session.update_byte_budget(200);
        assert!(session.can_send()); // 100 < 200
    }

    #[test]
    fn test_byte_budget_decrease_below_current() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.update_byte_budget(1000);

        session.on_message_sent(1, 500);
        assert!(session.can_send()); // 500 < 1000

        // Decrease budget below current in-flight
        session.update_byte_budget(200);
        assert!(!session.can_send()); // 500 >= 200
    }

    #[test]
    fn test_remove_subscription_frees_bytes_for_pacing() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.add_subscription(queue_sub(2));
        session.update_byte_budget(1000);

        session.on_message_sent(1, 600);
        session.on_message_sent(2, 400);
        assert!(!session.can_send()); // 1000 == 1000

        // Remove sub 1 — its in-flight bytes should be freed
        session.remove_subscription(1);
        assert_eq!(session.total_in_flight_bytes, 400);
        assert!(session.can_send()); // 400 < 1000
    }

    #[test]
    fn test_add_topic_sub_not_ready_until_head_advances() {
        let mut session = make_session();
        let sub = SubscriptionState::new(1, 1, ENTITY_TYPE_TOPIC, 0xCAFE, 10, 0, 100);
        // head_index is 0, offset is 0 => not ready
        session.add_subscription(sub);
        assert!(!session.can_send());

        // Simulate head advance
        if let Some(sub) = session.get_subscription_mut(1) {
            sub.update_head_index(50);
        }
        session.update_readiness(1);
        assert!(session.can_send());
    }

    #[test]
    fn test_interleaved_ack_nack_across_subs() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1)); // max 50
        session.add_subscription(queue_sub(2)); // max 50

        // Fill both to capacity
        for _ in 0..50 {
            session.on_message_sent(1, 10);
        }
        for _ in 0..50 {
            session.on_message_sent(2, 20);
        }
        assert_eq!(session.total_in_flight_bytes, 50 * 10 + 50 * 20);
        assert!(!session.can_send());

        // Ack some from sub 1
        session.on_ack(1, 10, 100);
        assert!(session.can_send()); // sub 1 has capacity again

        // Nack some from sub 2
        session.on_nack(2, 5, 100);
        assert!(session.can_send());
    }

    #[test]
    fn test_update_max_in_flight_on_nonexistent_sub() {
        let mut session = make_session();
        assert!(!session.update_max_in_flight(999, 10));
    }

    #[test]
    fn test_on_ack_nonexistent_sub() {
        let mut session = make_session();
        // Should not panic
        session.on_ack(999, 5, 100);
        assert_eq!(session.total_in_flight_bytes, 0);
    }

    #[test]
    fn test_on_nack_nonexistent_sub() {
        let mut session = make_session();
        session.on_nack(999, 5, 100);
        assert_eq!(session.total_in_flight_bytes, 0);
    }

    #[test]
    fn test_on_message_sent_nonexistent_sub() {
        let mut session = make_session();
        session.on_message_sent(999, 100);
        assert_eq!(session.total_in_flight_bytes, 0);
    }

    #[test]
    fn test_update_readiness_nonexistent_sub() {
        let mut session = make_session();
        // Should not panic
        session.update_readiness(999);
    }

    #[test]
    fn test_subscribe_unsubscribe_resubscribe() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        assert!(session.can_send());

        session.on_message_sent(1, 100);
        assert_eq!(session.total_in_flight_bytes, 100);

        session.remove_subscription(1);
        assert_eq!(session.total_in_flight_bytes, 0);
        assert!(!session.can_send()); // no subs

        // Re-add same sub_id
        session.add_subscription(queue_sub(1));
        assert!(session.can_send());
        assert_eq!(session.total_in_flight_bytes, 0); // fresh
    }

    #[test]
    fn test_multiple_group_ids() {
        let mut session = make_session();
        session.add_subscription(SubscriptionState::new(
            1,
            10,
            ENTITY_TYPE_QUEUE,
            0,
            1,
            0,
            10,
        ));
        session.add_subscription(SubscriptionState::new(
            2,
            20,
            ENTITY_TYPE_QUEUE,
            0,
            2,
            0,
            10,
        ));
        session.add_subscription(SubscriptionState::new(
            3,
            30,
            ENTITY_TYPE_TOPIC,
            0,
            3,
            0,
            10,
        ));
        session.add_subscription(SubscriptionState::new(
            4,
            10,
            ENTITY_TYPE_QUEUE,
            0,
            4,
            0,
            10,
        ));

        let ids = session.group_ids();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_group_ids_empty() {
        let session = make_session();
        assert!(session.group_ids().is_empty());
    }

    #[test]
    fn test_round_robin_skips_paused_sub() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.add_subscription(queue_sub(2));
        session.add_subscription(queue_sub(3));

        // Pause sub 2
        session.update_max_in_flight(2, 0);

        // Round robin should skip sub 2
        let mut seen = vec![];
        for _ in 0..6 {
            if let Some(id) = session.next_delivery_sub() {
                seen.push(id);
            }
        }
        // Should only see 1 and 3
        assert!(seen.iter().all(|&id| id == 1 || id == 3));
        assert!(!seen.contains(&2));
    }

    #[test]
    fn test_pacing_zero_budget() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.update_byte_budget(0);
        assert!(!session.can_send()); // 0 < 0 is false
    }

    #[test]
    fn test_subscriptions_iterator() {
        let mut session = make_session();
        session.add_subscription(queue_sub(1));
        session.add_subscription(queue_sub(2));
        let subs: Vec<_> = session.subscriptions().collect();
        assert_eq!(subs.len(), 2);
    }
}
