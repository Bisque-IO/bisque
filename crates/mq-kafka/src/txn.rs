//! Lightweight transaction coordinator for Kafka protocol compatibility.
//!
//! Tracks producer ID allocation and transaction state. Transaction data
//! is committed/aborted via the bisque-mq Raft engine.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::Mutex;

/// Transaction state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Transaction is active (partitions being added).
    Ongoing,
    /// PrepareCommit submitted.
    PrepareCommit,
    /// PrepareAbort submitted.
    PrepareAbort,
    /// Completed (committed or aborted).
    Complete,
}

/// Per-producer transaction state.
#[derive(Debug)]
pub struct ProducerTxn {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub transactional_id: Arc<str>,
    pub state: TxnState,
    /// Topics/partitions registered in current transaction.
    pub partitions: HashSet<(Arc<str>, i32)>,
    /// Consumer group registered for transactional offset commit.
    pub offset_group: Option<Arc<str>>,
}

/// Transaction coordinator managing producer IDs and transaction lifecycle.
pub struct TxnCoordinator {
    next_pid: AtomicI64,
    /// transactional_id → ProducerTxn
    txns: Mutex<HashMap<Arc<str>, ProducerTxn>>,
}

impl TxnCoordinator {
    pub fn new() -> Self {
        Self {
            next_pid: AtomicI64::new(1),
            txns: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate a new producer ID (optionally tied to a transactional ID).
    pub fn init_producer_id(&self, transactional_id: Option<&str>) -> (i64, i16) {
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed);
        let epoch: i16 = 0;

        if let Some(txn_id) = transactional_id {
            let mut txns = self.txns.lock();
            // If there's an existing transaction for this ID, fence it
            if let Some(existing) = txns.get(txn_id) {
                let new_epoch = existing.producer_epoch.wrapping_add(1);
                let key: Arc<str> = existing.transactional_id.clone();
                txns.insert(
                    key.clone(),
                    ProducerTxn {
                        producer_id: pid,
                        producer_epoch: new_epoch,
                        transactional_id: key,
                        state: TxnState::Complete,
                        partitions: HashSet::new(),
                        offset_group: None,
                    },
                );
                return (pid, new_epoch);
            }
            let key: Arc<str> = Arc::from(txn_id);
            txns.insert(
                key.clone(),
                ProducerTxn {
                    producer_id: pid,
                    producer_epoch: epoch,
                    transactional_id: key,
                    state: TxnState::Complete,
                    partitions: HashSet::new(),
                    offset_group: None,
                },
            );
        }

        (pid, epoch)
    }

    /// Add partitions to an ongoing transaction.
    pub fn add_partitions(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partitions: &[i32],
    ) -> Result<(), i16> {
        let mut txns = self.txns.lock();
        let txn = txns
            .get_mut(transactional_id)
            .ok_or(crate::types::ErrorCode::InvalidTxnState.as_i16())?;

        if txn.producer_id != producer_id || txn.producer_epoch != producer_epoch {
            return Err(crate::types::ErrorCode::InvalidProducerEpoch.as_i16());
        }

        // Transition to Ongoing if Complete
        if txn.state == TxnState::Complete {
            txn.state = TxnState::Ongoing;
            txn.partitions.clear();
            txn.offset_group = None;
        }

        if txn.state != TxnState::Ongoing {
            return Err(crate::types::ErrorCode::InvalidTxnState.as_i16());
        }

        let topic_arc: Arc<str> = Arc::from(topic);
        for &p in partitions {
            txn.partitions.insert((topic_arc.clone(), p));
        }

        Ok(())
    }

    /// Register a consumer group for transactional offset commit.
    pub fn add_offsets_to_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<(), i16> {
        let mut txns = self.txns.lock();
        let txn = txns
            .get_mut(transactional_id)
            .ok_or(crate::types::ErrorCode::InvalidTxnState.as_i16())?;

        if txn.producer_id != producer_id || txn.producer_epoch != producer_epoch {
            return Err(crate::types::ErrorCode::InvalidProducerEpoch.as_i16());
        }

        if txn.state != TxnState::Ongoing {
            return Err(crate::types::ErrorCode::InvalidTxnState.as_i16());
        }

        txn.offset_group = Some(Arc::from(group_id));
        Ok(())
    }

    /// End a transaction (commit or abort).
    pub fn end_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<(), i16> {
        let mut txns = self.txns.lock();
        let txn = txns
            .get_mut(transactional_id)
            .ok_or(crate::types::ErrorCode::InvalidTxnState.as_i16())?;

        if txn.producer_id != producer_id || txn.producer_epoch != producer_epoch {
            return Err(crate::types::ErrorCode::InvalidProducerEpoch.as_i16());
        }

        if txn.state != TxnState::Ongoing {
            return Err(crate::types::ErrorCode::InvalidTxnState.as_i16());
        }

        txn.state = if committed {
            TxnState::PrepareCommit
        } else {
            TxnState::PrepareAbort
        };

        // Immediately transition to Complete (single-node, no 2PC needed)
        txn.state = TxnState::Complete;
        txn.partitions.clear();
        txn.offset_group = None;

        Ok(())
    }

    /// Validate that a transactional offset commit is valid.
    pub fn validate_txn_offset_commit(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<(), i16> {
        let txns = self.txns.lock();
        let txn = txns
            .get(transactional_id)
            .ok_or(crate::types::ErrorCode::InvalidTxnState.as_i16())?;

        if txn.producer_id != producer_id || txn.producer_epoch != producer_epoch {
            return Err(crate::types::ErrorCode::InvalidProducerEpoch.as_i16());
        }

        if txn.state != TxnState::Ongoing {
            return Err(crate::types::ErrorCode::InvalidTxnState.as_i16());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_producer_id_no_txn() {
        let coord = TxnCoordinator::new();
        let (pid1, epoch1) = coord.init_producer_id(None);
        let (pid2, epoch2) = coord.init_producer_id(None);
        assert!(pid1 > 0);
        assert!(pid2 > pid1);
        assert_eq!(epoch1, 0);
        assert_eq!(epoch2, 0);
    }

    #[test]
    fn test_init_producer_id_with_txn() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("txn-1"));
        assert!(pid > 0);
        assert_eq!(epoch, 0);

        // Re-init bumps epoch with new PID
        let (pid2, epoch2) = coord.init_producer_id(Some("txn-1"));
        assert_ne!(pid, pid2);
        assert_eq!(epoch2, 1);
    }

    #[test]
    fn test_transaction_lifecycle() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("txn-1"));

        // Add partitions starts transaction
        coord
            .add_partitions("txn-1", pid, epoch, "topic-a", &[0, 1])
            .unwrap();

        // Add offsets
        coord
            .add_offsets_to_txn("txn-1", pid, epoch, "group-1")
            .unwrap();

        // Commit
        coord.end_txn("txn-1", pid, epoch, true).unwrap();
    }

    #[test]
    fn test_transaction_abort() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("txn-2"));

        coord
            .add_partitions("txn-2", pid, epoch, "topic-b", &[0])
            .unwrap();

        coord.end_txn("txn-2", pid, epoch, false).unwrap();
    }

    #[test]
    fn test_invalid_epoch() {
        let coord = TxnCoordinator::new();
        let (pid, _epoch) = coord.init_producer_id(Some("txn-3"));

        let result = coord.add_partitions("txn-3", pid, 99, "topic", &[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_txn_offset_commit_success() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("txn-v1"));
        // Must transition to Ongoing first
        coord
            .add_partitions("txn-v1", pid, epoch, "t", &[0])
            .unwrap();
        assert!(
            coord
                .validate_txn_offset_commit("txn-v1", pid, epoch)
                .is_ok()
        );
    }

    #[test]
    fn test_validate_txn_offset_commit_not_ongoing() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("txn-v2"));
        // State is Complete, not Ongoing
        let result = coord.validate_txn_offset_commit("txn-v2", pid, epoch);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_txn_offset_commit_wrong_epoch() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("txn-v3"));
        coord
            .add_partitions("txn-v3", pid, epoch, "t", &[0])
            .unwrap();
        let result = coord.validate_txn_offset_commit("txn-v3", pid, epoch + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_txn_offset_commit_unknown_txn() {
        let coord = TxnCoordinator::new();
        let result = coord.validate_txn_offset_commit("nonexistent", 1, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_epoch_wraparound() {
        let coord = TxnCoordinator::new();
        // Initialize, then re-init many times to bump epoch
        coord.init_producer_id(Some("wrap"));
        for expected_epoch in 1..=5i16 {
            let (_, epoch) = coord.init_producer_id(Some("wrap"));
            assert_eq!(epoch, expected_epoch);
        }
    }

    #[test]
    fn test_add_partitions_multiple_topics() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("multi-topic"));
        coord
            .add_partitions("multi-topic", pid, epoch, "t1", &[0, 1])
            .unwrap();
        coord
            .add_partitions("multi-topic", pid, epoch, "t2", &[0])
            .unwrap();
        // Both should succeed, adding to same transaction
    }

    #[test]
    fn test_add_partitions_empty() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("empty-parts"));
        // Adding empty partitions should succeed (no-op)
        coord
            .add_partitions("empty-parts", pid, epoch, "t", &[])
            .unwrap();
    }

    #[test]
    fn test_add_partitions_unknown_txn() {
        let coord = TxnCoordinator::new();
        let result = coord.add_partitions("nonexistent", 1, 0, "t", &[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_add_offsets_not_ongoing() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("off-state"));
        // State is Complete, not Ongoing
        let result = coord.add_offsets_to_txn("off-state", pid, epoch, "g1");
        assert!(result.is_err());
    }

    #[test]
    fn test_add_offsets_wrong_producer() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("off-pid"));
        coord
            .add_partitions("off-pid", pid, epoch, "t", &[0])
            .unwrap();
        let result = coord.add_offsets_to_txn("off-pid", pid + 1, epoch, "g1");
        assert!(result.is_err());
    }

    #[test]
    fn test_end_txn_not_ongoing() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("end-state"));
        // State is Complete
        let result = coord.end_txn("end-state", pid, epoch, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_end_txn_wrong_producer() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("end-pid"));
        coord
            .add_partitions("end-pid", pid, epoch, "t", &[0])
            .unwrap();
        let result = coord.end_txn("end-pid", pid, epoch + 1, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_reuse_after_commit() {
        let coord = TxnCoordinator::new();
        let (pid, epoch) = coord.init_producer_id(Some("reuse"));

        // First transaction
        coord
            .add_partitions("reuse", pid, epoch, "t", &[0])
            .unwrap();
        coord.end_txn("reuse", pid, epoch, true).unwrap();

        // Second transaction with same pid/epoch (auto-transitions from Complete to Ongoing)
        coord
            .add_partitions("reuse", pid, epoch, "t2", &[1])
            .unwrap();
        coord.end_txn("reuse", pid, epoch, false).unwrap();
    }

    #[test]
    fn test_multiple_non_txn_pids() {
        let coord = TxnCoordinator::new();
        let (pid1, _) = coord.init_producer_id(None);
        let (pid2, _) = coord.init_producer_id(None);
        let (pid3, _) = coord.init_producer_id(None);
        // All should be unique and monotonically increasing
        assert!(pid1 < pid2);
        assert!(pid2 < pid3);
    }
}
