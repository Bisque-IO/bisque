//! Integration tests for MqSegmentCursor, MqSegmentScanner, point-read helpers,
//! and MqReader — all backed by real mmap raft storage.
//!
//! These tests create a real `MmapPerGroupLogStorage`, write raft entries
//! containing `MqCommand` payloads, then exercise the cursor/scanner/reader APIs.

use std::sync::Arc;

use bytes::Bytes;
use openraft::entry::EntryPayload;
use openraft::storage::RaftLogStorage;
use openraft::type_config::async_runtime::AsyncRuntime;
use openraft::type_config::async_runtime::oneshot::Oneshot;
use openraft::{LogId, storage::IOFlushed};
use tempfile::TempDir;

use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::metadata::TopicMeta;
use bisque_mq::types::{MqCommand, MqResponse};
use bisque_mq::{
    MqMetadata, MqReader, MqSegmentCursor, MqSegmentScanner, read_command,
    read_latest_topic_message, read_message_at, read_messages_at_into,
};
use bisque_raft::{BisqueRaftTypeConfig, MmapPerGroupLogStorage, MmapStorageConfig};

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type C = BisqueRaftTypeConfig<MqCommand, MqResponse>;
type Rt = <C as openraft::RaftTypeConfig>::AsyncRuntime;
type Os = <Rt as AsyncRuntime>::Oneshot;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn run_async<F>(f: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    rt.block_on(f)
}

fn make_entry(index: u64, term: u64, cmd: MqCommand) -> openraft::impls::Entry<C> {
    openraft::impls::Entry::<C> {
        log_id: LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
            index,
        },
        payload: EntryPayload::Normal(cmd),
    }
}

fn make_callback() -> (
    IOFlushed<C>,
    <Os as Oneshot>::Receiver<Result<(), std::io::Error>>,
) {
    let (tx, rx) = Os::channel::<Result<(), std::io::Error>>();
    let cb = IOFlushed::<C>::signal(tx);
    (cb, rx)
}

fn make_flat_msg(value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(Bytes::from(value.to_vec()))
        .timestamp(1000)
        .build()
}

/// Create storage + log with small segments for multi-segment testing.
async fn setup_storage(
    tmp: &TempDir,
    segment_size: u64,
) -> (
    MmapPerGroupLogStorage<C>,
    bisque_raft::MmapGroupLogStorage<C>,
) {
    let config = MmapStorageConfig::new(tmp.path())
        .with_segment_size(segment_size)
        .with_max_pinned_segments(64)
        .with_max_concurrent_segment_opens(4);
    let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
    let log = storage.get_log_storage(0).await.unwrap();
    (storage, log)
}

/// Append a batch of entries and wait for flush.
async fn append_entries(
    log: &mut bisque_raft::MmapGroupLogStorage<C>,
    entries: Vec<openraft::impls::Entry<C>>,
) {
    let (cb, rx) = make_callback();
    log.append(entries, cb).await.unwrap();
    rx.await.unwrap().unwrap();
}

// =============================================================================
// MqSegmentScanner tests
// =============================================================================

#[test]
fn scanner_single_segment() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let entries = vec![
            make_entry(1, 1, MqCommand::publish(10, &[make_flat_msg(b"hello")])),
            make_entry(2, 1, MqCommand::publish(20, &[make_flat_msg(b"world")])),
            make_entry(3, 1, MqCommand::publish(30, &[make_flat_msg(b"msg")])),
        ];
        append_entries(&mut log, entries).await;

        let mut scanner = MqSegmentScanner::from_start(prefetcher);

        let rec = scanner.next_record().unwrap();
        assert_eq!(rec.log_index, 1);
        assert_eq!(rec.command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(rec.command.as_publish().topic_id(), 10);

        let rec = scanner.next_record().unwrap();
        assert_eq!(rec.log_index, 2);
        assert_eq!(rec.command.as_publish().topic_id(), 20);

        let rec = scanner.next_record().unwrap();
        assert_eq!(rec.log_index, 3);
        assert_eq!(rec.command.tag(), MqCommand::TAG_PUBLISH);

        assert!(scanner.next_record().is_none());
        storage.stop();
    });
}

#[test]
fn scanner_multi_segment() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        // Very small segments to force multiple segment files.
        let (storage, mut log) = setup_storage(&tmp, 4096).await;
        let prefetcher = log.prefetcher();

        // Write enough entries to span multiple segments.
        let mut entries = Vec::new();
        for i in 1..=20 {
            entries.push(make_entry(
                i,
                1,
                MqCommand::publish(i % 5, &[make_flat_msg(format!("msg-{i}").as_bytes())]),
            ));
        }
        append_entries(&mut log, entries).await;

        let mut scanner = MqSegmentScanner::from_start(prefetcher);

        let mut count = 0;
        let mut last_index = 0;
        while let Some(rec) = scanner.next_record() {
            count += 1;
            assert!(rec.log_index > last_index, "indices should be ascending");
            last_index = rec.log_index;
        }
        assert_eq!(count, 20);

        storage.stop();
    });
}

#[test]
fn scanner_entity_filter_across_segments() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 4096).await;
        let prefetcher = log.prefetcher();

        let mut entries = Vec::new();
        for i in 1..=20 {
            let topic_id = i % 3; // topics 0, 1, 2
            entries.push(make_entry(
                i,
                1,
                MqCommand::publish(topic_id, &[make_flat_msg(b"data")]),
            ));
        }
        append_entries(&mut log, entries).await;

        let mut scanner = MqSegmentScanner::from_start(prefetcher);

        let mut topic1_count = 0;
        while let Some(rec) = scanner.next_record_for_entity(MqCommand::TAG_PUBLISH, 1) {
            assert_eq!(rec.command.as_publish().topic_id(), 1);
            topic1_count += 1;
        }
        // indices 1,4,7,10,13,16,19 → i%3==1 → 7 entries
        assert_eq!(topic1_count, 7);

        storage.stop();
    });
}

#[test]
fn scanner_batch_explosion_across_segments() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 4096).await;
        let prefetcher = log.prefetcher();

        // Mix standalone and batch commands.
        let batch1 = MqCommand::batch(&[
            MqCommand::publish(10, &[make_flat_msg(b"b1-a")]),
            MqCommand::publish(20, &[make_flat_msg(b"b1-b")]),
        ]);
        let batch2 = MqCommand::batch(&[
            MqCommand::publish(10, &[make_flat_msg(b"b2-a")]),
            MqCommand::publish(30, &[make_flat_msg(b"b2-b")]),
        ]);

        let entries = vec![
            make_entry(1, 1, MqCommand::publish(10, &[make_flat_msg(b"solo1")])),
            make_entry(2, 1, batch1),
            make_entry(3, 1, MqCommand::publish(20, &[make_flat_msg(b"solo2")])),
            make_entry(4, 1, batch2),
            make_entry(5, 1, MqCommand::publish(10, &[make_flat_msg(b"solo3")])),
        ];
        append_entries(&mut log, entries).await;

        let mut scanner = MqSegmentScanner::from_start(prefetcher);

        // Collect all: 1 + 2 + 1 + 2 + 1 = 7 commands total.
        let mut log_indices = Vec::new();
        let mut tags = Vec::new();
        while let Some(rec) = scanner.next_record() {
            log_indices.push(rec.log_index);
            tags.push(rec.command.tag());
        }
        assert_eq!(log_indices.len(), 7);

        // Check log_index grouping.
        assert_eq!(log_indices, vec![1, 2, 2, 3, 4, 4, 5]);

        // Check tags: solo=Publish, batch1=[Publish,Publish], solo=Publish,
        //             batch2=[Publish,Enqueue], solo=Publish
        assert_eq!(tags[0], MqCommand::TAG_PUBLISH);
        assert_eq!(tags[1], MqCommand::TAG_PUBLISH);
        assert_eq!(tags[2], MqCommand::TAG_PUBLISH);
        assert_eq!(tags[3], MqCommand::TAG_PUBLISH);
        assert_eq!(tags[4], MqCommand::TAG_PUBLISH);
        assert_eq!(tags[5], MqCommand::TAG_PUBLISH);
        assert_eq!(tags[6], MqCommand::TAG_PUBLISH);

        storage.stop();
    });
}

#[test]
fn scanner_start_from_specific_segment() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 4096).await;
        let prefetcher = log.prefetcher();

        let mut entries = Vec::new();
        for i in 1..=20 {
            entries.push(make_entry(
                i,
                1,
                MqCommand::publish(1, &[make_flat_msg(b"x")]),
            ));
        }
        append_entries(&mut log, entries).await;

        // Find the active segment — start from there (should get a subset).
        let active_id = prefetcher.active_segment_id();
        let mut scanner = MqSegmentScanner::new(prefetcher, active_id);

        let mut count = 0;
        while let Some(_rec) = scanner.next_record() {
            count += 1;
        }
        // Should get entries from the active segment only — fewer than 20.
        assert!(count > 0);
        assert!(count <= 20);

        // Full scan should get all 20.
        let mut full_scanner = MqSegmentScanner::from_start(log.prefetcher());
        let mut full_count = 0;
        while let Some(_rec) = full_scanner.next_record() {
            full_count += 1;
        }
        assert_eq!(full_count, 20);

        storage.stop();
    });
}

#[test]
fn scanner_empty_storage() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let mut scanner = MqSegmentScanner::from_start(prefetcher);
        assert!(scanner.next_record().is_none());

        storage.stop();
    });
}

// =============================================================================
// Point-read helper tests
// =============================================================================

#[test]
fn read_command_returns_correct_command() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let cmd = MqCommand::publish(42, &[make_flat_msg(b"hello")]);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        let result = read_command(&prefetcher, 1).unwrap();
        assert_eq!(result.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(result.as_publish().topic_id(), 42);

        storage.stop();
    });
}

#[test]
fn read_command_missing_index() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        append_entries(
            &mut log,
            vec![make_entry(
                1,
                1,
                MqCommand::publish(1, &[make_flat_msg(b"x")]),
            )],
        )
        .await;

        // Index 99 doesn't exist.
        assert!(read_command(&prefetcher, 99).is_none());

        storage.stop();
    });
}

#[test]
fn read_message_at_extracts_publish_payload() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let msg = make_flat_msg(b"payload-data");
        let cmd = MqCommand::publish(1, &[msg.clone()]);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        let result = read_message_at(&prefetcher, 1).unwrap();
        assert_eq!(&result[..], &msg[..]);

        storage.stop();
    });
}

#[test]
fn read_message_at_returns_none_for_non_publish() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        // A non-publish command — read_message_at should return None.
        let cmd = MqCommand::commit_offset(1, 1, 0);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        assert!(read_message_at(&prefetcher, 1).is_none());

        storage.stop();
    });
}

#[test]
fn read_message_at_returns_none_for_missing_index() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        assert!(read_message_at(&prefetcher, 1).is_none());

        storage.stop();
    });
}

#[test]
fn read_messages_at_into_multi_message_publish() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let msgs = vec![
            make_flat_msg(b"msg-1"),
            make_flat_msg(b"msg-2"),
            make_flat_msg(b"msg-3"),
        ];
        let cmd = MqCommand::publish(1, &msgs);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        let mut out = Vec::new();
        read_messages_at_into(&prefetcher, 1, &mut out);
        assert_eq!(out.len(), 3);
        assert_eq!(&out[0][..], &msgs[0][..]);
        assert_eq!(&out[1][..], &msgs[1][..]);
        assert_eq!(&out[2][..], &msgs[2][..]);

        storage.stop();
    });
}

#[test]
fn read_messages_at_into_clears_output_vec() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let cmd = MqCommand::publish(1, &[make_flat_msg(b"msg")]);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        let mut out = vec![Bytes::from_static(b"garbage")];
        read_messages_at_into(&prefetcher, 1, &mut out);
        assert_eq!(out.len(), 1); // garbage replaced with 1 message

        // Non-existent index should clear the vec.
        read_messages_at_into(&prefetcher, 99, &mut out);
        assert!(out.is_empty());

        storage.stop();
    });
}

#[test]
fn read_latest_topic_message_with_position() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        // Publish 3 messages to topic 42 in one entry.
        let msgs = vec![
            make_flat_msg(b"first"),
            make_flat_msg(b"second"),
            make_flat_msg(b"third"),
        ];
        let cmd = MqCommand::publish(42, &msgs);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        // Read message at position 0 (first).
        let result = read_latest_topic_message(&prefetcher, 42, (1, 0)).unwrap();
        assert_eq!(&result[..], &msgs[0][..]);

        // Read message at position 2 (third).
        let result = read_latest_topic_message(&prefetcher, 42, (1, 2)).unwrap();
        assert_eq!(&result[..], &msgs[2][..]);

        // Wrong topic_id should return None.
        assert!(read_latest_topic_message(&prefetcher, 99, (1, 0)).is_none());

        storage.stop();
    });
}

// =============================================================================
// MqReader tests
// =============================================================================

#[test]
fn mq_reader_read_message_at() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let msg = make_flat_msg(b"reader-test");
        let cmd = MqCommand::publish(1, &[msg.clone()]);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        let metadata = Arc::new(MqMetadata::default());
        let reader = MqReader::new(prefetcher, metadata);

        let result = reader.read_message_at(1).unwrap();
        assert_eq!(&result[..], &msg[..]);

        assert!(reader.read_message_at(99).is_none());

        storage.stop();
    });
}

#[test]
fn mq_reader_read_messages_at_into() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let msgs = vec![make_flat_msg(b"a"), make_flat_msg(b"b")];
        let cmd = MqCommand::publish(1, &msgs);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        let metadata = Arc::new(MqMetadata::default());
        let reader = MqReader::new(prefetcher, metadata);

        let mut out = Vec::new();
        reader.read_messages_at_into(1, &mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(&out[0][..], &msgs[0][..]);
        assert_eq!(&out[1][..], &msgs[1][..]);

        storage.stop();
    });
}

#[test]
fn mq_reader_list_topics_with_prefix() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let metadata = Arc::new(MqMetadata::default());
        metadata.insert_topic(Arc::new(TopicMeta::with_state(
            1,
            "events.clicks".into(),
            10,
            0,
            10,
            0,
            0,
            0,
        )));
        metadata.insert_topic(Arc::new(TopicMeta::with_state(
            2,
            "events.views".into(),
            5,
            0,
            5,
            0,
            0,
            0,
        )));
        metadata.insert_topic(Arc::new(TopicMeta::with_state(
            3,
            "logs.errors".into(),
            1,
            0,
            1,
            0,
            0,
            0,
        )));

        let reader = MqReader::new(prefetcher, metadata);

        let events = reader.list_topics_with_prefix("events");
        assert_eq!(events.len(), 2);

        let logs = reader.list_topics_with_prefix("logs");
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].1, 3);

        let all = reader.list_topics_with_prefix("");
        assert_eq!(all.len(), 3);

        let none = reader.list_topics_with_prefix("nonexistent");
        assert!(none.is_empty());

        storage.stop();
    });
}

#[test]
fn mq_reader_read_latest_topic_message() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let msgs = vec![make_flat_msg(b"retained-msg")];
        let cmd = MqCommand::publish(42, &msgs);
        append_entries(&mut log, vec![make_entry(1, 1, cmd)]).await;

        let metadata = Arc::new(MqMetadata::default());
        metadata.insert_topic(Arc::new(TopicMeta::with_state(
            42,
            "retained-topic".into(),
            1,
            0,
            1,
            0,
            1,
            0,
        )));

        let reader = MqReader::new(prefetcher, metadata);

        let result = reader.read_latest_topic_message(42).unwrap();
        assert_eq!(&result[..], &msgs[0][..]);

        // Non-existent topic.
        assert!(reader.read_latest_topic_message(99).is_none());

        storage.stop();
    });
}

#[test]
fn mq_reader_segment_cursor() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let entries = vec![
            make_entry(1, 1, MqCommand::publish(1, &[make_flat_msg(b"a")])),
            make_entry(2, 1, MqCommand::publish(2, &[make_flat_msg(b"b")])),
        ];
        append_entries(&mut log, entries).await;

        let metadata = Arc::new(MqMetadata::default());
        let reader = MqReader::new(prefetcher, metadata);

        let active_id = log.prefetcher().active_segment_id();
        let mut cursor = reader.segment_cursor(active_id).unwrap();

        let mut count = 0;
        while let Some(_rec) = cursor.next_record() {
            count += 1;
        }
        assert!(count > 0);

        // Non-existent segment returns None.
        assert!(reader.segment_cursor(999).is_none());

        storage.stop();
    });
}

// =============================================================================
// MqSegmentCursor with real storage
// =============================================================================

#[test]
fn cursor_from_real_segment_bytes() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let entries = vec![
            make_entry(1, 1, MqCommand::publish(10, &[make_flat_msg(b"hello")])),
            make_entry(2, 1, MqCommand::publish(20, &[make_flat_msg(b"world")])),
            make_entry(3, 1, MqCommand::publish(30, &[make_flat_msg(b"q")])),
        ];
        append_entries(&mut log, entries).await;

        let active_id = prefetcher.active_segment_id();
        let data = prefetcher.segment_bytes(active_id).unwrap();
        let mut cursor = MqSegmentCursor::new(active_id, data);

        let all = cursor.collect_all();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(all[0].command.as_publish().topic_id(), 10);
        assert_eq!(all[1].command.as_publish().topic_id(), 20);
        assert_eq!(all[2].command.tag(), MqCommand::TAG_PUBLISH);

        storage.stop();
    });
}

#[test]
fn cursor_entity_filter_with_real_storage() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let mut entries = Vec::new();
        for i in 1..=10 {
            let topic_id = if i % 2 == 0 { 100 } else { 200 };
            entries.push(make_entry(
                i,
                1,
                MqCommand::publish(topic_id, &[make_flat_msg(b"data")]),
            ));
        }
        append_entries(&mut log, entries).await;

        let active_id = prefetcher.active_segment_id();
        let data = prefetcher.segment_bytes(active_id).unwrap();
        let mut cursor = MqSegmentCursor::new(active_id, data);

        let filtered = cursor.collect_for_entity(MqCommand::TAG_PUBLISH, 100);
        assert_eq!(filtered.len(), 5);

        cursor.rewind();
        let filtered = cursor.collect_for_entity(MqCommand::TAG_PUBLISH, 200);
        assert_eq!(filtered.len(), 5);

        storage.stop();
    });
}

#[test]
fn cursor_batch_with_real_storage() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 1024 * 1024).await;
        let prefetcher = log.prefetcher();

        let batch = MqCommand::batch(&[
            MqCommand::publish(1, &[make_flat_msg(b"a")]),
            MqCommand::publish(2, &[make_flat_msg(b"b")]),
            MqCommand::publish(3, &[make_flat_msg(b"c")]),
        ]);
        let entries = vec![
            make_entry(1, 1, MqCommand::publish(0, &[make_flat_msg(b"solo")])),
            make_entry(2, 1, batch),
        ];
        append_entries(&mut log, entries).await;

        let active_id = prefetcher.active_segment_id();
        let data = prefetcher.segment_bytes(active_id).unwrap();
        let mut cursor = MqSegmentCursor::new(active_id, data);

        let all = cursor.collect_all();
        // 1 solo + 3 from batch = 4
        assert_eq!(all.len(), 4);
        assert_eq!(all[0].log_index, 1);
        assert_eq!(all[1].log_index, 2);
        assert_eq!(all[2].log_index, 2);
        assert_eq!(all[3].log_index, 2);
        assert_eq!(all[1].command.as_publish().topic_id(), 1);
        assert_eq!(all[2].command.as_publish().topic_id(), 2);
        assert_eq!(all[3].command.tag(), MqCommand::TAG_PUBLISH);

        storage.stop();
    });
}

// =============================================================================
// End-to-end: scanner + reader + metadata together
// =============================================================================

#[test]
fn end_to_end_scanner_and_reader() {
    run_async(async {
        let tmp = TempDir::new().unwrap();
        let (storage, mut log) = setup_storage(&tmp, 4096).await;
        let prefetcher = log.prefetcher();

        // Write 30 entries across multiple segments.
        let mut entries = Vec::new();
        for i in 1..=30 {
            let topic_id = i % 3;
            entries.push(make_entry(
                i,
                1,
                MqCommand::publish(topic_id, &[make_flat_msg(format!("msg-{i}").as_bytes())]),
            ));
        }
        append_entries(&mut log, entries).await;

        // Set up metadata.
        let metadata = Arc::new(MqMetadata::default());
        metadata.insert_topic(Arc::new(TopicMeta::with_state(
            0,
            "topic-0".into(),
            30,
            0,
            10,
            0,
            30,
            0,
        )));
        metadata.insert_topic(Arc::new(TopicMeta::with_state(
            1,
            "topic-1".into(),
            30,
            0,
            10,
            0,
            28,
            0,
        )));
        metadata.insert_topic(Arc::new(TopicMeta::with_state(
            2,
            "topic-2".into(),
            30,
            0,
            10,
            0,
            29,
            0,
        )));

        let reader = MqReader::new(prefetcher, metadata);

        // Scanner: count all records.
        let mut scanner = MqSegmentScanner::from_start(log.prefetcher());
        let mut total = 0;
        while scanner.next_record().is_some() {
            total += 1;
        }
        assert_eq!(total, 30);

        // Scanner with entity filter: count topic-1 records.
        let mut scanner = MqSegmentScanner::from_start(log.prefetcher());
        let mut topic1_count = 0;
        while scanner
            .next_record_for_entity(MqCommand::TAG_PUBLISH, 1)
            .is_some()
        {
            topic1_count += 1;
        }
        assert_eq!(topic1_count, 10);

        // Reader: point-read a specific message.
        let msg = reader.read_message_at(15).unwrap();
        assert!(!msg.is_empty());

        // Reader: list topics.
        let topics = reader.list_topics_with_prefix("topic-");
        assert_eq!(topics.len(), 3);

        // Reader: latest topic message.
        let latest = reader.read_latest_topic_message(0).unwrap();
        assert!(!latest.is_empty());

        storage.stop();
    });
}
