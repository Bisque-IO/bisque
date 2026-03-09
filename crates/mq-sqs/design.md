# bisque-mq-sqs Design

## Overview

SQS-compatible HTTP API adapter for bisque-mq. Exposes an AWS SQS-compatible JSON API
that translates SQS actions into `MqCommand`/`MqResponse` interactions via `MqWriteBatcher`.

## Architecture

```
HTTP Request (SQS JSON)
    │
    ▼
axum Router (POST /)
    │
    ▼
Action Dispatcher (parse Action header/param)
    │
    ▼
Action Handler (e.g., SendMessage → MqCommand::Enqueue)
    │
    ▼
MqWriteBatcher::submit(cmd) → MqResponse
    │
    ▼
SQS JSON Response
```

## SQS → bisque-mq Mapping

| SQS Action | MqCommand | Notes |
|---|---|---|
| CreateQueue | CreateQueue | Maps SQS attributes to QueueConfig |
| DeleteQueue | DeleteQueue | Resolved by queue URL → queue_id |
| SendMessage | Enqueue | Single message, MessageDeduplicationId → dedup_key |
| SendMessageBatch | Enqueue | Batch of messages |
| ReceiveMessage | Deliver | WaitTimeSeconds → long-poll |
| DeleteMessage | Ack | ReceiptHandle → message_id |
| DeleteMessageBatch | Ack (batch) | Multiple message_ids |
| ChangeMessageVisibility | ExtendVisibility | |
| ChangeMessageVisibilityBatch | ExtendVisibility (batch) | |
| PurgeQueue | PurgeQueue | |
| GetQueueAttributes | GetQueueAttributes | Read from QueueState |
| GetQueueUrl | Name→ID resolution | Uses MqRouter |
| ListQueues | Engine read | Iterate queue_names |
| SetQueueAttributes | UpdateQueue (not yet) | |
| TagQueue / UntagQueue / ListQueueTags | Not implemented | |

## Queue URL Scheme

SQS queue URLs follow: `http://{host}/{account_id}/{queue_name}`

We use: `http://{host}/sqs/{group_id}/{queue_name}`

The group_id maps to the Raft group. Queue name is resolved via name_hash.

## Message Attributes

SQS MessageAttributes are mapped to bisque-mq message headers:
- `MessageAttribute.Name` → header key
- `MessageAttribute.Value` → header value (binary or string)

SQS system attributes:
- `MessageDeduplicationId` → dedup_key
- `MessageGroupId` → message key (for FIFO ordering)
- `DelaySeconds` → per-message deliver_after override

## Receipt Handles

SQS uses opaque receipt handles for message acknowledgement.
We encode `{queue_id}:{message_id}` as the receipt handle (base64).

## Long Polling

SQS `WaitTimeSeconds` is implemented as a server-side loop:
1. Try Deliver immediately
2. If no messages, sleep briefly and retry up to WaitTimeSeconds
3. Return whatever is available when timeout expires

## FIFO Queues

SQS FIFO queues (.fifo suffix) map to:
- `QueueConfig::dedup_window_secs` = 300 (5 min, SQS default)
- Message group ordering via message key

## Dead Letter Queues

SQS RedrivePolicy maps to:
- `QueueConfig::dead_letter_queue_id`
- `QueueConfig::max_retries` (= maxReceiveCount)

## Open Questions

- **Authentication**: How to map SQS IAM-style auth to bisque tokens?
  Could use X-Amz-Security-Token header or basic auth.
- **Account/region mapping**: SQS URLs include account ID and region.
  Map account → tenant, region → ignored or → raft group?
- **SetQueueAttributes**: Need UpdateQueue MqCommand variant.
- **Message body encoding**: SQS supports both String and Binary.
  Map to MessagePayload.value with content-type header?
- **Batch error handling**: SQS returns per-entry success/failure.
  Current Ack is all-or-nothing.
- **ListQueues prefix filter**: Need engine accessor for queue name enumeration.
- **GetQueueAttributes completeness**: Need to expose all SQS-equivalent
  attributes (visibility timeout, delay, message count, etc.)
- **Max message size**: SQS limit is 256KB. Should we enforce?
- **Long poll efficiency**: Current Deliver is a Raft write. Long-poll
  should avoid hammering Raft. Consider a notify/watch mechanism.
