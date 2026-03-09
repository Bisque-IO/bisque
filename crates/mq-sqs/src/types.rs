use serde::{Deserialize, Serialize};

// =============================================================================
// Request types
// =============================================================================

#[derive(Debug, Deserialize)]
#[serde(tag = "Action")]
pub enum SqsAction {
    CreateQueue(CreateQueueRequest),
    DeleteQueue(DeleteQueueRequest),
    GetQueueUrl(GetQueueUrlRequest),
    ListQueues(ListQueuesRequest),
    SendMessage(SendMessageRequest),
    SendMessageBatch(SendMessageBatchRequest),
    ReceiveMessage(ReceiveMessageRequest),
    DeleteMessage(DeleteMessageRequest),
    DeleteMessageBatch(DeleteMessageBatchRequest),
    ChangeMessageVisibility(ChangeMessageVisibilityRequest),
    ChangeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest),
    PurgeQueue(PurgeQueueRequest),
    GetQueueAttributes(GetQueueAttributesRequest),
    SetQueueAttributes(SetQueueAttributesRequest),
}

#[derive(Debug, Deserialize)]
pub struct CreateQueueRequest {
    #[serde(rename = "QueueName")]
    pub queue_name: String,
    #[serde(rename = "Attributes", default)]
    pub attributes: Option<QueueAttributes>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteQueueRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Deserialize)]
pub struct GetQueueUrlRequest {
    #[serde(rename = "QueueName")]
    pub queue_name: String,
}

#[derive(Debug, Deserialize)]
pub struct ListQueuesRequest {
    #[serde(rename = "QueueNamePrefix", default)]
    pub queue_name_prefix: Option<String>,
    #[serde(rename = "MaxResults", default)]
    pub max_results: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "MessageBody")]
    pub message_body: String,
    #[serde(rename = "DelaySeconds", default)]
    pub delay_seconds: Option<u32>,
    #[serde(rename = "MessageAttributes", default)]
    pub message_attributes: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(rename = "MessageDeduplicationId", default)]
    pub message_deduplication_id: Option<String>,
    #[serde(rename = "MessageGroupId", default)]
    pub message_group_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageBatchRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "Entries")]
    pub entries: Vec<SendMessageBatchEntry>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageBatchEntry {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "MessageBody")]
    pub message_body: String,
    #[serde(rename = "DelaySeconds", default)]
    pub delay_seconds: Option<u32>,
    #[serde(rename = "MessageDeduplicationId", default)]
    pub message_deduplication_id: Option<String>,
    #[serde(rename = "MessageGroupId", default)]
    pub message_group_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ReceiveMessageRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "MaxNumberOfMessages", default)]
    pub max_number_of_messages: Option<u32>,
    #[serde(rename = "VisibilityTimeout", default)]
    pub visibility_timeout: Option<u32>,
    #[serde(rename = "WaitTimeSeconds", default)]
    pub wait_time_seconds: Option<u32>,
    #[serde(rename = "AttributeNames", default)]
    pub attribute_names: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteMessageRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "ReceiptHandle")]
    pub receipt_handle: String,
}

#[derive(Debug, Deserialize)]
pub struct DeleteMessageBatchRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "Entries")]
    pub entries: Vec<DeleteMessageBatchEntry>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteMessageBatchEntry {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "ReceiptHandle")]
    pub receipt_handle: String,
}

#[derive(Debug, Deserialize)]
pub struct ChangeMessageVisibilityRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "ReceiptHandle")]
    pub receipt_handle: String,
    #[serde(rename = "VisibilityTimeout")]
    pub visibility_timeout: u32,
}

#[derive(Debug, Deserialize)]
pub struct ChangeMessageVisibilityBatchRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "Entries")]
    pub entries: Vec<ChangeMessageVisibilityBatchEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ChangeMessageVisibilityBatchEntry {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "ReceiptHandle")]
    pub receipt_handle: String,
    #[serde(rename = "VisibilityTimeout")]
    pub visibility_timeout: u32,
}

#[derive(Debug, Deserialize)]
pub struct PurgeQueueRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Deserialize)]
pub struct GetQueueAttributesRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "AttributeNames", default)]
    pub attribute_names: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct SetQueueAttributesRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
    #[serde(rename = "Attributes")]
    pub attributes: QueueAttributes,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueueAttributes {
    #[serde(rename = "VisibilityTimeout", default)]
    pub visibility_timeout: Option<String>,
    #[serde(rename = "MaximumMessageSize", default)]
    pub maximum_message_size: Option<String>,
    #[serde(rename = "MessageRetentionPeriod", default)]
    pub message_retention_period: Option<String>,
    #[serde(rename = "DelaySeconds", default)]
    pub delay_seconds: Option<String>,
    #[serde(rename = "ReceiveMessageWaitTimeSeconds", default)]
    pub receive_message_wait_time_seconds: Option<String>,
    #[serde(rename = "RedrivePolicy", default)]
    pub redrive_policy: Option<String>,
    #[serde(rename = "FifoQueue", default)]
    pub fifo_queue: Option<String>,
    #[serde(rename = "ContentBasedDeduplication", default)]
    pub content_based_deduplication: Option<String>,
    #[serde(rename = "DeduplicationScope", default)]
    pub deduplication_scope: Option<String>,
}

// =============================================================================
// Response types
// =============================================================================

#[derive(Debug, Serialize)]
pub struct CreateQueueResponse {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Serialize)]
pub struct GetQueueUrlResponse {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Serialize)]
pub struct ListQueuesResponse {
    #[serde(rename = "QueueUrls")]
    pub queue_urls: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SendMessageResponse {
    #[serde(rename = "MessageId")]
    pub message_id: String,
    #[serde(rename = "MD5OfMessageBody")]
    pub md5_of_message_body: String,
    #[serde(rename = "SequenceNumber", skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SendMessageBatchResponse {
    #[serde(rename = "Successful")]
    pub successful: Vec<SendMessageBatchResultEntry>,
    #[serde(rename = "Failed")]
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Debug, Serialize)]
pub struct SendMessageBatchResultEntry {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "MessageId")]
    pub message_id: String,
    #[serde(rename = "MD5OfMessageBody")]
    pub md5_of_message_body: String,
}

#[derive(Debug, Serialize)]
pub struct BatchResultErrorEntry {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "SenderFault")]
    pub sender_fault: bool,
}

#[derive(Debug, Serialize)]
pub struct ReceiveMessageResponse {
    #[serde(rename = "Messages")]
    pub messages: Vec<SqsMessage>,
}

#[derive(Debug, Serialize)]
pub struct SqsMessage {
    #[serde(rename = "MessageId")]
    pub message_id: String,
    #[serde(rename = "ReceiptHandle")]
    pub receipt_handle: String,
    #[serde(rename = "Body")]
    pub body: String,
    #[serde(rename = "MD5OfBody")]
    pub md5_of_body: String,
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct DeleteMessageBatchResponse {
    #[serde(rename = "Successful")]
    pub successful: Vec<DeleteMessageBatchResultEntry>,
    #[serde(rename = "Failed")]
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Debug, Serialize)]
pub struct DeleteMessageBatchResultEntry {
    #[serde(rename = "Id")]
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct ChangeMessageVisibilityBatchResponse {
    #[serde(rename = "Successful")]
    pub successful: Vec<ChangeMessageVisibilityBatchResultEntry>,
    #[serde(rename = "Failed")]
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Debug, Serialize)]
pub struct ChangeMessageVisibilityBatchResultEntry {
    #[serde(rename = "Id")]
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct GetQueueAttributesResponse {
    #[serde(rename = "Attributes")]
    pub attributes: serde_json::Map<String, serde_json::Value>,
}
