use bytes::Bytes;

// =============================================================================
// API Keys
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
}

impl ApiKey {
    pub fn from_i16(v: i16) -> Option<Self> {
        match v {
            0 => Some(Self::Produce),
            1 => Some(Self::Fetch),
            2 => Some(Self::ListOffsets),
            3 => Some(Self::Metadata),
            8 => Some(Self::OffsetCommit),
            9 => Some(Self::OffsetFetch),
            10 => Some(Self::FindCoordinator),
            11 => Some(Self::JoinGroup),
            12 => Some(Self::Heartbeat),
            13 => Some(Self::LeaveGroup),
            14 => Some(Self::SyncGroup),
            15 => Some(Self::DescribeGroups),
            16 => Some(Self::ListGroups),
            18 => Some(Self::ApiVersions),
            19 => Some(Self::CreateTopics),
            20 => Some(Self::DeleteTopics),
            _ => None,
        }
    }

    /// Returns the supported API version range `(min, max)`.
    pub fn version_range(self) -> (i16, i16) {
        match self {
            Self::Produce => (0, 3),
            Self::Fetch => (0, 4),
            Self::ListOffsets => (0, 1),
            Self::Metadata => (0, 1),
            Self::OffsetCommit => (0, 2),
            Self::OffsetFetch => (0, 1),
            Self::FindCoordinator => (0, 0),
            Self::JoinGroup => (0, 1),
            Self::Heartbeat => (0, 0),
            Self::LeaveGroup => (0, 0),
            Self::SyncGroup => (0, 0),
            Self::DescribeGroups => (0, 0),
            Self::ListGroups => (0, 0),
            Self::ApiVersions => (0, 0),
            Self::CreateTopics => (0, 0),
            Self::DeleteTopics => (0, 0),
        }
    }
}

// =============================================================================
// Error Codes
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
#[allow(dead_code)]
pub enum ErrorCode {
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    MessageTooLarge = 10,
    OffsetMetadataTooLarge = 12,
    GroupLoadInProgress = 14,
    GroupCoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    MemberIdRequired = 79,
    UnsupportedVersion = 35,
}

impl ErrorCode {
    pub fn as_i16(self) -> i16 {
        self as i16
    }
}

// =============================================================================
// Request Header
// =============================================================================

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

// =============================================================================
// Requests
// =============================================================================

#[derive(Debug, Clone)]
pub enum KafkaRequest {
    ApiVersions,
    Metadata(MetadataRequest),
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    ListOffsets(ListOffsetsRequest),
    FindCoordinator(FindCoordinatorRequest),
    JoinGroup(JoinGroupRequest),
    SyncGroup(SyncGroupRequest),
    Heartbeat(HeartbeatRequest),
    LeaveGroup(LeaveGroupRequest),
    OffsetCommit(OffsetCommitRequest),
    OffsetFetch(OffsetFetchRequest),
    CreateTopics(CreateTopicsRequest),
    DeleteTopics(DeleteTopicsRequest),
    DescribeGroups(DescribeGroupsRequest),
    ListGroups,
}

// -- Metadata --

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    /// `None` means all topics.
    pub topics: Option<Vec<String>>,
}

// -- Produce --

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicData>,
}

#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    pub topic_name: String,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    pub partition_index: i32,
    /// Raw record batch bytes (decoded separately).
    pub record_set: Option<Bytes>,
}

// -- Fetch --

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub topics: Vec<FetchTopicData>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicData {
    pub topic_name: String,
    pub partitions: Vec<FetchPartitionData>,
}

#[derive(Debug, Clone)]
pub struct FetchPartitionData {
    pub partition_index: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
}

// -- ListOffsets --

#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub topics: Vec<ListOffsetsTopicData>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsTopicData {
    pub topic_name: String,
    pub partitions: Vec<ListOffsetsPartitionData>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsPartitionData {
    pub partition_index: i32,
    /// -1 = latest, -2 = earliest.
    pub timestamp: i64,
}

// -- FindCoordinator --

#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    pub key: String,
    /// 0 = group, 1 = transaction.
    pub key_type: i8,
}

// -- JoinGroup --

#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub protocol_type: String,
    pub protocols: Vec<JoinGroupProtocol>,
}

#[derive(Debug, Clone)]
pub struct JoinGroupProtocol {
    pub name: String,
    pub metadata: Bytes,
}

// -- SyncGroup --

#[derive(Debug, Clone)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub assignments: Vec<SyncGroupAssignment>,
}

#[derive(Debug, Clone)]
pub struct SyncGroupAssignment {
    pub member_id: String,
    pub assignment: Bytes,
}

// -- Heartbeat --

#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

// -- LeaveGroup --

#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

// -- OffsetCommit --

#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub topics: Vec<OffsetCommitTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitTopicData {
    pub topic_name: String,
    pub partitions: Vec<OffsetCommitPartitionData>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitPartitionData {
    pub partition_index: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}

// -- OffsetFetch --

#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    pub topics: Vec<OffsetFetchTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchTopicData {
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

// -- CreateTopics --

#[derive(Debug, Clone)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreateTopicRequest>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone)]
pub struct CreateTopicRequest {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

// -- DeleteTopics --

#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

// -- DescribeGroups --

#[derive(Debug, Clone)]
pub struct DescribeGroupsRequest {
    pub group_ids: Vec<String>,
}

// =============================================================================
// Responses
// =============================================================================

#[derive(Debug, Clone)]
pub enum KafkaResponse {
    ApiVersions(ApiVersionsResponse),
    Metadata(MetadataResponse),
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    ListOffsets(ListOffsetsResponse),
    FindCoordinator(FindCoordinatorResponse),
    JoinGroup(JoinGroupResponse),
    SyncGroup(SyncGroupResponse),
    Heartbeat(HeartbeatResponse),
    LeaveGroup(LeaveGroupResponse),
    OffsetCommit(OffsetCommitResponse),
    OffsetFetch(OffsetFetchResponse),
    CreateTopics(CreateTopicsResponse),
    DeleteTopics(DeleteTopicsResponse),
    DescribeGroups(DescribeGroupsResponse),
    ListGroups(ListGroupsResponse),
}

// -- ApiVersions --

#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersionRange>,
}

#[derive(Debug, Clone)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

// -- Metadata --

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    pub brokers: Vec<BrokerMeta>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone)]
pub struct BrokerMeta {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

// -- Produce --

#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub topics: Vec<ProduceTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct ProduceTopicResponse {
    pub topic_name: String,
    pub partitions: Vec<ProducePartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
}

// -- Fetch --

#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub topics: Vec<FetchTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicResponse {
    pub topic_name: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    /// Encoded record batch bytes.
    pub record_set: Bytes,
}

// -- ListOffsets --

#[derive(Debug, Clone)]
pub struct ListOffsetsResponse {
    pub topics: Vec<ListOffsetsTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsTopicResponse {
    pub topic_name: String,
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

// -- FindCoordinator --

#[derive(Debug, Clone)]
pub struct FindCoordinatorResponse {
    pub error_code: i16,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

// -- JoinGroup --

#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinGroupMember>,
}

#[derive(Debug, Clone)]
pub struct JoinGroupMember {
    pub member_id: String,
    pub metadata: Bytes,
}

// -- SyncGroup --

#[derive(Debug, Clone)]
pub struct SyncGroupResponse {
    pub error_code: i16,
    pub assignment: Bytes,
}

// -- Heartbeat --

#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub error_code: i16,
}

// -- LeaveGroup --

#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    pub error_code: i16,
}

// -- OffsetCommit --

#[derive(Debug, Clone)]
pub struct OffsetCommitResponse {
    pub topics: Vec<OffsetCommitTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitTopicResponse {
    pub topic_name: String,
    pub partitions: Vec<OffsetCommitPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
}

// -- OffsetFetch --

#[derive(Debug, Clone)]
pub struct OffsetFetchResponse {
    pub topics: Vec<OffsetFetchTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchTopicResponse {
    pub topic_name: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub offset: i64,
    pub metadata: Option<String>,
    pub error_code: i16,
}

// -- CreateTopics --

#[derive(Debug, Clone)]
pub struct CreateTopicsResponse {
    pub topics: Vec<CreateTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicResponse {
    pub name: String,
    pub error_code: i16,
}

// -- DeleteTopics --

#[derive(Debug, Clone)]
pub struct DeleteTopicsResponse {
    pub topics: Vec<DeleteTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct DeleteTopicResponse {
    pub name: String,
    pub error_code: i16,
}

// -- DescribeGroups --

#[derive(Debug, Clone)]
pub struct DescribeGroupsResponse {
    pub groups: Vec<DescribedGroup>,
}

#[derive(Debug, Clone)]
pub struct DescribedGroup {
    pub error_code: i16,
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<DescribedGroupMember>,
}

#[derive(Debug, Clone)]
pub struct DescribedGroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Bytes,
    pub assignment: Bytes,
}

// -- ListGroups --

#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    pub error_code: i16,
    pub groups: Vec<ListedGroup>,
}

#[derive(Debug, Clone)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
}

// =============================================================================
// RecordBatch (Kafka v2 message format)
// =============================================================================

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl RecordBatch {
    pub fn new(base_offset: i64) -> Self {
        Self {
            base_offset,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub offset_delta: i32,
    pub timestamp_delta: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub key: String,
    pub value: Bytes,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_roundtrip() {
        let keys = [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
            ApiKey::Metadata,
            ApiKey::ApiVersions,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::SyncGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
        ];
        for key in keys {
            let i = key as i16;
            let decoded = ApiKey::from_i16(i).unwrap();
            assert_eq!(key, decoded);
        }
    }

    #[test]
    fn test_unknown_api_key() {
        assert!(ApiKey::from_i16(999).is_none());
        assert!(ApiKey::from_i16(-1).is_none());
    }

    #[test]
    fn test_error_code_values() {
        assert_eq!(ErrorCode::None.as_i16(), 0);
        assert_eq!(ErrorCode::UnknownTopicOrPartition.as_i16(), 3);
        assert_eq!(ErrorCode::RebalanceInProgress.as_i16(), 27);
        assert_eq!(ErrorCode::MemberIdRequired.as_i16(), 79);
    }

    #[test]
    fn test_record_batch_new() {
        let batch = RecordBatch::new(42);
        assert_eq!(batch.base_offset, 42);
        assert_eq!(batch.producer_id, -1);
        assert_eq!(batch.producer_epoch, -1);
        assert_eq!(batch.base_sequence, -1);
        assert!(batch.records.is_empty());
    }

    #[test]
    fn test_api_version_ranges() {
        let (min, max) = ApiKey::Produce.version_range();
        assert_eq!(min, 0);
        assert!(max >= 3);

        let (min, max) = ApiKey::ApiVersions.version_range();
        assert_eq!(min, 0);
        assert_eq!(max, 0);
    }
}
