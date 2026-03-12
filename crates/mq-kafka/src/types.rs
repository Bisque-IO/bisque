use std::fmt;

use bytes::Bytes;
use smallvec::SmallVec;

// =============================================================================
// WireString — zero-copy UTF-8 string backed by Bytes
// =============================================================================

/// A validated UTF-8 string backed by `Bytes` for zero-copy decoding.
///
/// Created during Kafka protocol decoding by slicing into the input `Bytes`
/// buffer. Cloning is O(1) (atomic refcount increment, no data copy).
/// Dereferences to `&str` for transparent use in all string-accepting APIs.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct WireString(Bytes);

/// Hash as `str` so that `HashMap<WireString, _>` can be queried with `&str`
/// via `Borrow<str>`. Derived `Hash` would go through `Bytes → [u8]` which
/// differs from `str`'s hash (which appends a 0xFF separator).
impl std::hash::Hash for WireString {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl WireString {
    /// Create from a `Bytes` that is already known to be valid UTF-8.
    ///
    /// # Safety (logical)
    /// The caller must ensure the bytes are valid UTF-8. This is enforced
    /// by the codec's decode path which validates before constructing.
    #[inline]
    pub fn from_utf8_unchecked(b: Bytes) -> Self {
        Self(b)
    }

    /// Create from a `&'static str` — no allocation.
    #[inline]
    pub fn from_static(s: &'static str) -> Self {
        Self(Bytes::from_static(s.as_bytes()))
    }

    /// Create an empty WireString.
    #[inline]
    pub fn empty() -> Self {
        Self(Bytes::new())
    }

    /// The underlying bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume into the underlying `Bytes`.
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// The underlying `Bytes` handle (cheap clone).
    #[inline]
    pub fn bytes(&self) -> &Bytes {
        &self.0
    }

    /// View as `&str`. Zero-cost since UTF-8 was validated at construction.
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: validated at decode time by read_string / read_nullable_string
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::ops::Deref for WireString {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for WireString {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for WireString {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for WireString {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Debug for WireString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl fmt::Display for WireString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for WireString {
    #[inline]
    fn from(s: String) -> Self {
        Self(Bytes::from(s))
    }
}

impl From<&str> for WireString {
    #[inline]
    fn from(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl PartialEq<str> for WireString {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for WireString {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for WireString {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

// =============================================================================
// BytesCursor — position-tracked reader over Bytes for zero-copy slicing
// =============================================================================

/// A cursor over a `Bytes` buffer that tracks read position and supports
/// zero-copy sub-slicing via `Bytes::slice()`.
pub struct BytesCursor {
    data: Bytes,
    pos: usize,
}

impl BytesCursor {
    #[inline]
    pub fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.pos..]
    }

    #[inline]
    pub fn advance(&mut self, n: usize) {
        self.pos += n;
    }

    /// Zero-copy slice of `len` bytes at current position. Does NOT advance.
    #[inline]
    pub fn slice(&self, len: usize) -> Bytes {
        self.data.slice(self.pos..self.pos + len)
    }

    /// Zero-copy slice + advance.
    #[inline]
    pub fn read_slice(&mut self, len: usize) -> Bytes {
        let b = self.data.slice(self.pos..self.pos + len);
        self.pos += len;
        b
    }

    #[inline]
    pub fn read_u8(&mut self) -> u8 {
        let v = self.data[self.pos];
        self.pos += 1;
        v
    }

    #[inline]
    pub fn read_i8(&mut self) -> i8 {
        self.read_u8() as i8
    }

    #[inline]
    pub fn read_i16(&mut self) -> i16 {
        let v = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        v
    }

    #[inline]
    pub fn read_i32(&mut self) -> i32 {
        let s = self.pos;
        let v = i32::from_be_bytes([
            self.data[s],
            self.data[s + 1],
            self.data[s + 2],
            self.data[s + 3],
        ]);
        self.pos += 4;
        v
    }

    #[inline]
    pub fn read_i64(&mut self) -> i64 {
        let s = self.pos;
        let v = i64::from_be_bytes([
            self.data[s],
            self.data[s + 1],
            self.data[s + 2],
            self.data[s + 3],
            self.data[s + 4],
            self.data[s + 5],
            self.data[s + 6],
            self.data[s + 7],
        ]);
        self.pos += 8;
        v
    }

    #[inline]
    pub fn read_u32(&mut self) -> u32 {
        let s = self.pos;
        let v = u32::from_be_bytes([
            self.data[s],
            self.data[s + 1],
            self.data[s + 2],
            self.data[s + 3],
        ]);
        self.pos += 4;
        v
    }
}

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
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    TxnOffsetCommit = 28,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    OffsetForLeaderEpoch = 23,
    DeleteGroups = 42,
    OffsetDelete = 47,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeLogDirs = 35,
    IncrementalAlterConfigs = 44,
    WriteTxnMarkers = 27,
    DescribeUserScramCredentials = 50,
    AlterUserScramCredentials = 51,
    DescribeQuorum = 55,
    UpdateFeatures = 57,
    DescribeCluster = 60,
    DescribeProducers = 61,
    UnregisterBroker = 64,
    DescribeTransactions = 65,
    ListTransactions = 66,
    ConsumerGroupHeartbeat = 68,
    ConsumerGroupDescribe = 69,
    GetTelemetrySubscriptions = 71,
    PushTelemetry = 72,
    ListConfigResources = 74,
    DescribeTopicPartitions = 75,
    ShareGroupHeartbeat = 76,
    ShareGroupDescribe = 77,
    ShareFetch = 78,
    ShareAcknowledge = 79,
    AddRaftVoter = 80,
    RemoveRaftVoter = 81,
    InitializeShareGroupState = 83,
    ReadShareGroupState = 84,
    WriteShareGroupState = 85,
    DeleteShareGroupState = 86,
    ReadShareGroupStateSummary = 87,
    StreamsGroupHeartbeat = 88,
    StreamsGroupDescribe = 89,
    DescribeShareGroupOffsets = 90,
    AlterShareGroupOffsets = 91,
    DeleteShareGroupOffsets = 92,
    AlterReplicaLogDirs = 34,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    ElectLeaders = 43,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
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
            17 => Some(Self::SaslHandshake),
            18 => Some(Self::ApiVersions),
            19 => Some(Self::CreateTopics),
            20 => Some(Self::DeleteTopics),
            21 => Some(Self::DeleteRecords),
            22 => Some(Self::InitProducerId),
            23 => Some(Self::OffsetForLeaderEpoch),
            24 => Some(Self::AddPartitionsToTxn),
            25 => Some(Self::AddOffsetsToTxn),
            26 => Some(Self::EndTxn),
            28 => Some(Self::TxnOffsetCommit),
            32 => Some(Self::DescribeConfigs),
            33 => Some(Self::AlterConfigs),
            36 => Some(Self::SaslAuthenticate),
            37 => Some(Self::CreatePartitions),
            42 => Some(Self::DeleteGroups),
            29 => Some(Self::DescribeAcls),
            30 => Some(Self::CreateAcls),
            31 => Some(Self::DeleteAcls),
            35 => Some(Self::DescribeLogDirs),
            44 => Some(Self::IncrementalAlterConfigs),
            27 => Some(Self::WriteTxnMarkers),
            34 => Some(Self::AlterReplicaLogDirs),
            38 => Some(Self::CreateDelegationToken),
            39 => Some(Self::RenewDelegationToken),
            40 => Some(Self::ExpireDelegationToken),
            41 => Some(Self::DescribeDelegationToken),
            43 => Some(Self::ElectLeaders),
            45 => Some(Self::AlterPartitionReassignments),
            46 => Some(Self::ListPartitionReassignments),
            47 => Some(Self::OffsetDelete),
            48 => Some(Self::DescribeClientQuotas),
            49 => Some(Self::AlterClientQuotas),
            50 => Some(Self::DescribeUserScramCredentials),
            51 => Some(Self::AlterUserScramCredentials),
            55 => Some(Self::DescribeQuorum),
            57 => Some(Self::UpdateFeatures),
            60 => Some(Self::DescribeCluster),
            61 => Some(Self::DescribeProducers),
            64 => Some(Self::UnregisterBroker),
            65 => Some(Self::DescribeTransactions),
            66 => Some(Self::ListTransactions),
            68 => Some(Self::ConsumerGroupHeartbeat),
            69 => Some(Self::ConsumerGroupDescribe),
            71 => Some(Self::GetTelemetrySubscriptions),
            72 => Some(Self::PushTelemetry),
            74 => Some(Self::ListConfigResources),
            75 => Some(Self::DescribeTopicPartitions),
            76 => Some(Self::ShareGroupHeartbeat),
            77 => Some(Self::ShareGroupDescribe),
            78 => Some(Self::ShareFetch),
            79 => Some(Self::ShareAcknowledge),
            80 => Some(Self::AddRaftVoter),
            81 => Some(Self::RemoveRaftVoter),
            83 => Some(Self::InitializeShareGroupState),
            84 => Some(Self::ReadShareGroupState),
            85 => Some(Self::WriteShareGroupState),
            86 => Some(Self::DeleteShareGroupState),
            87 => Some(Self::ReadShareGroupStateSummary),
            88 => Some(Self::StreamsGroupHeartbeat),
            89 => Some(Self::StreamsGroupDescribe),
            90 => Some(Self::DescribeShareGroupOffsets),
            91 => Some(Self::AlterShareGroupOffsets),
            92 => Some(Self::DeleteShareGroupOffsets),
            _ => None,
        }
    }

    /// Returns the supported API version range `(min, max)`.
    pub fn version_range(self) -> (i16, i16) {
        match self {
            Self::Produce => (0, 10),
            Self::Fetch => (0, 12),
            Self::ListOffsets => (0, 6),
            Self::Metadata => (0, 9),
            Self::OffsetCommit => (0, 8),
            Self::OffsetFetch => (0, 6),
            Self::FindCoordinator => (0, 3),
            Self::JoinGroup => (0, 6),
            Self::Heartbeat => (0, 4),
            Self::LeaveGroup => (0, 4),
            Self::SyncGroup => (0, 4),
            Self::DescribeGroups => (0, 5),
            Self::ListGroups => (0, 4),
            Self::SaslHandshake => (0, 1),
            Self::ApiVersions => (0, 3),
            Self::CreateTopics => (0, 5),
            Self::DeleteTopics => (0, 4),
            Self::DeleteRecords => (0, 2),
            Self::InitProducerId => (0, 2),
            Self::OffsetForLeaderEpoch => (0, 4),
            Self::AddPartitionsToTxn => (0, 3),
            Self::AddOffsetsToTxn => (0, 3),
            Self::EndTxn => (0, 3),
            Self::TxnOffsetCommit => (0, 3),
            Self::DescribeConfigs => (0, 4),
            Self::AlterConfigs => (0, 2),
            Self::SaslAuthenticate => (0, 2),
            Self::CreatePartitions => (0, 2),
            Self::DeleteGroups => (0, 2),
            Self::DescribeAcls => (0, 2),
            Self::CreateAcls => (0, 2),
            Self::DeleteAcls => (0, 2),
            Self::DescribeLogDirs => (0, 2),
            Self::IncrementalAlterConfigs => (0, 1),
            Self::WriteTxnMarkers => (0, 2),
            Self::AlterReplicaLogDirs => (0, 2),
            Self::CreateDelegationToken => (0, 3),
            Self::RenewDelegationToken => (0, 2),
            Self::ExpireDelegationToken => (0, 2),
            Self::DescribeDelegationToken => (0, 3),
            Self::ElectLeaders => (0, 2),
            Self::AlterPartitionReassignments => (0, 1),
            Self::ListPartitionReassignments => (0, 0),
            Self::DescribeClientQuotas => (0, 1),
            Self::AlterClientQuotas => (0, 1),
            Self::DescribeUserScramCredentials => (0, 0),
            Self::AlterUserScramCredentials => (0, 0),
            Self::DescribeQuorum => (0, 2),
            Self::UpdateFeatures => (0, 2),
            Self::OffsetDelete => (0, 0),
            Self::DescribeCluster => (0, 0),
            Self::DescribeProducers => (0, 0),
            Self::UnregisterBroker => (0, 0),
            Self::DescribeTransactions => (0, 0),
            Self::ListTransactions => (0, 2),
            Self::ConsumerGroupHeartbeat => (0, 1),
            Self::ConsumerGroupDescribe => (0, 1),
            Self::GetTelemetrySubscriptions => (0, 0),
            Self::PushTelemetry => (0, 0),
            Self::ListConfigResources => (0, 1),
            Self::DescribeTopicPartitions => (0, 0),
            Self::ShareGroupHeartbeat => (0, 1),
            Self::ShareGroupDescribe => (0, 1),
            Self::ShareFetch => (0, 2),
            Self::ShareAcknowledge => (0, 2),
            Self::AddRaftVoter => (0, 1),
            Self::RemoveRaftVoter => (0, 0),
            Self::InitializeShareGroupState => (0, 0),
            Self::ReadShareGroupState => (0, 0),
            Self::WriteShareGroupState => (0, 1),
            Self::DeleteShareGroupState => (0, 0),
            Self::ReadShareGroupStateSummary => (0, 1),
            Self::StreamsGroupHeartbeat => (0, 0),
            Self::StreamsGroupDescribe => (0, 0),
            Self::DescribeShareGroupOffsets => (0, 1),
            Self::AlterShareGroupOffsets => (0, 0),
            Self::DeleteShareGroupOffsets => (0, 0),
        }
    }

    /// Returns the first API version that uses flexible encoding (request header v2,
    /// response header v1, compact encoding). Returns `None` if the API never uses
    /// flexible encoding.
    pub fn flexible_version(self) -> Option<i16> {
        match self {
            Self::Produce => Some(9),
            Self::Fetch => Some(12),
            Self::ListOffsets => Some(6),
            Self::Metadata => Some(9),
            Self::OffsetCommit => Some(8),
            Self::OffsetFetch => Some(6),
            Self::FindCoordinator => Some(3),
            Self::JoinGroup => Some(6),
            Self::Heartbeat => Some(4),
            Self::LeaveGroup => Some(4),
            Self::SyncGroup => Some(4),
            Self::DescribeGroups => Some(5),
            Self::ListGroups => Some(3),
            Self::SaslHandshake => None,
            Self::ApiVersions => Some(3),
            Self::CreateTopics => Some(5),
            Self::DeleteTopics => Some(4),
            Self::DeleteRecords => Some(2),
            Self::InitProducerId => Some(2),
            Self::OffsetForLeaderEpoch => None,
            Self::AddPartitionsToTxn => Some(3),
            Self::AddOffsetsToTxn => Some(3),
            Self::EndTxn => Some(3),
            Self::TxnOffsetCommit => Some(3),
            Self::DescribeConfigs => Some(4),
            Self::AlterConfigs => Some(2),
            Self::SaslAuthenticate => Some(2),
            Self::CreatePartitions => Some(2),
            Self::DeleteGroups => Some(2),
            Self::DescribeAcls => Some(2),
            Self::CreateAcls => Some(2),
            Self::DeleteAcls => Some(2),
            Self::DescribeLogDirs => Some(2),
            Self::IncrementalAlterConfigs => Some(1),
            Self::WriteTxnMarkers => Some(1),
            Self::AlterReplicaLogDirs => Some(2),
            Self::CreateDelegationToken => Some(2),
            Self::RenewDelegationToken => Some(2),
            Self::ExpireDelegationToken => Some(2),
            Self::DescribeDelegationToken => Some(2),
            Self::ElectLeaders => Some(2),
            Self::AlterPartitionReassignments => Some(0),
            Self::ListPartitionReassignments => Some(0),
            Self::DescribeClientQuotas => Some(1),
            Self::AlterClientQuotas => Some(1),
            Self::DescribeUserScramCredentials => Some(0),
            Self::AlterUserScramCredentials => Some(0),
            Self::DescribeQuorum => Some(0),
            Self::UpdateFeatures => Some(0),
            Self::OffsetDelete => None,
            Self::DescribeCluster => None,
            Self::DescribeProducers => Some(0),
            Self::UnregisterBroker => Some(0),
            Self::DescribeTransactions => Some(0),
            Self::ListTransactions => Some(0),
            Self::ConsumerGroupHeartbeat => Some(0),
            Self::ConsumerGroupDescribe => Some(0),
            Self::GetTelemetrySubscriptions => Some(0),
            Self::PushTelemetry => Some(0),
            Self::ListConfigResources => Some(0),
            Self::DescribeTopicPartitions => Some(0),
            Self::ShareGroupHeartbeat => Some(0),
            Self::ShareGroupDescribe => Some(0),
            Self::ShareFetch => Some(0),
            Self::ShareAcknowledge => Some(0),
            Self::AddRaftVoter => Some(0),
            Self::RemoveRaftVoter => Some(0),
            Self::InitializeShareGroupState => Some(0),
            Self::ReadShareGroupState => Some(0),
            Self::WriteShareGroupState => Some(0),
            Self::DeleteShareGroupState => Some(0),
            Self::ReadShareGroupStateSummary => Some(0),
            Self::StreamsGroupHeartbeat => Some(0),
            Self::StreamsGroupDescribe => Some(0),
            Self::DescribeShareGroupOffsets => Some(0),
            Self::AlterShareGroupOffsets => Some(0),
            Self::DeleteShareGroupOffsets => Some(0),
        }
    }

    /// Returns `true` if the given API version uses flexible encoding.
    pub fn is_flexible(self, api_version: i16) -> bool {
        self.flexible_version()
            .map_or(false, |fv| api_version >= fv)
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
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidTransactionTimeout = 38,
    ConcurrentTransactions = 39,
    TransactionCoordinatorFenced = 40,
    TransactionalIdAuthorizationFailed = 53,
    SecurityDisabled = 54,
    OperationNotAttempted = 55,
    GroupNotEmpty = 68,
    MemberIdRequired = 79,
    GroupIdNotFound = 69,
    InvalidProducerEpoch = 47,
    UnknownProducerId = 59,
    ProducerFenced = 90,
    InvalidTxnState = 48,
    SaslAuthenticationFailed = 58,
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
    pub client_id: Option<WireString>,
}

// =============================================================================
// Requests — all string fields use WireString (zero-copy from wire)
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
    SaslHandshake(SaslHandshakeRequest),
    SaslAuthenticate(SaslAuthenticateRequest),
    DeleteRecords(DeleteRecordsRequest),
    InitProducerId(InitProducerIdRequest),
    AddPartitionsToTxn(AddPartitionsToTxnRequest),
    AddOffsetsToTxn(AddOffsetsToTxnRequest),
    EndTxn(EndTxnRequest),
    TxnOffsetCommit(TxnOffsetCommitRequest),
    DescribeConfigs(DescribeConfigsRequest),
    AlterConfigs(AlterConfigsRequest),
    CreatePartitions(CreatePartitionsRequest),
    DeleteGroups(DeleteGroupsRequest),
    OffsetDelete(OffsetDeleteRequest),
    OffsetForLeaderEpoch(OffsetForLeaderEpochRequest),
    IncrementalAlterConfigs(IncrementalAlterConfigsRequest),
    DescribeAcls(DescribeAclsRequest),
    CreateAcls(CreateAclsRequest),
    DeleteAcls(DeleteAclsRequest),
    DescribeLogDirs(DescribeLogDirsRequest),
    DescribeUserScramCredentials(DescribeUserScramCredentialsRequest),
    AlterUserScramCredentials(AlterUserScramCredentialsRequest),
    DescribeCluster(DescribeClusterRequest),
    WriteTxnMarkers(WriteTxnMarkersRequest),
    AlterReplicaLogDirs(AlterReplicaLogDirsRequest),
    CreateDelegationToken(CreateDelegationTokenRequest),
    RenewDelegationToken(RenewDelegationTokenRequest),
    ExpireDelegationToken(ExpireDelegationTokenRequest),
    DescribeDelegationToken(DescribeDelegationTokenRequest),
    ElectLeaders(ElectLeadersRequest),
    AlterPartitionReassignments(AlterPartitionReassignmentsRequest),
    ListPartitionReassignments(ListPartitionReassignmentsRequest),
    DescribeClientQuotas(DescribeClientQuotasRequest),
    AlterClientQuotas(AlterClientQuotasRequest),
    DescribeQuorum(DescribeQuorumRequest),
    UpdateFeatures(UpdateFeaturesRequest),
    DescribeProducers(DescribeProducersRequest),
    UnregisterBroker(UnregisterBrokerRequest),
    DescribeTransactions(DescribeTransactionsRequest),
    ListTransactions(ListTransactionsRequest),
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest),
    ConsumerGroupDescribe(ConsumerGroupDescribeRequest),
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsRequest),
    PushTelemetry(PushTelemetryRequest),
    ListConfigResources(ListConfigResourcesRequest),
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
    ShareGroupHeartbeat(ShareGroupHeartbeatRequest),
    ShareGroupDescribe(ShareGroupDescribeRequest),
    ShareFetch(ShareFetchRequest),
    ShareAcknowledge(ShareAcknowledgeRequest),
    AddRaftVoter(AddRaftVoterRequest),
    RemoveRaftVoter(RemoveRaftVoterRequest),
    InitializeShareGroupState(InitializeShareGroupStateRequest),
    ReadShareGroupState(ReadShareGroupStateRequest),
    WriteShareGroupState(WriteShareGroupStateRequest),
    DeleteShareGroupState(DeleteShareGroupStateRequest),
    ReadShareGroupStateSummary(ReadShareGroupStateSummaryRequest),
    StreamsGroupHeartbeat(StreamsGroupHeartbeatRequest),
    StreamsGroupDescribe(StreamsGroupDescribeRequest),
    DescribeShareGroupOffsets(DescribeShareGroupOffsetsRequest),
    AlterShareGroupOffsets(AlterShareGroupOffsetsRequest),
    DeleteShareGroupOffsets(DeleteShareGroupOffsetsRequest),
}

// -- Metadata --

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    /// `None` means all topics.
    pub topics: Option<Vec<WireString>>,
}

// -- Produce --

#[derive(Debug, Clone, Default)]
pub struct ProduceRequest {
    pub transactional_id: Option<WireString>, // v3+
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicData>,
}

#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    pub partition_index: i32,
    /// Raw record batch bytes (decoded separately). Zero-copy from wire.
    pub record_set: Option<Bytes>,
}

// -- Fetch --

#[derive(Debug, Clone, Default)]
pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,      // v3+
    pub isolation_level: i8, // v4+
    pub session_id: i32,     // v7+
    pub session_epoch: i32,  // v7+
    pub topics: Vec<FetchTopicData>,
    pub forgotten_topics: Vec<FetchForgottenTopic>, // v7+
    pub rack_id: Option<WireString>,                // v11+
}

#[derive(Debug, Clone, Default)]
pub struct FetchForgottenTopic {
    pub topic_name: WireString,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<FetchPartitionData>,
}

#[derive(Debug, Clone, Default)]
pub struct FetchPartitionData {
    pub partition_index: i32,
    pub current_leader_epoch: i32, // v9+
    pub fetch_offset: i64,
    pub log_start_offset: i64, // v5+
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
    pub topic_name: WireString,
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
    pub key: WireString,
    /// 0 = group, 1 = transaction.
    pub key_type: i8,
}

// -- JoinGroup --

#[derive(Debug, Clone, Default)]
pub struct JoinGroupRequest {
    pub group_id: WireString,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>, // v5+
    pub protocol_type: WireString,
    pub protocols: Vec<JoinGroupProtocol>,
}

#[derive(Debug, Clone, Default)]
pub struct JoinGroupProtocol {
    pub name: WireString,
    pub metadata: Bytes,
}

// -- SyncGroup --

#[derive(Debug, Clone, Default)]
pub struct SyncGroupRequest {
    pub group_id: WireString,
    pub generation_id: i32,
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>, // v3+
    pub assignments: Vec<SyncGroupAssignment>,
}

#[derive(Debug, Clone, Default)]
pub struct SyncGroupAssignment {
    pub member_id: WireString,
    pub assignment: Bytes,
}

// -- Heartbeat --

#[derive(Debug, Clone, Default)]
pub struct HeartbeatRequest {
    pub group_id: WireString,
    pub generation_id: i32,
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>, // v3+
}

// -- LeaveGroup --

#[derive(Debug, Clone, Default)]
pub struct LeaveGroupRequest {
    pub group_id: WireString,
    pub member_id: WireString,
    pub members: Vec<LeaveGroupMember>, // v3+ replaces single member_id
}

#[derive(Debug, Clone, Default)]
pub struct LeaveGroupMember {
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>,
}

// -- OffsetCommit --

#[derive(Debug, Clone, Default)]
pub struct OffsetCommitRequest {
    pub group_id: WireString,
    pub generation_id: i32,
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>, // v7+
    pub topics: Vec<OffsetCommitTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<OffsetCommitPartitionData>,
}

#[derive(Debug, Clone, Default)]
pub struct OffsetCommitPartitionData {
    pub partition_index: i32,
    pub offset: i64,
    pub committed_leader_epoch: i32, // v6+
    pub metadata: Option<WireString>,
}

// -- OffsetFetch --

#[derive(Debug, Clone, Default)]
pub struct OffsetFetchRequest {
    pub group_id: WireString,
    pub topics: Vec<OffsetFetchTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchTopicData {
    pub topic_name: WireString,
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
    pub name: WireString,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

// -- DeleteTopics --

#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    pub topic_names: Vec<WireString>,
    pub timeout_ms: i32,
}

// -- DescribeGroups --

#[derive(Debug, Clone)]
pub struct DescribeGroupsRequest {
    pub group_ids: Vec<WireString>,
}

// -- SaslHandshake --

#[derive(Debug, Clone)]
pub struct SaslHandshakeRequest {
    pub mechanism: WireString,
}

// -- SaslAuthenticate --

#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequest {
    pub auth_bytes: Bytes,
}

// -- DeleteRecords --

#[derive(Debug, Clone)]
pub struct DeleteRecordsRequest {
    pub topics: Vec<DeleteRecordsTopicData>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<DeleteRecordsPartitionData>,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsPartitionData {
    pub partition_index: i32,
    pub offset: i64,
}

// -- InitProducerId --

#[derive(Debug, Clone)]
pub struct InitProducerIdRequest {
    pub transactional_id: Option<WireString>,
    pub transaction_timeout_ms: i32,
}

// -- AddPartitionsToTxn --

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequest {
    pub transactional_id: WireString,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<AddPartitionsToTxnTopicData>,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<i32>,
}

// -- AddOffsetsToTxn --

#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnRequest {
    pub transactional_id: WireString,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub group_id: WireString,
}

// -- EndTxn --

#[derive(Debug, Clone)]
pub struct EndTxnRequest {
    pub transactional_id: WireString,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub committed: bool,
}

// -- TxnOffsetCommit --

#[derive(Debug, Clone, Default)]
pub struct TxnOffsetCommitRequest {
    pub transactional_id: WireString,
    pub group_id: WireString,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<TxnOffsetCommitTopicData>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<TxnOffsetCommitPartitionData>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitPartitionData {
    pub partition_index: i32,
    pub offset: i64,
    pub metadata: Option<WireString>,
}

// -- DescribeConfigs --

#[derive(Debug, Clone)]
pub struct DescribeConfigsRequest {
    pub resources: Vec<DescribeConfigsResource>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsResource {
    /// 2 = topic, 4 = broker
    pub resource_type: i8,
    pub resource_name: WireString,
    pub config_names: Option<Vec<WireString>>,
}

// -- AlterConfigs --

#[derive(Debug, Clone)]
pub struct AlterConfigsRequest {
    pub resources: Vec<AlterConfigsResource>,
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsResource {
    pub resource_type: i8,
    pub resource_name: WireString,
    pub configs: Vec<AlterConfigEntry>,
}

#[derive(Debug, Clone)]
pub struct AlterConfigEntry {
    pub name: WireString,
    pub value: Option<WireString>,
}

// -- IncrementalAlterConfigs --

#[derive(Debug, Clone, Default)]
pub struct IncrementalAlterConfigsRequest {
    pub resources: Vec<IncrementalAlterConfigsResource>,
    pub validate_only: bool,
}

#[derive(Debug, Clone, Default)]
pub struct IncrementalAlterConfigsResource {
    pub resource_type: i8,
    pub resource_name: WireString,
    pub configs: Vec<IncrementalAlterConfigEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct IncrementalAlterConfigEntry {
    pub name: WireString,
    pub config_operation: i8,
    pub value: Option<WireString>,
}

// -- DescribeAcls --

#[derive(Debug, Clone, Default)]
pub struct DescribeAclsRequest {
    pub resource_type_filter: i8,
    pub resource_name_filter: Option<WireString>,
    pub pattern_type_filter: i8,
    pub principal_filter: Option<WireString>,
    pub host_filter: Option<WireString>,
    pub operation: i8,
    pub permission_type: i8,
}

// -- CreateAcls --

#[derive(Debug, Clone, Default)]
pub struct CreateAclsRequest {
    pub creations: Vec<AclCreation>,
}

#[derive(Debug, Clone, Default)]
pub struct AclCreation {
    pub resource_type: i8,
    pub resource_name: WireString,
    pub resource_pattern_type: i8,
    pub principal: WireString,
    pub host: WireString,
    pub operation: i8,
    pub permission_type: i8,
}

// -- DeleteAcls --

#[derive(Debug, Clone, Default)]
pub struct DeleteAclsRequest {
    pub filters: Vec<AclFilter>,
}

#[derive(Debug, Clone, Default)]
pub struct AclFilter {
    pub resource_type_filter: i8,
    pub resource_name_filter: Option<WireString>,
    pub pattern_type_filter: i8,
    pub principal_filter: Option<WireString>,
    pub host_filter: Option<WireString>,
    pub operation: i8,
    pub permission_type: i8,
}

// -- DescribeLogDirs --

#[derive(Debug, Clone, Default)]
pub struct DescribeLogDirsRequest {
    pub topics: Option<Vec<DescribeLogDirsTopic>>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribeLogDirsTopic {
    pub topic: WireString,
    pub partitions: Vec<i32>,
}

// -- DescribeUserScramCredentials --

#[derive(Debug, Clone, Default)]
pub struct DescribeUserScramCredentialsRequest {
    pub users: Option<Vec<WireString>>,
}

// -- AlterUserScramCredentials --

#[derive(Debug, Clone, Default)]
pub struct AlterUserScramCredentialsRequest {
    pub upsertions: Vec<ScramCredentialUpsertion>,
    pub deletions: Vec<ScramCredentialDeletion>,
}

#[derive(Debug, Clone, Default)]
pub struct ScramCredentialUpsertion {
    pub name: WireString,
    pub mechanism: i8,
    pub iterations: i32,
    pub salt: Bytes,
    pub salted_password: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct ScramCredentialDeletion {
    pub name: WireString,
    pub mechanism: i8,
}

// -- CreatePartitions --

#[derive(Debug, Clone)]
pub struct CreatePartitionsRequest {
    pub topics: Vec<CreatePartitionsTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsTopic {
    pub name: WireString,
    pub count: i32,
}

// -- DeleteGroups --

#[derive(Debug, Clone)]
pub struct DeleteGroupsRequest {
    pub group_ids: Vec<WireString>,
}

// -- OffsetDelete --

#[derive(Debug, Clone)]
pub struct OffsetDeleteRequest {
    pub group_id: WireString,
    pub topics: Vec<OffsetDeleteTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeleteTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<OffsetDeletePartitionData>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeletePartitionData {
    pub partition_index: i32,
}

// -- OffsetForLeaderEpoch --

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochRequest {
    pub topics: Vec<OffsetForLeaderEpochTopicData>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopicData {
    pub topic_name: WireString,
    pub partitions: Vec<OffsetForLeaderEpochPartitionData>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartitionData {
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub leader_epoch: i32,
}

// -- DescribeCluster --

#[derive(Debug, Clone)]
pub struct DescribeClusterRequest {
    pub include_cluster_authorized_operations: bool,
}

// -- WriteTxnMarkers --

#[derive(Debug, Clone, Default)]
pub struct WriteTxnMarkersRequest {
    pub data: Bytes,
}

// -- AlterReplicaLogDirs --

#[derive(Debug, Clone, Default)]
pub struct AlterReplicaLogDirsRequest {
    pub data: Bytes,
}

// -- CreateDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct CreateDelegationTokenRequest {
    pub data: Bytes,
}

// -- RenewDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct RenewDelegationTokenRequest {
    pub data: Bytes,
}

// -- ExpireDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct ExpireDelegationTokenRequest {
    pub data: Bytes,
}

// -- DescribeDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct DescribeDelegationTokenRequest {
    pub data: Bytes,
}

// -- ElectLeaders --

#[derive(Debug, Clone, Default)]
pub struct ElectLeadersRequest {
    pub data: Bytes,
}

// -- AlterPartitionReassignments --

#[derive(Debug, Clone, Default)]
pub struct AlterPartitionReassignmentsRequest {
    pub data: Bytes,
}

// -- ListPartitionReassignments --

#[derive(Debug, Clone, Default)]
pub struct ListPartitionReassignmentsRequest {
    pub data: Bytes,
}

// -- DescribeClientQuotas --

#[derive(Debug, Clone, Default)]
pub struct DescribeClientQuotasRequest {
    pub data: Bytes,
}

// -- AlterClientQuotas --

#[derive(Debug, Clone, Default)]
pub struct AlterClientQuotasRequest {
    pub data: Bytes,
}

// -- DescribeQuorum --

#[derive(Debug, Clone, Default)]
pub struct DescribeQuorumRequest {
    pub data: Bytes,
}

// -- UpdateFeatures --

#[derive(Debug, Clone, Default)]
pub struct UpdateFeaturesRequest {
    pub data: Bytes,
}

// -- DescribeProducers --

#[derive(Debug, Clone, Default)]
pub struct DescribeProducersRequest {
    pub data: Bytes,
}

// -- UnregisterBroker --

#[derive(Debug, Clone, Default)]
pub struct UnregisterBrokerRequest {
    pub data: Bytes,
}

// -- DescribeTransactions --

#[derive(Debug, Clone, Default)]
pub struct DescribeTransactionsRequest {
    pub data: Bytes,
}

// -- ListTransactions --

#[derive(Debug, Clone, Default)]
pub struct ListTransactionsRequest {
    pub data: Bytes,
}

// -- ConsumerGroupHeartbeat --

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroupHeartbeatRequest {
    pub data: Bytes,
}

// -- ConsumerGroupDescribe --

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroupDescribeRequest {
    pub data: Bytes,
}

// -- GetTelemetrySubscriptions --

#[derive(Debug, Clone, Default)]
pub struct GetTelemetrySubscriptionsRequest {
    pub data: Bytes,
}

// -- PushTelemetry --

#[derive(Debug, Clone, Default)]
pub struct PushTelemetryRequest {
    pub data: Bytes,
}

// -- ListConfigResources --

#[derive(Debug, Clone, Default)]
pub struct ListConfigResourcesRequest {
    pub data: Bytes,
}

// -- DescribeTopicPartitions --

#[derive(Debug, Clone, Default)]
pub struct DescribeTopicPartitionsRequest {
    pub data: Bytes,
}

// -- ShareGroupHeartbeat --

#[derive(Debug, Clone, Default)]
pub struct ShareGroupHeartbeatRequest {
    pub data: Bytes,
}

// -- ShareGroupDescribe --

#[derive(Debug, Clone, Default)]
pub struct ShareGroupDescribeRequest {
    pub data: Bytes,
}

// -- ShareFetch --

#[derive(Debug, Clone, Default)]
pub struct ShareFetchRequest {
    pub data: Bytes,
}

// -- ShareAcknowledge --

#[derive(Debug, Clone, Default)]
pub struct ShareAcknowledgeRequest {
    pub data: Bytes,
}

// -- AddRaftVoter --

#[derive(Debug, Clone, Default)]
pub struct AddRaftVoterRequest {
    pub data: Bytes,
}

// -- RemoveRaftVoter --

#[derive(Debug, Clone, Default)]
pub struct RemoveRaftVoterRequest {
    pub data: Bytes,
}

// -- InitializeShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct InitializeShareGroupStateRequest {
    pub data: Bytes,
}

// -- ReadShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct ReadShareGroupStateRequest {
    pub data: Bytes,
}

// -- WriteShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct WriteShareGroupStateRequest {
    pub data: Bytes,
}

// -- DeleteShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct DeleteShareGroupStateRequest {
    pub data: Bytes,
}

// -- ReadShareGroupStateSummary --

#[derive(Debug, Clone, Default)]
pub struct ReadShareGroupStateSummaryRequest {
    pub data: Bytes,
}

// -- StreamsGroupHeartbeat --

#[derive(Debug, Clone, Default)]
pub struct StreamsGroupHeartbeatRequest {
    pub data: Bytes,
}

// -- StreamsGroupDescribe --

#[derive(Debug, Clone, Default)]
pub struct StreamsGroupDescribeRequest {
    pub data: Bytes,
}

// -- DescribeShareGroupOffsets --

#[derive(Debug, Clone, Default)]
pub struct DescribeShareGroupOffsetsRequest {
    pub data: Bytes,
}

// -- AlterShareGroupOffsets --

#[derive(Debug, Clone, Default)]
pub struct AlterShareGroupOffsetsRequest {
    pub data: Bytes,
}

// -- DeleteShareGroupOffsets --

#[derive(Debug, Clone, Default)]
pub struct DeleteShareGroupOffsetsRequest {
    pub data: Bytes,
}

// =============================================================================
// Responses — WireString for string fields (zero-copy echo from requests)
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
    SaslHandshake(SaslHandshakeResponse),
    SaslAuthenticate(SaslAuthenticateResponse),
    DeleteRecords(DeleteRecordsResponse),
    InitProducerId(InitProducerIdResponse),
    AddPartitionsToTxn(AddPartitionsToTxnResponse),
    AddOffsetsToTxn(AddOffsetsToTxnResponse),
    EndTxn(EndTxnResponse),
    TxnOffsetCommit(TxnOffsetCommitResponse),
    DescribeConfigs(DescribeConfigsResponse),
    AlterConfigs(AlterConfigsResponse),
    CreatePartitions(CreatePartitionsResponse),
    DeleteGroups(DeleteGroupsResponse),
    OffsetDelete(OffsetDeleteResponse),
    OffsetForLeaderEpoch(OffsetForLeaderEpochResponse),
    IncrementalAlterConfigs(IncrementalAlterConfigsResponse),
    DescribeAcls(DescribeAclsResponse),
    CreateAcls(CreateAclsResponse),
    DeleteAcls(DeleteAclsResponse),
    DescribeLogDirs(DescribeLogDirsResponse),
    DescribeUserScramCredentials(DescribeUserScramCredentialsResponse),
    AlterUserScramCredentials(AlterUserScramCredentialsResponse),
    DescribeCluster(DescribeClusterResponse),
    WriteTxnMarkers(WriteTxnMarkersResponse),
    AlterReplicaLogDirs(AlterReplicaLogDirsResponse),
    CreateDelegationToken(CreateDelegationTokenResponse),
    RenewDelegationToken(RenewDelegationTokenResponse),
    ExpireDelegationToken(ExpireDelegationTokenResponse),
    DescribeDelegationToken(DescribeDelegationTokenResponse),
    ElectLeaders(ElectLeadersResponse),
    AlterPartitionReassignments(AlterPartitionReassignmentsResponse),
    ListPartitionReassignments(ListPartitionReassignmentsResponse),
    DescribeClientQuotas(DescribeClientQuotasResponse),
    AlterClientQuotas(AlterClientQuotasResponse),
    DescribeQuorum(DescribeQuorumResponse),
    UpdateFeatures(UpdateFeaturesResponse),
    DescribeProducers(DescribeProducersResponse),
    UnregisterBroker(UnregisterBrokerResponse),
    DescribeTransactions(DescribeTransactionsResponse),
    ListTransactions(ListTransactionsResponse),
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatResponse),
    ConsumerGroupDescribe(ConsumerGroupDescribeResponse),
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsResponse),
    PushTelemetry(PushTelemetryResponse),
    ListConfigResources(ListConfigResourcesResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
    ShareGroupHeartbeat(ShareGroupHeartbeatResponse),
    ShareGroupDescribe(ShareGroupDescribeResponse),
    ShareFetch(ShareFetchResponse),
    ShareAcknowledge(ShareAcknowledgeResponse),
    AddRaftVoter(AddRaftVoterResponse),
    RemoveRaftVoter(RemoveRaftVoterResponse),
    InitializeShareGroupState(InitializeShareGroupStateResponse),
    ReadShareGroupState(ReadShareGroupStateResponse),
    WriteShareGroupState(WriteShareGroupStateResponse),
    DeleteShareGroupState(DeleteShareGroupStateResponse),
    ReadShareGroupStateSummary(ReadShareGroupStateSummaryResponse),
    StreamsGroupHeartbeat(StreamsGroupHeartbeatResponse),
    StreamsGroupDescribe(StreamsGroupDescribeResponse),
    DescribeShareGroupOffsets(DescribeShareGroupOffsetsResponse),
    AlterShareGroupOffsets(AlterShareGroupOffsetsResponse),
    DeleteShareGroupOffsets(DeleteShareGroupOffsetsResponse),
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
    pub cluster_id: WireString,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone)]
pub struct BrokerMeta {
    pub node_id: i32,
    pub host: WireString,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: WireString,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader: i32,
    pub replicas: SmallVec<[i32; 1]>,
    pub isr: SmallVec<[i32; 1]>,
}

// -- Produce --

#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub topics: Vec<ProduceTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct ProduceTopicResponse {
    pub topic_name: WireString,
    pub partitions: Vec<ProducePartitionResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct ProducePartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64, // v2+
    pub log_start_offset: i64,   // v5+
}

// -- Fetch --

#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub topics: Vec<FetchTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicResponse {
    pub topic_name: WireString,
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,     // v4+
    pub log_start_offset: i64,       // v5+
    pub preferred_read_replica: i32, // v11+
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
    pub topic_name: WireString,
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
    pub host: WireString,
    pub port: i32,
}

// -- JoinGroup --

#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_name: WireString,
    pub leader: WireString,
    pub member_id: WireString,
    pub members: Vec<JoinGroupMember>,
}

#[derive(Debug, Clone, Default)]
pub struct JoinGroupMember {
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>, // v5+
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

#[derive(Debug, Clone, Default)]
pub struct LeaveGroupResponse {
    pub error_code: i16,
    pub members: Vec<LeaveGroupResponseMember>, // v3+
}

#[derive(Debug, Clone, Default)]
pub struct LeaveGroupResponseMember {
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>,
    pub error_code: i16,
}

// -- OffsetCommit --

#[derive(Debug, Clone)]
pub struct OffsetCommitResponse {
    pub topics: Vec<OffsetCommitTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitTopicResponse {
    pub topic_name: WireString,
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
    pub topic_name: WireString,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub offset: i64,
    pub committed_leader_epoch: i32, // v5+
    pub metadata: Option<WireString>,
    pub error_code: i16,
}

// -- CreateTopics --

#[derive(Debug, Clone)]
pub struct CreateTopicsResponse {
    pub topics: Vec<CreateTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicResponse {
    pub name: WireString,
    pub error_code: i16,
}

// -- DeleteTopics --

#[derive(Debug, Clone)]
pub struct DeleteTopicsResponse {
    pub topics: Vec<DeleteTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct DeleteTopicResponse {
    pub name: WireString,
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
    pub group_id: WireString,
    pub state: WireString,
    pub protocol_type: WireString,
    pub protocol: WireString,
    pub members: Vec<DescribedGroupMember>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribedGroupMember {
    pub member_id: WireString,
    pub group_instance_id: Option<WireString>, // v4+
    pub client_id: WireString,
    pub client_host: WireString,
    pub metadata: Bytes,
    pub assignment: Bytes,
}

// -- ListGroups --

#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    pub error_code: i16,
    pub groups: Vec<ListedGroup>,
}

#[derive(Debug, Clone, Default)]
pub struct ListedGroup {
    pub group_id: WireString,
    pub protocol_type: WireString,
    pub group_state: WireString, // v4+
}

// -- SaslHandshake --

#[derive(Debug, Clone)]
pub struct SaslHandshakeResponse {
    pub error_code: i16,
    pub mechanisms: Vec<WireString>,
}

// -- SaslAuthenticate --

#[derive(Debug, Clone, Default)]
pub struct SaslAuthenticateResponse {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub auth_bytes: Bytes,
    pub session_lifetime_ms: i64, // v1+
}

// -- DeleteRecords --

#[derive(Debug, Clone)]
pub struct DeleteRecordsResponse {
    pub topics: Vec<DeleteRecordsTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsTopicResponse {
    pub topic_name: WireString,
    pub partitions: Vec<DeleteRecordsPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsPartitionResponse {
    pub partition_index: i32,
    pub low_watermark: i64,
    pub error_code: i16,
}

// -- InitProducerId --

#[derive(Debug, Clone)]
pub struct InitProducerIdResponse {
    pub error_code: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

// -- AddPartitionsToTxn --

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponse {
    pub topics: Vec<AddPartitionsToTxnTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnTopicResponse {
    pub topic_name: WireString,
    pub partitions: Vec<AddPartitionsToTxnPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
}

// -- AddOffsetsToTxn --

#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnResponse {
    pub error_code: i16,
}

// -- EndTxn --

#[derive(Debug, Clone)]
pub struct EndTxnResponse {
    pub error_code: i16,
}

// -- TxnOffsetCommit --

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitResponse {
    pub topics: Vec<TxnOffsetCommitTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitTopicResponse {
    pub topic_name: WireString,
    pub partitions: Vec<TxnOffsetCommitPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
}

// -- DescribeConfigs --

#[derive(Debug, Clone)]
pub struct DescribeConfigsResponse {
    pub resources: Vec<DescribeConfigsResourceResult>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsResourceResult {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub resource_type: i8,
    pub resource_name: WireString,
    pub configs: Vec<DescribeConfigEntry>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigEntry {
    pub name: WireString,
    pub value: Option<WireString>,
    pub read_only: bool,
    pub is_default: bool,
    pub is_sensitive: bool,
}

// -- AlterConfigs --

#[derive(Debug, Clone)]
pub struct AlterConfigsResponse {
    pub resources: Vec<AlterConfigsResourceResult>,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsResourceResult {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub resource_type: i8,
    pub resource_name: WireString,
}

// -- IncrementalAlterConfigs --

#[derive(Debug, Clone, Default)]
pub struct IncrementalAlterConfigsResponse {
    pub resources: Vec<IncrementalAlterConfigsResourceResult>,
}

#[derive(Debug, Clone, Default)]
pub struct IncrementalAlterConfigsResourceResult {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub resource_type: i8,
    pub resource_name: WireString,
}

// -- DescribeAcls --

#[derive(Debug, Clone, Default)]
pub struct DescribeAclsResponse {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub resources: Vec<DescribeAclsResource>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribeAclsResource {
    pub resource_type: i8,
    pub resource_name: WireString,
    pub pattern_type: i8,
    pub acls: Vec<DescribeAclsAcl>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribeAclsAcl {
    pub principal: WireString,
    pub host: WireString,
    pub operation: i8,
    pub permission_type: i8,
}

// -- CreateAcls --

#[derive(Debug, Clone, Default)]
pub struct CreateAclsResponse {
    pub results: Vec<CreateAclsResult>,
}

#[derive(Debug, Clone, Default)]
pub struct CreateAclsResult {
    pub error_code: i16,
    pub error_message: Option<WireString>,
}

// -- DeleteAcls --

#[derive(Debug, Clone, Default)]
pub struct DeleteAclsResponse {
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

#[derive(Debug, Clone, Default)]
pub struct DeleteAclsFilterResult {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,
}

#[derive(Debug, Clone, Default)]
pub struct DeleteAclsMatchingAcl {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub resource_type: i8,
    pub resource_name: WireString,
    pub resource_pattern_type: i8,
    pub principal: WireString,
    pub host: WireString,
    pub operation: i8,
    pub permission_type: i8,
}

// -- DescribeLogDirs --

#[derive(Debug, Clone, Default)]
pub struct DescribeLogDirsResponse {
    pub results: Vec<DescribeLogDirsResult>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribeLogDirsResult {
    pub error_code: i16,
    pub log_dir: WireString,
    pub topics: Vec<DescribeLogDirsTopicResult>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribeLogDirsTopicResult {
    pub name: WireString,
    pub partitions: Vec<DescribeLogDirsPartitionResult>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribeLogDirsPartitionResult {
    pub partition_index: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
}

// -- DescribeUserScramCredentials --

#[derive(Debug, Clone, Default)]
pub struct DescribeUserScramCredentialsResponse {
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub results: Vec<DescribeUserScramCredentialsResult>,
}

#[derive(Debug, Clone, Default)]
pub struct DescribeUserScramCredentialsResult {
    pub user: WireString,
    pub error_code: i16,
    pub error_message: Option<WireString>,
    pub credential_infos: Vec<ScramCredentialInfo>,
}

#[derive(Debug, Clone, Default)]
pub struct ScramCredentialInfo {
    pub mechanism: i8,
    pub iterations: i32,
}

// -- AlterUserScramCredentials --

#[derive(Debug, Clone, Default)]
pub struct AlterUserScramCredentialsResponse {
    pub results: Vec<AlterUserScramCredentialsResult>,
}

#[derive(Debug, Clone, Default)]
pub struct AlterUserScramCredentialsResult {
    pub user: WireString,
    pub error_code: i16,
    pub error_message: Option<WireString>,
}

// -- CreatePartitions --

#[derive(Debug, Clone)]
pub struct CreatePartitionsResponse {
    pub topics: Vec<CreatePartitionsTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsTopicResponse {
    pub name: WireString,
    pub error_code: i16,
    pub error_message: Option<WireString>,
}

// -- DeleteGroups --

#[derive(Debug, Clone)]
pub struct DeleteGroupsResponse {
    pub results: Vec<DeleteGroupResult>,
}

#[derive(Debug, Clone)]
pub struct DeleteGroupResult {
    pub group_id: WireString,
    pub error_code: i16,
}

// -- OffsetDelete --

#[derive(Debug, Clone)]
pub struct OffsetDeleteResponse {
    pub error_code: i16,
    pub topics: Vec<OffsetDeleteTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeleteTopicResponse {
    pub topic_name: WireString,
    pub partitions: Vec<OffsetDeletePartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeletePartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
}

// -- OffsetForLeaderEpoch --

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochResponse {
    pub topics: Vec<OffsetForLeaderEpochTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopicResponse {
    pub topic_name: WireString,
    pub partitions: Vec<OffsetForLeaderEpochPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartitionResponse {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_epoch: i32,
    pub end_offset: i64,
}

// -- DescribeCluster --

#[derive(Debug, Clone)]
pub struct DescribeClusterResponse {
    pub error_code: i16,
    pub cluster_id: WireString,
    pub controller_id: i32,
    pub brokers: Vec<BrokerMeta>,
    pub cluster_authorized_operations: i32,
}

// -- WriteTxnMarkers --

#[derive(Debug, Clone, Default)]
pub struct WriteTxnMarkersResponse {
    pub error_code: i16,
}

// -- AlterReplicaLogDirs --

#[derive(Debug, Clone, Default)]
pub struct AlterReplicaLogDirsResponse {
    pub error_code: i16,
}

// -- CreateDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct CreateDelegationTokenResponse {
    pub error_code: i16,
}

// -- RenewDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct RenewDelegationTokenResponse {
    pub error_code: i16,
}

// -- ExpireDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct ExpireDelegationTokenResponse {
    pub error_code: i16,
}

// -- DescribeDelegationToken --

#[derive(Debug, Clone, Default)]
pub struct DescribeDelegationTokenResponse {
    pub error_code: i16,
}

// -- ElectLeaders --

#[derive(Debug, Clone, Default)]
pub struct ElectLeadersResponse {
    pub error_code: i16,
}

// -- AlterPartitionReassignments --

#[derive(Debug, Clone, Default)]
pub struct AlterPartitionReassignmentsResponse {
    pub error_code: i16,
}

// -- ListPartitionReassignments --

#[derive(Debug, Clone, Default)]
pub struct ListPartitionReassignmentsResponse {
    pub error_code: i16,
}

// -- DescribeClientQuotas --

#[derive(Debug, Clone, Default)]
pub struct DescribeClientQuotasResponse {
    pub error_code: i16,
}

// -- AlterClientQuotas --

#[derive(Debug, Clone, Default)]
pub struct AlterClientQuotasResponse {
    pub error_code: i16,
}

// -- DescribeQuorum --

#[derive(Debug, Clone, Default)]
pub struct DescribeQuorumResponse {
    pub error_code: i16,
}

// -- UpdateFeatures --

#[derive(Debug, Clone, Default)]
pub struct UpdateFeaturesResponse {
    pub error_code: i16,
}

// -- DescribeProducers --

#[derive(Debug, Clone, Default)]
pub struct DescribeProducersResponse {
    pub error_code: i16,
}

// -- UnregisterBroker --

#[derive(Debug, Clone, Default)]
pub struct UnregisterBrokerResponse {
    pub error_code: i16,
}

// -- DescribeTransactions --

#[derive(Debug, Clone, Default)]
pub struct DescribeTransactionsResponse {
    pub error_code: i16,
}

// -- ListTransactions --

#[derive(Debug, Clone, Default)]
pub struct ListTransactionsResponse {
    pub error_code: i16,
}

// -- ConsumerGroupHeartbeat --

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroupHeartbeatResponse {
    pub error_code: i16,
}

// -- ConsumerGroupDescribe --

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroupDescribeResponse {
    pub error_code: i16,
}

// -- GetTelemetrySubscriptions --

#[derive(Debug, Clone, Default)]
pub struct GetTelemetrySubscriptionsResponse {
    pub error_code: i16,
}

// -- PushTelemetry --

#[derive(Debug, Clone, Default)]
pub struct PushTelemetryResponse {
    pub error_code: i16,
}

// -- ListConfigResources --

#[derive(Debug, Clone, Default)]
pub struct ListConfigResourcesResponse {
    pub error_code: i16,
}

// -- DescribeTopicPartitions --

#[derive(Debug, Clone, Default)]
pub struct DescribeTopicPartitionsResponse {
    pub error_code: i16,
}

// -- ShareGroupHeartbeat --

#[derive(Debug, Clone, Default)]
pub struct ShareGroupHeartbeatResponse {
    pub error_code: i16,
}

// -- ShareGroupDescribe --

#[derive(Debug, Clone, Default)]
pub struct ShareGroupDescribeResponse {
    pub error_code: i16,
}

// -- ShareFetch --

#[derive(Debug, Clone, Default)]
pub struct ShareFetchResponse {
    pub error_code: i16,
}

// -- ShareAcknowledge --

#[derive(Debug, Clone, Default)]
pub struct ShareAcknowledgeResponse {
    pub error_code: i16,
}

// -- AddRaftVoter --

#[derive(Debug, Clone, Default)]
pub struct AddRaftVoterResponse {
    pub error_code: i16,
}

// -- RemoveRaftVoter --

#[derive(Debug, Clone, Default)]
pub struct RemoveRaftVoterResponse {
    pub error_code: i16,
}

// -- InitializeShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct InitializeShareGroupStateResponse {
    pub error_code: i16,
}

// -- ReadShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct ReadShareGroupStateResponse {
    pub error_code: i16,
}

// -- WriteShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct WriteShareGroupStateResponse {
    pub error_code: i16,
}

// -- DeleteShareGroupState --

#[derive(Debug, Clone, Default)]
pub struct DeleteShareGroupStateResponse {
    pub error_code: i16,
}

// -- ReadShareGroupStateSummary --

#[derive(Debug, Clone, Default)]
pub struct ReadShareGroupStateSummaryResponse {
    pub error_code: i16,
}

// -- StreamsGroupHeartbeat --

#[derive(Debug, Clone, Default)]
pub struct StreamsGroupHeartbeatResponse {
    pub error_code: i16,
}

// -- StreamsGroupDescribe --

#[derive(Debug, Clone, Default)]
pub struct StreamsGroupDescribeResponse {
    pub error_code: i16,
}

// -- DescribeShareGroupOffsets --

#[derive(Debug, Clone, Default)]
pub struct DescribeShareGroupOffsetsResponse {
    pub error_code: i16,
}

// -- AlterShareGroupOffsets --

#[derive(Debug, Clone, Default)]
pub struct AlterShareGroupOffsetsResponse {
    pub error_code: i16,
}

// -- DeleteShareGroupOffsets --

#[derive(Debug, Clone, Default)]
pub struct DeleteShareGroupOffsetsResponse {
    pub error_code: i16,
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
    /// Header key — stored as Bytes (zero-copy from wire).
    pub key: Bytes,
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
            ApiKey::SaslHandshake,
            ApiKey::SaslAuthenticate,
            ApiKey::DeleteRecords,
            ApiKey::InitProducerId,
            ApiKey::OffsetForLeaderEpoch,
            ApiKey::AddPartitionsToTxn,
            ApiKey::AddOffsetsToTxn,
            ApiKey::EndTxn,
            ApiKey::TxnOffsetCommit,
            ApiKey::DescribeConfigs,
            ApiKey::AlterConfigs,
            ApiKey::CreatePartitions,
            ApiKey::DeleteGroups,
            ApiKey::OffsetDelete,
            ApiKey::DescribeCluster,
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
        assert_eq!(max, 3);
    }

    #[test]
    fn test_wire_string_deref() {
        let ws = WireString::from("hello");
        assert_eq!(&*ws, "hello");
        assert_eq!(ws.as_str(), "hello");
        assert_eq!(ws.len(), 5);
    }

    #[test]
    fn test_wire_string_clone_is_cheap() {
        let ws = WireString::from("test");
        let ws2 = ws.clone();
        // Both point to same underlying data
        assert_eq!(ws.as_bytes().as_ptr(), ws2.as_bytes().as_ptr());
    }

    #[test]
    fn test_wire_string_from_static() {
        let ws = WireString::from_static("static");
        assert_eq!(&*ws, "static");
    }

    #[test]
    fn test_wire_string_eq() {
        let ws = WireString::from("abc");
        assert_eq!(ws, "abc");
        assert_eq!(ws, String::from("abc"));
    }

    #[test]
    fn test_bytes_cursor_basic() {
        let data = Bytes::from_static(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.remaining(), 10);
        let s = cur.read_slice(3);
        assert_eq!(&s[..], &[0, 1, 2]);
        assert_eq!(cur.remaining(), 7);
        assert_eq!(cur.read_u8(), 3);
        assert_eq!(cur.remaining(), 6);
    }

    #[test]
    fn test_bytes_cursor_zero_copy() {
        let data = Bytes::from(vec![10, 20, 30, 40, 50]);
        let mut cur = BytesCursor::new(data.clone());
        let slice = cur.read_slice(3);
        // slice should share underlying allocation with data
        assert_eq!(&slice[..], &[10, 20, 30]);
    }

    #[test]
    fn test_wire_string_empty() {
        let ws = WireString::empty();
        assert!(ws.is_empty());
        assert_eq!(ws.len(), 0);
        assert_eq!(ws.as_str(), "");
        assert_eq!(ws.as_bytes(), b"");
    }

    #[test]
    fn test_wire_string_into_bytes() {
        let ws = WireString::from("hello");
        let b = ws.into_bytes();
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn test_wire_string_bytes() {
        let ws = WireString::from("world");
        let b = ws.bytes();
        assert_eq!(&b[..], b"world");
    }

    #[test]
    fn test_wire_string_as_ref_str() {
        let ws = WireString::from("test");
        let s: &str = ws.as_ref();
        assert_eq!(s, "test");
    }

    #[test]
    fn test_wire_string_as_ref_u8() {
        let ws = WireString::from("abc");
        let b: &[u8] = ws.as_ref();
        assert_eq!(b, b"abc");
    }

    #[test]
    fn test_wire_string_borrow() {
        use std::borrow::Borrow;
        let ws = WireString::from("borrow");
        let s: &str = ws.borrow();
        assert_eq!(s, "borrow");
    }

    #[test]
    fn test_wire_string_display() {
        let ws = WireString::from("display");
        assert_eq!(format!("{ws}"), "display");
    }

    #[test]
    fn test_wire_string_from_string() {
        let s = String::from("owned");
        let ws = WireString::from(s);
        assert_eq!(ws.as_str(), "owned");
    }

    #[test]
    fn test_wire_string_partial_eq_ref_str() {
        let ws = WireString::from("cmp");
        assert_eq!(ws, "cmp");
        let s: &str = "cmp";
        assert!(ws == s);
    }

    #[test]
    fn test_wire_string_is_empty() {
        assert!(WireString::empty().is_empty());
        assert!(!WireString::from("x").is_empty());
    }

    #[test]
    fn test_wire_string_len() {
        assert_eq!(WireString::from("hello").len(), 5);
        assert_eq!(WireString::from("").len(), 0);
        assert_eq!(WireString::from("日本語").len(), 9); // 3 chars × 3 bytes
    }

    #[test]
    fn test_bytes_cursor_read_i8() {
        let data = Bytes::from_static(&[0xFF]);
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_i8(), -1);
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn test_bytes_cursor_read_i16() {
        let data = Bytes::from_static(&[0x00, 0x2A]); // 42
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_i16(), 42);
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn test_bytes_cursor_read_i16_negative() {
        let data = Bytes::from_static(&[0xFF, 0xFE]); // -2
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_i16(), -2);
    }

    #[test]
    fn test_bytes_cursor_read_i32() {
        let data = Bytes::from_static(&[0x00, 0x00, 0x01, 0x00]); // 256
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_i32(), 256);
    }

    #[test]
    fn test_bytes_cursor_read_i32_negative() {
        let data = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]); // -1
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_i32(), -1);
    }

    #[test]
    fn test_bytes_cursor_read_i64() {
        let val: i64 = 0x0102030405060708;
        let data = Bytes::from(val.to_be_bytes().to_vec());
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_i64(), val);
    }

    #[test]
    fn test_bytes_cursor_read_u32() {
        let data = Bytes::from_static(&[0x00, 0x00, 0x00, 0xFF]); // 255
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_u32(), 255);
    }

    #[test]
    fn test_bytes_cursor_as_slice() {
        let data = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.as_slice(), &[1, 2, 3, 4, 5]);
        cur.advance(2);
        assert_eq!(cur.as_slice(), &[3, 4, 5]);
    }

    #[test]
    fn test_bytes_cursor_advance() {
        let data = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let mut cur = BytesCursor::new(data);
        cur.advance(3);
        assert_eq!(cur.remaining(), 2);
        assert_eq!(cur.read_u8(), 4);
    }

    #[test]
    fn test_bytes_cursor_slice() {
        let data = Bytes::from_static(&[10, 20, 30, 40, 50]);
        let mut cur = BytesCursor::new(data);
        cur.advance(1); // skip first byte
        let s = cur.slice(3);
        assert_eq!(&s[..], &[20, 30, 40]);
    }

    #[test]
    fn test_bytes_cursor_sequential_reads() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&42i16.to_be_bytes());
        buf.extend_from_slice(&1000i32.to_be_bytes());
        buf.extend_from_slice(&(-1i64).to_be_bytes());
        buf.push(0xAB);
        let data = Bytes::from(buf);
        let mut cur = BytesCursor::new(data);
        assert_eq!(cur.read_i16(), 42);
        assert_eq!(cur.read_i32(), 1000);
        assert_eq!(cur.read_i64(), -1);
        assert_eq!(cur.read_u8(), 0xAB);
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn test_error_code_all_values() {
        // Verify a selection of error codes have correct i16 values
        assert_eq!(ErrorCode::None.as_i16(), 0);
        assert_eq!(ErrorCode::OffsetOutOfRange.as_i16(), 1);
        assert_eq!(ErrorCode::CorruptMessage.as_i16(), 2);
        assert_eq!(ErrorCode::UnknownTopicOrPartition.as_i16(), 3);
        assert_eq!(ErrorCode::InvalidFetchSize.as_i16(), 4);
        assert_eq!(ErrorCode::LeaderNotAvailable.as_i16(), 5);
        assert_eq!(ErrorCode::NotLeaderOrFollower.as_i16(), 6);
        assert_eq!(ErrorCode::RequestTimedOut.as_i16(), 7);
        assert_eq!(ErrorCode::MessageTooLarge.as_i16(), 10);
        assert_eq!(ErrorCode::GroupLoadInProgress.as_i16(), 14);
        assert_eq!(ErrorCode::GroupCoordinatorNotAvailable.as_i16(), 15);
        assert_eq!(ErrorCode::InvalidTopicException.as_i16(), 17);
        assert_eq!(ErrorCode::IllegalGeneration.as_i16(), 22);
        assert_eq!(ErrorCode::InvalidGroupId.as_i16(), 24);
        assert_eq!(ErrorCode::UnknownMemberId.as_i16(), 25);
        assert_eq!(ErrorCode::RebalanceInProgress.as_i16(), 27);
        assert_eq!(ErrorCode::UnsupportedVersion.as_i16(), 35);
        assert_eq!(ErrorCode::TopicAlreadyExists.as_i16(), 36);
        assert_eq!(ErrorCode::InvalidPartitions.as_i16(), 37);
        assert_eq!(ErrorCode::InvalidTxnState.as_i16(), 48);
        assert_eq!(ErrorCode::InvalidProducerEpoch.as_i16(), 47);
        assert_eq!(ErrorCode::SaslAuthenticationFailed.as_i16(), 58);
        assert_eq!(ErrorCode::GroupNotEmpty.as_i16(), 68);
        assert_eq!(ErrorCode::GroupIdNotFound.as_i16(), 69);
        assert_eq!(ErrorCode::MemberIdRequired.as_i16(), 79);
        assert_eq!(ErrorCode::ProducerFenced.as_i16(), 90);
    }

    #[test]
    fn test_api_key_from_i16_all_valid() {
        let valid_keys = [
            (0, ApiKey::Produce),
            (1, ApiKey::Fetch),
            (2, ApiKey::ListOffsets),
            (3, ApiKey::Metadata),
            (8, ApiKey::OffsetCommit),
            (9, ApiKey::OffsetFetch),
            (10, ApiKey::FindCoordinator),
            (11, ApiKey::JoinGroup),
            (12, ApiKey::Heartbeat),
            (13, ApiKey::LeaveGroup),
            (14, ApiKey::SyncGroup),
            (15, ApiKey::DescribeGroups),
            (16, ApiKey::ListGroups),
            (17, ApiKey::SaslHandshake),
            (18, ApiKey::ApiVersions),
            (19, ApiKey::CreateTopics),
            (20, ApiKey::DeleteTopics),
            (21, ApiKey::DeleteRecords),
            (22, ApiKey::InitProducerId),
            (23, ApiKey::OffsetForLeaderEpoch),
            (24, ApiKey::AddPartitionsToTxn),
            (25, ApiKey::AddOffsetsToTxn),
            (26, ApiKey::EndTxn),
            (27, ApiKey::WriteTxnMarkers),
            (28, ApiKey::TxnOffsetCommit),
            (29, ApiKey::DescribeAcls),
            (30, ApiKey::CreateAcls),
            (31, ApiKey::DeleteAcls),
            (32, ApiKey::DescribeConfigs),
            (33, ApiKey::AlterConfigs),
            (34, ApiKey::AlterReplicaLogDirs),
            (35, ApiKey::DescribeLogDirs),
            (36, ApiKey::SaslAuthenticate),
            (37, ApiKey::CreatePartitions),
            (38, ApiKey::CreateDelegationToken),
            (39, ApiKey::RenewDelegationToken),
            (40, ApiKey::ExpireDelegationToken),
            (41, ApiKey::DescribeDelegationToken),
            (42, ApiKey::DeleteGroups),
            (43, ApiKey::ElectLeaders),
            (44, ApiKey::IncrementalAlterConfigs),
            (45, ApiKey::AlterPartitionReassignments),
            (46, ApiKey::ListPartitionReassignments),
            (47, ApiKey::OffsetDelete),
            (48, ApiKey::DescribeClientQuotas),
            (49, ApiKey::AlterClientQuotas),
            (50, ApiKey::DescribeUserScramCredentials),
            (51, ApiKey::AlterUserScramCredentials),
            (55, ApiKey::DescribeQuorum),
            (57, ApiKey::UpdateFeatures),
            (60, ApiKey::DescribeCluster),
            (61, ApiKey::DescribeProducers),
            (64, ApiKey::UnregisterBroker),
            (65, ApiKey::DescribeTransactions),
            (66, ApiKey::ListTransactions),
            (68, ApiKey::ConsumerGroupHeartbeat),
            (69, ApiKey::ConsumerGroupDescribe),
            (71, ApiKey::GetTelemetrySubscriptions),
            (72, ApiKey::PushTelemetry),
            (74, ApiKey::ListConfigResources),
            (75, ApiKey::DescribeTopicPartitions),
            (76, ApiKey::ShareGroupHeartbeat),
            (77, ApiKey::ShareGroupDescribe),
            (78, ApiKey::ShareFetch),
            (79, ApiKey::ShareAcknowledge),
            (80, ApiKey::AddRaftVoter),
            (81, ApiKey::RemoveRaftVoter),
            (83, ApiKey::InitializeShareGroupState),
            (84, ApiKey::ReadShareGroupState),
            (85, ApiKey::WriteShareGroupState),
            (86, ApiKey::DeleteShareGroupState),
            (87, ApiKey::ReadShareGroupStateSummary),
            (88, ApiKey::StreamsGroupHeartbeat),
            (89, ApiKey::StreamsGroupDescribe),
            (90, ApiKey::DescribeShareGroupOffsets),
            (91, ApiKey::AlterShareGroupOffsets),
            (92, ApiKey::DeleteShareGroupOffsets),
        ];
        assert_eq!(valid_keys.len(), 77, "expected 77 total API keys");
        for (i, expected) in &valid_keys {
            assert_eq!(ApiKey::from_i16(*i), Some(*expected), "ApiKey {i}");
        }
    }

    #[test]
    fn test_api_key_from_i16_invalid() {
        assert!(ApiKey::from_i16(-1).is_none());
        assert!(ApiKey::from_i16(4).is_none()); // gap
        assert!(ApiKey::from_i16(100).is_none());
        assert!(ApiKey::from_i16(i16::MAX).is_none());
        assert!(ApiKey::from_i16(i16::MIN).is_none());
    }

    #[test]
    fn test_api_key_version_range_all() {
        // Every valid ApiKey should return a valid range where min <= max
        let keys = [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
            ApiKey::Metadata,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::SyncGroup,
            ApiKey::DescribeGroups,
            ApiKey::ListGroups,
            ApiKey::SaslHandshake,
            ApiKey::ApiVersions,
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
            ApiKey::DeleteRecords,
            ApiKey::InitProducerId,
            ApiKey::OffsetForLeaderEpoch,
            ApiKey::AddPartitionsToTxn,
            ApiKey::AddOffsetsToTxn,
            ApiKey::EndTxn,
            ApiKey::WriteTxnMarkers,
            ApiKey::TxnOffsetCommit,
            ApiKey::DescribeConfigs,
            ApiKey::AlterConfigs,
            ApiKey::AlterReplicaLogDirs,
            ApiKey::SaslAuthenticate,
            ApiKey::CreatePartitions,
            ApiKey::CreateDelegationToken,
            ApiKey::RenewDelegationToken,
            ApiKey::ExpireDelegationToken,
            ApiKey::DescribeDelegationToken,
            ApiKey::DeleteGroups,
            ApiKey::ElectLeaders,
            ApiKey::DescribeAcls,
            ApiKey::CreateAcls,
            ApiKey::DeleteAcls,
            ApiKey::DescribeLogDirs,
            ApiKey::IncrementalAlterConfigs,
            ApiKey::AlterPartitionReassignments,
            ApiKey::ListPartitionReassignments,
            ApiKey::OffsetDelete,
            ApiKey::DescribeClientQuotas,
            ApiKey::AlterClientQuotas,
            ApiKey::DescribeUserScramCredentials,
            ApiKey::AlterUserScramCredentials,
            ApiKey::DescribeQuorum,
            ApiKey::UpdateFeatures,
            ApiKey::DescribeCluster,
            ApiKey::DescribeProducers,
            ApiKey::UnregisterBroker,
            ApiKey::DescribeTransactions,
            ApiKey::ListTransactions,
            ApiKey::ConsumerGroupHeartbeat,
            ApiKey::ConsumerGroupDescribe,
            ApiKey::GetTelemetrySubscriptions,
            ApiKey::PushTelemetry,
            ApiKey::ListConfigResources,
            ApiKey::DescribeTopicPartitions,
            ApiKey::ShareGroupHeartbeat,
            ApiKey::ShareGroupDescribe,
            ApiKey::ShareFetch,
            ApiKey::ShareAcknowledge,
            ApiKey::AddRaftVoter,
            ApiKey::RemoveRaftVoter,
            ApiKey::InitializeShareGroupState,
            ApiKey::ReadShareGroupState,
            ApiKey::WriteShareGroupState,
            ApiKey::DeleteShareGroupState,
            ApiKey::ReadShareGroupStateSummary,
            ApiKey::StreamsGroupHeartbeat,
            ApiKey::StreamsGroupDescribe,
            ApiKey::DescribeShareGroupOffsets,
            ApiKey::AlterShareGroupOffsets,
            ApiKey::DeleteShareGroupOffsets,
        ];
        assert_eq!(keys.len(), 77, "expected 77 total API keys");
        for key in &keys {
            let (min, max) = key.version_range();
            assert!(min <= max, "ApiKey {:?}: min={min} > max={max}", key);
            assert!(min >= 0, "ApiKey {:?}: negative min version", key);
        }
    }

    #[test]
    fn test_wire_string_default() {
        let ws = WireString::default();
        assert!(ws.is_empty());
        assert_eq!(ws.len(), 0);
    }

    #[test]
    fn test_wire_string_from_unchecked() {
        let b = Bytes::from_static(b"safe");
        let ws = WireString::from_utf8_unchecked(b);
        assert_eq!(ws.as_str(), "safe");
    }
}
