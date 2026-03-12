Protocol
Kafka protocol guide This document covers the wire protocol implemented in Kafka.

280 min. readView original
Kafka protocol guide
This document covers the wire protocol implemented in Kafka. It is meant to give a readable guide to the protocol that covers the available requests, their binary format, and the proper way to make use of them to implement a client. This document assumes you understand the basic design and terminology described here

Preliminaries
Network
Kafka uses a binary protocol over TCP. The protocol defines all APIs as request response message pairs. All messages are size delimited and are made up of the following primitive types.

The client initiates a socket connection and then writes a sequence of request messages and reads back the corresponding response message. No handshake is required on connection or disconnection. TCP is happier if you maintain persistent connections used for many requests to amortize the cost of the TCP handshake, but beyond this penalty connecting is pretty cheap.

The client will likely need to maintain a connection to multiple brokers, as data is partitioned and the clients will need to talk to the server that has their data. However it should not generally be necessary to maintain multiple connections to a single broker from a single client instance (i.e. connection pooling).

The server guarantees that on a single TCP connection, requests will be processed in the order they are sent and responses will return in that order as well. The broker’s request processing allows only a single in-flight request per connection in order to guarantee this ordering. Note that clients can (and ideally should) use non-blocking IO to implement request pipelining and achieve higher throughput. i.e., clients can send requests even while awaiting responses for preceding requests since the outstanding requests will be buffered in the underlying OS socket buffer. All requests are initiated by the client, and result in a corresponding response message from the server except where noted.

The server has a configurable maximum limit on request size and any request that exceeds this limit will result in the socket being disconnected.

Partitioning and bootstrapping
Kafka is a partitioned system so not all servers have the complete data set. Instead recall that topics are split into a pre-defined number of partitions, P, and each partition is replicated with some replication factor, N. Topic partitions themselves are just ordered “commit logs” numbered 0, 1, …, P-1.

All systems of this nature have the question of how a particular piece of data is assigned to a particular partition. Kafka clients directly control this assignment, the brokers themselves enforce no particular semantics of which messages should be published to a particular partition. Rather, to publish messages the client directly addresses messages to a particular partition, and when fetching messages, fetches from a particular partition. If two clients want to use the same partitioning scheme they must use the same method to compute the mapping of key to partition.

These requests to publish or fetch data must be sent to the broker that is currently acting as the leader for a given partition. This condition is enforced by the broker, so a request for a particular partition to the wrong broker will result in an the NotLeaderForPartition error code (described below).

How can the client find out which topics exist, what partitions they have, and which brokers currently host those partitions so that it can direct its requests to the right hosts? This information is dynamic, so you can’t just configure each client with some static mapping file. Instead all Kafka brokers can answer a metadata request that describes the current state of the cluster: what topics there are, which partitions those topics have, which broker is the leader for those partitions, and the host and port information for these brokers.

In other words, the client needs to somehow find one broker and that broker will tell the client about all the other brokers that exist and what partitions they host. This first broker may itself go down so the best practice for a client implementation is to take a list of two or three URLs to bootstrap from. The user can then choose to use a load balancer or just statically configure two or three of their Kafka hosts in the clients.

The client does not need to keep polling to see if the cluster has changed; it can fetch metadata once when it is instantiated cache that metadata until it receives an error indicating that the metadata is out of date. This error can come in two forms: (1) a socket error indicating the client cannot communicate with a particular broker, (2) an error code in the response to a request indicating that this broker no longer hosts the partition for which data was requested.

Cycle through a list of “bootstrap” Kafka URLs until we find one we can connect to. Fetch cluster metadata.
Process fetch or produce requests, directing them to the appropriate broker based on the topic/partitions they send to or fetch from.
If we get an appropriate error, refresh the metadata and try again.
Partitioning Strategies
As mentioned above the assignment of messages to partitions is something the producing client controls. That said, how should this functionality be exposed to the end-user?

Partitioning really serves two purposes in Kafka:

It balances data and request load over brokers
It serves as a way to divvy up processing among consumer processes while allowing local state and preserving order within the partition. We call this semantic partitioning.
For a given use case you may care about only one of these or both.

To accomplish simple load balancing a simple approach would be for the client to just round robin requests over all brokers. Another alternative, in an environment where there are many more producers than brokers, would be to have each client chose a single partition at random and publish to that. This later strategy will result in far fewer TCP connections.

Semantic partitioning means using some key in the message to assign messages to partitions. For example if you were processing a click message stream you might want to partition the stream by the user id so that all data for a particular user would go to a single consumer. To accomplish this the client can take a key associated with the message and use some hash of this key to choose the partition to which to deliver the message.

Batching
Our APIs encourage batching small things together for efficiency. We have found this is a very significant performance win. Both our API to send messages and our API to fetch messages always work with a sequence of messages not a single message to encourage this. A clever client can make use of this and support an “asynchronous” mode in which it batches together messages sent individually and sends them in larger clumps. We go even further with this and allow the batching across multiple topics and partitions, so a produce request may contain data to append to many partitions and a fetch request may pull data from many partitions all at once.

The client implementer can choose to ignore this and send everything one at a time if they like.

Compatibility
Kafka has a “bidirectional” client compatibility policy. In other words, new clients can talk to old servers, and old clients can talk to new servers. This allows users to upgrade either clients or servers without experiencing any downtime.

Since the Kafka protocol has changed over time, clients and servers need to agree on the schema of the message that they are sending over the wire. This is done through API versioning.

Before each request is sent, the client sends the API key and the API version. These two 16-bit numbers, when taken together, uniquely identify the schema of the message to follow.

The intention is that clients will support a range of API versions. When communicating with a particular broker, a given client should use the highest API version supported by both and indicate this version in their requests.

The server will reject requests with a version it does not support, and will always respond to the client with exactly the protocol format it expects based on the version it included in its request. The intended upgrade path is that new features would first be rolled out on the server (with the older clients not making use of them) and then as newer clients are deployed these new features would gradually be taken advantage of. Note there is an exceptional case while retrieving supported API versions where the server can respond with a different version.

Note that KIP-482 tagged fields can be added to a request without incrementing the version number. This offers an additional way of evolving the message schema without breaking compatibility. Tagged fields do not take up any space when the field is not set. Therefore, if a field is rarely used, it is more efficient to make it a tagged field than to put it in the mandatory schema. However, tagged fields are ignored by recipients that don’t know about them, which could pose a challenge if this is not the behavior that the sender wants. In such cases, a version bump may be more appropriate.

Retrieving Supported API versions
In order to work against multiple broker versions, clients need to know what versions of various APIs a broker supports. The broker exposes this information since 0.10.0.0 as described in KIP-35. Clients should use the supported API versions information to choose the highest API version supported by both client and broker. If no such version exists, an error should be reported to the user.

The following sequence may be used by a client to obtain supported API versions from a broker.

Client sends ApiVersionsRequest to a broker after connection has been established with the broker. If SSL is enabled, this happens after SSL connection has been established.
On receiving ApiVersionsRequest, a broker returns its full list of supported ApiKeys and versions regardless of current authentication state (e.g., before SASL authentication on an SASL listener, do note that no Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished). If this is considered to leak information about the broker version a workaround is to use SSL with client authentication which is performed at an earlier stage of the connection where the ApiVersionRequest is not available. Also, note that broker versions older than 0.10.0.0 do not support this API and will either ignore the request or close connection in response to the request. Also note that if the client ApiVersionsRequest version is unsupported by the broker (client is ahead), and the broker version is 2.4.0 or greater, then the broker will respond with a version 0 ApiVersionsResponse with the error code set to UNSUPPORTED_VERSION and the api_versions field populated with the supported version of the ApiVersionsRequest. It is then up to the client to retry, making another ApiVersionsRequest using the highest version supported by the client and broker. See KIP-511: Collect and Expose Client’s Name and Version in the Brokers
If multiple versions of an API are supported by broker and client, clients are recommended to use the latest version supported by the broker and itself.
Deprecation of a protocol version is done by marking an API version as deprecated in the protocol documentation.
Supported API versions obtained from a broker are only valid for the connection on which that information is obtained. In the event of disconnection, the client should obtain the information from the broker again, as the broker might have been upgraded/downgraded in the mean time.
SASL Authentication Sequence
The following sequence is used for SASL authentication:

Kafka ApiVersionsRequest may be sent by the client to obtain the version ranges of requests supported by the broker. This is optional.
Kafka SaslHandshakeRequest containing the SASL mechanism for authentication is sent by the client. If the requested mechanism is not enabled in the server, the server responds with the list of supported mechanisms and closes the client connection. If the mechanism is enabled in the server, the server sends a successful response and continues with SASL authentication.
The actual SASL authentication is now performed. If SaslHandshakeRequest version is v0, a series of SASL client and server tokens corresponding to the mechanism are sent as opaque packets without wrapping the messages with Kafka protocol headers. If SaslHandshakeRequest version is v1, the SaslAuthenticate request/response are used, where the actual SASL tokens are wrapped in the Kafka protocol. The error code in the final message from the broker will indicate if authentication succeeded or failed.
If authentication succeeds, subsequent packets are handled as Kafka API requests. Otherwise, the client connection is closed.
For interoperability with 0.9.0.x clients, the first packet received by the server is handled as a SASL/GSSAPI client token if it is not a valid Kafka request. SASL/GSSAPI authentication is performed starting with this packet, skipping the first two steps above.

Protocol Primitive Types
The protocol is built out of the following primitive types.

Type	
Description
BOOLEAN	
Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true respectively. When reading a boolean value, any non-zero value is considered true.
INT8	
Represents an integer between -27 and 27-1 inclusive.
INT16	
Represents an integer between -215 and 215-1 inclusive. The values are encoded using two bytes in network byte order (big-endian).
INT32	
Represents an integer between -231 and 231-1 inclusive. The values are encoded using four bytes in network byte order (big-endian).
INT64	
Represents an integer between -263 and 263-1 inclusive. The values are encoded using eight bytes in network byte order (big-endian).
UINT16	
Represents an integer between 0 and 65535 inclusive. The values are encoded using two bytes in network byte order (big-endian).
UINT32	
Represents an integer between 0 and 232-1 inclusive. The values are encoded using four bytes in network byte order (big-endian).
VARINT	
Represents an integer between -231 and 231-1 inclusive. Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
VARLONG	
Represents an integer between -263 and 263-1 inclusive. Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
UUID	
Represents a type 4 immutable universally unique identifier (Uuid). The values are encoded using sixteen bytes in network byte order (big-endian).
FLOAT64	
Represents a double-precision 64-bit format IEEE 754 value. The values are encoded using eight bytes in network byte order (big-endian).
STRING	
Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. Length must not be negative.
COMPACT_STRING	
Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT . Then N bytes follow which are the UTF-8 encoding of the character sequence.
NULLABLE_STRING	
Represents a sequence of characters or null. For non-null strings, first the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is encoded with length of -1 and there are no following bytes.
COMPACT_NULLABLE_STRING	
Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT . Then N bytes follow which are the UTF-8 encoding of the character sequence. A null string is represented with a length of 0.
BYTES	
Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.
COMPACT_BYTES	
Represents a raw sequence of bytes. First the length N+1 is given as an UNSIGNED_VARINT.Then N bytes follow.
NULLABLE_BYTES	
Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following bytes.
COMPACT_NULLABLE_BYTES	
Represents a raw sequence of bytes. First the length N+1 is given as an UNSIGNED_VARINT.Then N bytes follow. A null object is represented with a length of 0.
RECORDS	
Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see Message Sets.
COMPACT_RECORDS	
Represents a sequence of Kafka records as COMPACT_NULLABLE_BYTES. For a detailed description of records see Message Sets.
ARRAY	
Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure. First, the length N is given as an INT32. Then N instances of type T follow. A null array is represented with a length of -1. In protocol documentation an array of T instances is referred to as [T].
COMPACT_ARRAY	
Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure. First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow. A null array is represented with a length of 0. In protocol documentation an array of T instances is referred to as [T].
Notes on reading the request format grammars
The BNFs below give an exact context free grammar for the request and response binary format. The BNF is intentionally not compact in order to give human-readable name. As always in a BNF a sequence of productions indicates concatenation. When there are multiple possible productions these are separated with ‘|’ and may be enclosed in parenthesis for grouping. The top-level definition is always given first and subsequent sub-parts are indented.

Common Request and Response Structure
All requests and responses originate from the following grammar which will be incrementally describe through the rest of this document:

RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32  
Field

Description

message_size

The message_size field gives the size of the subsequent request or response message in bytes. The client can read requests by first reading this 4 byte size as an integer N, and then reading and parsing the subsequent N bytes of the request.

Request and Response Headers
Different request and response versions require different versions of the corresponding headers. These header versions are specified below together with API message descriptions.

Record Batch
A description of the record batch format can be found here.

Constants
Error Codes
We use numeric codes to indicate what problem occurred on the server. These can be translated by the client into exceptions or whatever the appropriate error handling mechanism in the client language. Here is a table of the error codes currently in use:

Error	
Code	
Retriable	
Description
UNKNOWN_SERVER_ERROR	
-1	
False	
The server experienced an unexpected error when processing the request.
NONE	
0	
False	
OFFSET_OUT_OF_RANGE	
1	
False	
The requested offset is not within the range of offsets maintained by the server.
CORRUPT_MESSAGE	
2	
True	
This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
UNKNOWN_TOPIC_OR_PARTITION	
3	
True	
This server does not host this topic-partition.
INVALID_FETCH_SIZE	
4	
False	
The requested fetch size is invalid.
LEADER_NOT_AVAILABLE	
5	
True	
There is no leader for this topic-partition as we are in the middle of a leadership election.
NOT_LEADER_OR_FOLLOWER	
6	
True	
For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.
REQUEST_TIMED_OUT	
7	
True	
The request timed out.
BROKER_NOT_AVAILABLE	
8	
False	
The broker is not available.
REPLICA_NOT_AVAILABLE	
9	
True	
The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.
MESSAGE_TOO_LARGE	
10	
False	
The request included a message larger than the max message size the server will accept.
STALE_CONTROLLER_EPOCH	
11	
False	
The controller moved to another broker.
OFFSET_METADATA_TOO_LARGE	
12	
False	
The metadata field of the offset request was too large.
NETWORK_EXCEPTION	
13	
True	
The server disconnected before a response was received.
COORDINATOR_LOAD_IN_PROGRESS	
14	
True	
The coordinator is loading and hence can't process requests.
COORDINATOR_NOT_AVAILABLE	
15	
True	
The coordinator is not available.
NOT_COORDINATOR	
16	
True	
This is not the correct coordinator.
INVALID_TOPIC_EXCEPTION	
17	
False	
The request attempted to perform an operation on an invalid topic.
RECORD_LIST_TOO_LARGE	
18	
False	
The request included message batch larger than the configured segment size on the server.
NOT_ENOUGH_REPLICAS	
19	
True	
Messages are rejected since there are fewer in-sync replicas than required.
NOT_ENOUGH_REPLICAS_AFTER_APPEND	
20	
True	
Messages are written to the log, but to fewer in-sync replicas than required.
INVALID_REQUIRED_ACKS	
21	
False	
Produce request specified an invalid value for required acks.
ILLEGAL_GENERATION	
22	
False	
Specified group generation id is not valid.
INCONSISTENT_GROUP_PROTOCOL	
23	
False	
The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.
INVALID_GROUP_ID	
24	
False	
The group id is invalid.
UNKNOWN_MEMBER_ID	
25	
False	
The coordinator is not aware of this member.
INVALID_SESSION_TIMEOUT	
26	
False	
The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
REBALANCE_IN_PROGRESS	
27	
False	
The group is rebalancing, so a rejoin is needed.
INVALID_COMMIT_OFFSET_SIZE	
28	
False	
The committing offset data size is not valid.
TOPIC_AUTHORIZATION_FAILED	
29	
False	
Topic authorization failed.
GROUP_AUTHORIZATION_FAILED	
30	
False	
Group authorization failed.
CLUSTER_AUTHORIZATION_FAILED	
31	
False	
Cluster authorization failed.
INVALID_TIMESTAMP	
32	
False	
The timestamp of the message is out of acceptable range.
UNSUPPORTED_SASL_MECHANISM	
33	
False	
The broker does not support the requested SASL mechanism.
ILLEGAL_SASL_STATE	
34	
False	
Request is not valid given the current SASL state.
UNSUPPORTED_VERSION	
35	
False	
The version of API is not supported.
TOPIC_ALREADY_EXISTS	
36	
False	
Topic with this name already exists.
INVALID_PARTITIONS	
37	
False	
Number of partitions is below 1.
INVALID_REPLICATION_FACTOR	
38	
False	
Replication factor is below 1 or larger than the number of available brokers.
INVALID_REPLICA_ASSIGNMENT	
39	
False	
Replica assignment is invalid.
INVALID_CONFIG	
40	
False	
Configuration is invalid.
NOT_CONTROLLER	
41	
True	
This is not the correct controller for this cluster.
INVALID_REQUEST	
42	
False	
This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
UNSUPPORTED_FOR_MESSAGE_FORMAT	
43	
False	
The message format version on the broker does not support the request.
POLICY_VIOLATION	
44	
False	
Request parameters do not satisfy the configured policy.
OUT_OF_ORDER_SEQUENCE_NUMBER	
45	
False	
The broker received an out of order sequence number.
DUPLICATE_SEQUENCE_NUMBER	
46	
False	
The broker received a duplicate sequence number.
INVALID_PRODUCER_EPOCH	
47	
False	
Producer attempted to produce with an old epoch.
INVALID_TXN_STATE	
48	
False	
The producer attempted a transactional operation in an invalid state.
INVALID_PRODUCER_ID_MAPPING	
49	
False	
The producer attempted to use a producer id which is not currently assigned to its transactional id.
INVALID_TRANSACTION_TIMEOUT	
50	
False	
The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
CONCURRENT_TRANSACTIONS	
51	
True	
The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
TRANSACTION_COORDINATOR_FENCED	
52	
False	
Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
TRANSACTIONAL_ID_AUTHORIZATION_FAILED	
53	
False	
Transactional Id authorization failed.
SECURITY_DISABLED	
54	
False	
Security features are disabled.
OPERATION_NOT_ATTEMPTED	
55	
False	
The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.
KAFKA_STORAGE_ERROR	
56	
True	
Disk error when trying to access log file on the disk.
LOG_DIR_NOT_FOUND	
57	
False	
The user-specified log directory is not found in the broker config.
SASL_AUTHENTICATION_FAILED	
58	
False	
SASL Authentication failed.
UNKNOWN_PRODUCER_ID	
59	
False	
This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.
REASSIGNMENT_IN_PROGRESS	
60	
False	
A partition reassignment is in progress.
DELEGATION_TOKEN_AUTH_DISABLED	
61	
False	
Delegation Token feature is not enabled.
DELEGATION_TOKEN_NOT_FOUND	
62	
False	
Delegation Token is not found on server.
DELEGATION_TOKEN_OWNER_MISMATCH	
63	
False	
Specified Principal is not valid Owner/Renewer.
DELEGATION_TOKEN_REQUEST_NOT_ALLOWED	
64	
False	
Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
DELEGATION_TOKEN_AUTHORIZATION_FAILED	
65	
False	
Delegation Token authorization failed.
DELEGATION_TOKEN_EXPIRED	
66	
False	
Delegation Token is expired.
INVALID_PRINCIPAL_TYPE	
67	
False	
Supplied principalType is not supported.
NON_EMPTY_GROUP	
68	
False	
The group is not empty.
GROUP_ID_NOT_FOUND	
69	
False	
The group id does not exist.
FETCH_SESSION_ID_NOT_FOUND	
70	
True	
The fetch session ID was not found.
INVALID_FETCH_SESSION_EPOCH	
71	
True	
The fetch session epoch is invalid.
LISTENER_NOT_FOUND	
72	
True	
There is no listener on the leader broker that matches the listener on which metadata request was processed.
TOPIC_DELETION_DISABLED	
73	
False	
Topic deletion is disabled.
FENCED_LEADER_EPOCH	
74	
True	
The leader epoch in the request is older than the epoch on the broker.
UNKNOWN_LEADER_EPOCH	
75	
True	
The leader epoch in the request is newer than the epoch on the broker.
UNSUPPORTED_COMPRESSION_TYPE	
76	
False	
The requesting client does not support the compression type of given partition.
STALE_BROKER_EPOCH	
77	
False	
Broker epoch has changed.
OFFSET_NOT_AVAILABLE	
78	
True	
The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.
MEMBER_ID_REQUIRED	
79	
False	
The group member needs to have a valid member id before actually entering a consumer group.
PREFERRED_LEADER_NOT_AVAILABLE	
80	
True	
The preferred leader was not available.
GROUP_MAX_SIZE_REACHED	
81	
False	
The group has reached its maximum size.
FENCED_INSTANCE_ID	
82	
False	
The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.
ELIGIBLE_LEADERS_NOT_AVAILABLE	
83	
True	
Eligible topic partition leaders are not available.
ELECTION_NOT_NEEDED	
84	
True	
Leader election not needed for topic partition.
NO_REASSIGNMENT_IN_PROGRESS	
85	
False	
No partition reassignment is in progress.
GROUP_SUBSCRIBED_TO_TOPIC	
86	
False	
Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.
INVALID_RECORD	
87	
False	
This record has failed the validation on broker and hence will be rejected.
UNSTABLE_OFFSET_COMMIT	
88	
True	
There are unstable offsets that need to be cleared.
THROTTLING_QUOTA_EXCEEDED	
89	
True	
The throttling quota has been exceeded.
PRODUCER_FENCED	
90	
False	
There is a newer producer with the same transactionalId which fences the current one.
RESOURCE_NOT_FOUND	
91	
False	
A request illegally referred to a resource that does not exist.
DUPLICATE_RESOURCE	
92	
False	
A request illegally referred to the same resource twice.
UNACCEPTABLE_CREDENTIAL	
93	
False	
Requested credential would not meet criteria for acceptability.
INCONSISTENT_VOTER_SET	
94	
False	
Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.
INVALID_UPDATE_VERSION	
95	
False	
The given update version was invalid.
FEATURE_UPDATE_FAILED	
96	
False	
Unable to update finalized features due to an unexpected server error.
PRINCIPAL_DESERIALIZATION_FAILURE	
97	
False	
Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.
SNAPSHOT_NOT_FOUND	
98	
False	
Requested snapshot was not found.
POSITION_OUT_OF_RANGE	
99	
False	
Requested position is not greater than or equal to zero, and less than the size of the snapshot.
UNKNOWN_TOPIC_ID	
100	
True	
This server does not host this topic ID.
DUPLICATE_BROKER_REGISTRATION	
101	
False	
This broker ID is already in use.
BROKER_ID_NOT_REGISTERED	
102	
False	
The given broker ID was not registered.
INCONSISTENT_TOPIC_ID	
103	
True	
The log's topic ID did not match the topic ID in the request.
INCONSISTENT_CLUSTER_ID	
104	
False	
The clusterId in the request does not match that found on the server.
TRANSACTIONAL_ID_NOT_FOUND	
105	
False	
The transactionalId could not be found.
FETCH_SESSION_TOPIC_ID_ERROR	
106	
True	
The fetch session encountered inconsistent topic ID usage.
INELIGIBLE_REPLICA	
107	
False	
The new ISR contains at least one ineligible replica.
NEW_LEADER_ELECTED	
108	
False	
The AlterPartition request successfully updated the partition state but the leader has changed.
OFFSET_MOVED_TO_TIERED_STORAGE	
109	
False	
The requested offset is moved to tiered storage.
FENCED_MEMBER_EPOCH	
110	
False	
The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.
UNRELEASED_INSTANCE_ID	
111	
False	
The instance ID is still used by another member in the consumer group. That member must leave first.
UNSUPPORTED_ASSIGNOR	
112	
False	
The assignor or its version range is not supported by the consumer group.
STALE_MEMBER_EPOCH	
113	
False	
The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.
MISMATCHED_ENDPOINT_TYPE	
114	
False	
The request was sent to an endpoint of the wrong type.
UNSUPPORTED_ENDPOINT_TYPE	
115	
False	
This endpoint type is not supported yet.
UNKNOWN_CONTROLLER_ID	
116	
False	
This controller ID is not known.
UNKNOWN_SUBSCRIPTION_ID	
117	
False	
Client sent a push telemetry request with an invalid or outdated subscription ID.
TELEMETRY_TOO_LARGE	
118	
False	
Client sent a push telemetry request larger than the maximum size the broker will accept.
INVALID_REGISTRATION	
119	
False	
The controller has considered the broker registration to be invalid.
TRANSACTION_ABORTABLE	
120	
False	
The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.
INVALID_RECORD_STATE	
121	
False	
The record state is invalid. The acknowledgement of delivery could not be completed.
SHARE_SESSION_NOT_FOUND	
122	
True	
The share session was not found.
INVALID_SHARE_SESSION_EPOCH	
123	
True	
The share session epoch is invalid.
FENCED_STATE_EPOCH	
124	
False	
The share coordinator rejected the request because the share-group state epoch did not match.
INVALID_VOTER_KEY	
125	
False	
The voter key doesn't match the receiving replica's key.
DUPLICATE_VOTER	
126	
False	
The voter is already part of the set of voters.
VOTER_NOT_FOUND	
127	
False	
The voter is not part of the set of voters.
INVALID_REGULAR_EXPRESSION	
128	
False	
The regular expression is not valid.
REBOOTSTRAP_REQUIRED	
129	
False	
Client metadata is stale. The client should rebootstrap to obtain new metadata.
STREAMS_INVALID_TOPOLOGY	
130	
False	
The supplied topology is invalid.
STREAMS_INVALID_TOPOLOGY_EPOCH	
131	
False	
The supplied topology epoch is invalid.
STREAMS_TOPOLOGY_FENCED	
132	
False	
The supplied topology epoch is outdated.
SHARE_SESSION_LIMIT_REACHED	
133	
True	
The limit of share sessions has been reached.
Api Keys
The following are the numeric codes that the stable ApiKey in the request can take for each of the below request types.

The Messages
This section gives details on each of the individual API Messages, their usage, their binary format, and the meaning of their fields.

The message consists of the header and body:

Message => RequestOrResponseHeader Body
RequestOrResponseHeader is the versioned request or response header. Body is the message-specific body.

Headers:
Request Header v1 => request_api_key request_api_version correlation_id client_id 
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING
Field	
Description
request_api_key	
The API key of this request.
request_api_version	
The API version of this request.
correlation_id	
The correlation ID of this request.
client_id	
The client ID string.
Request Header v2 => request_api_key request_api_version correlation_id client_id 
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING
Field	
Description
request_api_key	
The API key of this request.
request_api_version	
The API version of this request.
correlation_id	
The correlation ID of this request.
client_id	
The client ID string.
Response Header v0 => correlation_id 
  correlation_id => INT32
Field	
Description
correlation_id	
The correlation ID of this response.
Response Header v1 => correlation_id 
  correlation_id => INT32
Field	
Description
correlation_id	
The correlation ID of this response.
Produce API (Key: 0):
Requests:

Produce Request (Version: 3) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => STRING
    partition_data => index records 
      index => INT32
      records => RECORDS
Request header version: 1

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 4) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => STRING
    partition_data => index records 
      index => INT32
      records => RECORDS
Request header version: 1

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 5) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => STRING
    partition_data => index records 
      index => INT32
      records => RECORDS
Request header version: 1

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 6) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => STRING
    partition_data => index records 
      index => INT32
      records => RECORDS
Request header version: 1

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 7) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => STRING
    partition_data => index records 
      index => INT32
      records => RECORDS
Request header version: 1

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 8) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => STRING
    partition_data => index records 
      index => INT32
      records => RECORDS
Request header version: 1

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 9) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => COMPACT_STRING
    partition_data => index records 
      index => INT32
      records => COMPACT_RECORDS
Request header version: 2

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 10) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => COMPACT_STRING
    partition_data => index records 
      index => INT32
      records => COMPACT_RECORDS
Request header version: 2

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 11) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => COMPACT_STRING
    partition_data => index records 
      index => INT32
      records => COMPACT_RECORDS
Request header version: 2

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 12) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] 
    name => COMPACT_STRING
    partition_data => index records 
      index => INT32
      records => COMPACT_RECORDS
Request header version: 2

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
name	
The topic name.
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Produce Request (Version: 13) => transactional_id acks timeout_ms [topic_data] 
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => topic_id [partition_data] 
    topic_id => UUID
    partition_data => index records 
      index => INT32
      records => COMPACT_RECORDS
Request header version: 2

Field	
Description
transactional_id	
The transactional ID, or null if the producer is not transactional.
acks	
The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
timeout_ms	
The timeout to await a response in milliseconds.
topic_data	
Each topic to produce to.
topic_id	
The unique topic ID
partition_data	
Each partition to produce to.
index	
The partition index.
records	
The record data to be produced.
Responses:

Produce Response (Version: 3) => [responses] throttle_time_ms 
  responses => name [partition_responses] 
    name => STRING
    partition_responses => index error_code base_offset log_append_time_ms 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
Produce Response (Version: 4) => [responses] throttle_time_ms 
  responses => name [partition_responses] 
    name => STRING
    partition_responses => index error_code base_offset log_append_time_ms 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
Produce Response (Version: 5) => [responses] throttle_time_ms 
  responses => name [partition_responses] 
    name => STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
Produce Response (Version: 6) => [responses] throttle_time_ms 
  responses => name [partition_responses] 
    name => STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
Produce Response (Version: 7) => [responses] throttle_time_ms 
  responses => name [partition_responses] 
    name => STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
Produce Response (Version: 8) => [responses] throttle_time_ms 
  responses => name [partition_responses] 
    name => STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message 
        batch_index => INT32
        batch_index_error_message => NULLABLE_STRING
      error_message => NULLABLE_STRING
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
record_errors	
The batch indices of records that caused the batch to be dropped.
batch_index	
The batch index of the record that caused the batch to be dropped.
batch_index_error_message	
The error message of the record that caused the batch to be dropped.
error_message	
The global error message summarizing the common root cause of the records that caused the batch to be dropped.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
Produce Response (Version: 9) => [responses] throttle_time_ms 
  responses => name [partition_responses] 
    name => COMPACT_STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message 
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
  throttle_time_ms => INT32
Response header version: 1

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
record_errors	
The batch indices of records that caused the batch to be dropped.
batch_index	
The batch index of the record that caused the batch to be dropped.
batch_index_error_message	
The error message of the record that caused the batch to be dropped.
error_message	
The global error message summarizing the common root cause of the records that caused the batch to be dropped.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
Produce Response (Version: 10) => [responses] throttle_time_ms [node_endpoints]<tag: 0> 
  responses => name [partition_responses] 
    name => COMPACT_STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message current_leader<tag: 0> 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message 
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
      current_leader<tag: 0> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
  throttle_time_ms => INT32
  node_endpoints<tag: 0> => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
record_errors	
The batch indices of records that caused the batch to be dropped.
batch_index	
The batch index of the record that caused the batch to be dropped.
batch_index_error_message	
The error message of the record that caused the batch to be dropped.
error_message	
The global error message summarizing the common root cause of the records that caused the batch to be dropped.
current_leader<tag: 0>	
The leader broker that the producer should use for future requests.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
node_endpoints<tag: 0>	
Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
Field	Description
node_id	The ID of the associated node.
host	The node's hostname.
port	The node's port.
rack	The rack of the node, or null if it has not been assigned to a rack.
Produce Response (Version: 11) => [responses] throttle_time_ms [node_endpoints]<tag: 0> 
  responses => name [partition_responses] 
    name => COMPACT_STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message current_leader<tag: 0> 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message 
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
      current_leader<tag: 0> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
  throttle_time_ms => INT32
  node_endpoints<tag: 0> => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
record_errors	
The batch indices of records that caused the batch to be dropped.
batch_index	
The batch index of the record that caused the batch to be dropped.
batch_index_error_message	
The error message of the record that caused the batch to be dropped.
error_message	
The global error message summarizing the common root cause of the records that caused the batch to be dropped.
current_leader<tag: 0>	
The leader broker that the producer should use for future requests.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
node_endpoints<tag: 0>	
Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
Field	Description
node_id	The ID of the associated node.
host	The node's hostname.
port	The node's port.
rack	The rack of the node, or null if it has not been assigned to a rack.
Produce Response (Version: 12) => [responses] throttle_time_ms [node_endpoints]<tag: 0> 
  responses => name [partition_responses] 
    name => COMPACT_STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message current_leader<tag: 0> 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message 
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
      current_leader<tag: 0> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
  throttle_time_ms => INT32
  node_endpoints<tag: 0> => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
responses	
Each produce response.
name	
The topic name.
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
record_errors	
The batch indices of records that caused the batch to be dropped.
batch_index	
The batch index of the record that caused the batch to be dropped.
batch_index_error_message	
The error message of the record that caused the batch to be dropped.
error_message	
The global error message summarizing the common root cause of the records that caused the batch to be dropped.
current_leader<tag: 0>	
The leader broker that the producer should use for future requests.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
node_endpoints<tag: 0>	
Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
Field	Description
node_id	The ID of the associated node.
host	The node's hostname.
port	The node's port.
rack	The rack of the node, or null if it has not been assigned to a rack.
Produce Response (Version: 13) => [responses] throttle_time_ms [node_endpoints]<tag: 0> 
  responses => topic_id [partition_responses] 
    topic_id => UUID
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message current_leader<tag: 0> 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message 
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
      current_leader<tag: 0> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
  throttle_time_ms => INT32
  node_endpoints<tag: 0> => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
responses	
Each produce response.
topic_id	
The unique topic ID
partition_responses	
Each partition that we produced to within the topic.
index	
The partition index.
error_code	
The error code, or 0 if there was no error.
base_offset	
The base offset.
log_append_time_ms	
The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
log_start_offset	
The log start offset.
record_errors	
The batch indices of records that caused the batch to be dropped.
batch_index	
The batch index of the record that caused the batch to be dropped.
batch_index_error_message	
The error message of the record that caused the batch to be dropped.
error_message	
The global error message summarizing the common root cause of the records that caused the batch to be dropped.
current_leader<tag: 0>	
The leader broker that the producer should use for future requests.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
node_endpoints<tag: 0>	
Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
Field	Description
node_id	The ID of the associated node.
host	The node's hostname.
port	The node's port.
rack	The rack of the node, or null if it has not been assigned to a rack.
Fetch API (Key: 1):
Requests:

Fetch Request (Version: 4) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  topics => topic [partitions] 
    topic => STRING
    partitions => partition fetch_offset partition_max_bytes 
      partition => INT32
      fetch_offset => INT64
      partition_max_bytes => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
fetch_offset	
The message offset.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
Fetch Request (Version: 5) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  topics => topic [partitions] 
    topic => STRING
    partitions => partition fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
fetch_offset	
The message offset.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
Fetch Request (Version: 6) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  topics => topic [partitions] 
    topic => STRING
    partitions => partition fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
fetch_offset	
The message offset.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
Fetch Request (Version: 7) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic [partitions] 
    topic => STRING
    partitions => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
fetch_offset	
The message offset.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic	
The topic name.
partitions	
The partitions indexes to forget.
Fetch Request (Version: 8) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic [partitions] 
    topic => STRING
    partitions => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
fetch_offset	
The message offset.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic	
The topic name.
partitions	
The partitions indexes to forget.
Fetch Request (Version: 9) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition current_leader_epoch fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic [partitions] 
    topic => STRING
    partitions => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic	
The topic name.
partitions	
The partitions indexes to forget.
Fetch Request (Version: 10) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition current_leader_epoch fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic [partitions] 
    topic => STRING
    partitions => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic	
The topic name.
partitions	
The partitions indexes to forget.
Fetch Request (Version: 11) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition current_leader_epoch fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic [partitions] 
    topic => STRING
    partitions => INT32
  rack_id => STRING
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic	
The topic name.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
Fetch Request (Version: 12) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic [partitions] 
    topic => COMPACT_STRING
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic [partitions] 
    topic => COMPACT_STRING
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic	
The name of the topic to fetch.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
last_fetched_epoch	
The epoch of the last fetched record or -1 if there is none.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic	
The topic name.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
cluster_id<tag: 0>	
The clusterId if known. This is used to validate metadata fetches prior to broker registration.
Fetch Request (Version: 13) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
last_fetched_epoch	
The epoch of the last fetched record or -1 if there is none.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
cluster_id<tag: 0>	
The clusterId if known. This is used to validate metadata fetches prior to broker registration.
Fetch Request (Version: 14) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
last_fetched_epoch	
The epoch of the last fetched record or -1 if there is none.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
cluster_id<tag: 0>	
The clusterId if known. This is used to validate metadata fetches prior to broker registration.
Fetch Request (Version: 15) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> replica_state<tag: 1> 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
  replica_state<tag: 1> => replica_id replica_epoch 
    replica_id => INT32
    replica_epoch => INT64
Request header version: 2

Field	
Description
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
last_fetched_epoch	
The epoch of the last fetched record or -1 if there is none.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
cluster_id<tag: 0>	
The clusterId if known. This is used to validate metadata fetches prior to broker registration.
replica_state<tag: 1>	
The state of the replica in the follower.
Field	Description
replica_id	The replica ID of the follower, or -1 if this request is from a consumer.
replica_epoch	The epoch of this follower, or -1 if not available.
Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> replica_state<tag: 1> 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
  replica_state<tag: 1> => replica_id replica_epoch 
    replica_id => INT32
    replica_epoch => INT64
Request header version: 2

Field	
Description
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
last_fetched_epoch	
The epoch of the last fetched record or -1 if there is none.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
cluster_id<tag: 0>	
The clusterId if known. This is used to validate metadata fetches prior to broker registration.
replica_state<tag: 1>	
The state of the replica in the follower.
Field	Description
replica_id	The replica ID of the follower, or -1 if this request is from a consumer.
replica_epoch	The epoch of this follower, or -1 if not available.
Fetch Request (Version: 17) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> replica_state<tag: 1> 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes replica_directory_id<tag: 0> 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
      replica_directory_id<tag: 0> => UUID
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
  replica_state<tag: 1> => replica_id replica_epoch 
    replica_id => INT32
    replica_epoch => INT64
Request header version: 2

Field	
Description
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
last_fetched_epoch	
The epoch of the last fetched record or -1 if there is none.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
replica_directory_id<tag: 0>	
The directory id of the follower fetching.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
cluster_id<tag: 0>	
The clusterId if known. This is used to validate metadata fetches prior to broker registration.
replica_state<tag: 1>	
The state of the replica in the follower.
Field	Description
replica_id	The replica ID of the follower, or -1 if this request is from a consumer.
replica_epoch	The epoch of this follower, or -1 if not available.
Fetch Request (Version: 18) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> replica_state<tag: 1> 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes replica_directory_id<tag: 0> high_watermark<tag: 1> 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
      replica_directory_id<tag: 0> => UUID
      high_watermark<tag: 1> => INT64
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
  replica_state<tag: 1> => replica_id replica_epoch 
    replica_id => INT32
    replica_epoch => INT64
Request header version: 2

Field	
Description
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
session_id	
The fetch session ID.
session_epoch	
The fetch session epoch, which is used for ordering requests in a session.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition	
The partition index.
current_leader_epoch	
The current leader epoch of the partition.
fetch_offset	
The message offset.
last_fetched_epoch	
The epoch of the last fetched record or -1 if there is none.
log_start_offset	
The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
partition_max_bytes	
The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
replica_directory_id<tag: 0>	
The directory id of the follower fetching.
high_watermark<tag: 1>	
The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
forgotten_topics_data	
In an incremental fetch request, the partitions to remove.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
rack_id	
Rack ID of the consumer making this request.
cluster_id<tag: 0>	
The clusterId if known. This is used to validate metadata fetches prior to broker registration.
replica_state<tag: 1>	
The state of the replica in the follower.
Field	Description
replica_id	The replica ID of the follower, or -1 if this request is from a consumer.
replica_epoch	The epoch of this follower, or -1 if not available.
Responses:

Fetch Response (Version: 4) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
records	
The record data.
Fetch Response (Version: 5) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
records	
The record data.
Fetch Response (Version: 6) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
records	
The record data.
Fetch Response (Version: 7) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
records	
The record data.
Fetch Response (Version: 8) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
records	
The record data.
Fetch Response (Version: 9) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
records	
The record data.
Fetch Response (Version: 10) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
records	
The record data.
Fetch Response (Version: 11) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => RECORDS
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
Fetch Response (Version: 12) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => COMPACT_STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset 
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch 
        end_offset => INT64
        epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic	
The topic name.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
diverging_epoch<tag: 0>	
In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
Field	Description
epoch	The largest epoch.
end_offset	The end offset of the epoch.
current_leader<tag: 1>	
The current leader of the partition.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
snapshot_id<tag: 2>	
In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
Field	Description
end_offset	The end offset of the epoch.
epoch	The largest epoch.
Fetch Response (Version: 13) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset 
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch 
        end_offset => INT64
        epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
diverging_epoch<tag: 0>	
In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
Field	Description
epoch	The largest epoch.
end_offset	The end offset of the epoch.
current_leader<tag: 1>	
The current leader of the partition.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
snapshot_id<tag: 2>	
In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
Field	Description
end_offset	The end offset of the epoch.
epoch	The largest epoch.
Fetch Response (Version: 14) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset 
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch 
        end_offset => INT64
        epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
diverging_epoch<tag: 0>	
In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
Field	Description
epoch	The largest epoch.
end_offset	The end offset of the epoch.
current_leader<tag: 1>	
The current leader of the partition.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
snapshot_id<tag: 2>	
In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
Field	Description
end_offset	The end offset of the epoch.
epoch	The largest epoch.
Fetch Response (Version: 15) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset 
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch 
        end_offset => INT64
        epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
diverging_epoch<tag: 0>	
In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
Field	Description
epoch	The largest epoch.
end_offset	The end offset of the epoch.
current_leader<tag: 1>	
The current leader of the partition.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
snapshot_id<tag: 2>	
In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
Field	Description
end_offset	The end offset of the epoch.
epoch	The largest epoch.
Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] [node_endpoints]<tag: 0> 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset 
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch 
        end_offset => INT64
        epoch => INT32
  node_endpoints<tag: 0> => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
diverging_epoch<tag: 0>	
In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
Field	Description
epoch	The largest epoch.
end_offset	The end offset of the epoch.
current_leader<tag: 1>	
The current leader of the partition.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
snapshot_id<tag: 2>	
In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
Field	Description
end_offset	The end offset of the epoch.
epoch	The largest epoch.
node_endpoints<tag: 0>	
Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
Field	Description
node_id	The ID of the associated node.
host	The node's hostname.
port	The node's port.
rack	The rack of the node, or null if it has not been assigned to a rack.
Fetch Response (Version: 17) => throttle_time_ms error_code session_id [responses] [node_endpoints]<tag: 0> 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset 
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch 
        end_offset => INT64
        epoch => INT32
  node_endpoints<tag: 0> => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
diverging_epoch<tag: 0>	
In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
Field	Description
epoch	The largest epoch.
end_offset	The end offset of the epoch.
current_leader<tag: 1>	
The current leader of the partition.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
snapshot_id<tag: 2>	
In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
Field	Description
end_offset	The end offset of the epoch.
epoch	The largest epoch.
node_endpoints<tag: 0>	
Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
Field	Description
node_id	The ID of the associated node.
host	The node's hostname.
port	The node's port.
rack	The rack of the node, or null if it has not been assigned to a rack.
Fetch Response (Version: 18) => throttle_time_ms error_code session_id [responses] [node_endpoints]<tag: 0> 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset 
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch 
        end_offset => INT64
        epoch => INT32
  node_endpoints<tag: 0> => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
session_id	
The fetch session ID, or 0 if this is not part of a fetch session.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no fetch error.
high_watermark	
The current high water mark.
last_stable_offset	
The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
log_start_offset	
The current log start offset.
aborted_transactions	
The aborted transactions.
producer_id	
The producer id associated with the aborted transaction.
first_offset	
The first offset in the aborted transaction.
preferred_read_replica	
The preferred read replica for the consumer to use on its next fetch request.
records	
The record data.
diverging_epoch<tag: 0>	
In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
Field	Description
epoch	The largest epoch.
end_offset	The end offset of the epoch.
current_leader<tag: 1>	
The current leader of the partition.
Field	Description
leader_id	The ID of the current leader or -1 if the leader is unknown.
leader_epoch	The latest known leader epoch.
snapshot_id<tag: 2>	
In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
Field	Description
end_offset	The end offset of the epoch.
epoch	The largest epoch.
node_endpoints<tag: 0>	
Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
Field	Description
node_id	The ID of the associated node.
host	The node's hostname.
port	The node's port.
rack	The rack of the node, or null if it has not been assigned to a rack.
ListOffsets API (Key: 2):
Requests:

ListOffsets Request (Version: 1) => replica_id [topics] 
  replica_id => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index timestamp 
      partition_index => INT32
      timestamp => INT64
Request header version: 1

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
timestamp	
The current timestamp.
ListOffsets Request (Version: 2) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => STRING
    partitions => partition_index timestamp 
      partition_index => INT32
      timestamp => INT64
Request header version: 1

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
timestamp	
The current timestamp.
ListOffsets Request (Version: 3) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => STRING
    partitions => partition_index timestamp 
      partition_index => INT32
      timestamp => INT64
Request header version: 1

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
timestamp	
The current timestamp.
ListOffsets Request (Version: 4) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
Request header version: 1

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
ListOffsets Request (Version: 5) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
Request header version: 1

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
ListOffsets Request (Version: 6) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
Request header version: 2

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
ListOffsets Request (Version: 7) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
Request header version: 2

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
ListOffsets Request (Version: 8) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
Request header version: 2

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
ListOffsets Request (Version: 9) => replica_id isolation_level [topics] 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
Request header version: 2

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
ListOffsets Request (Version: 10) => replica_id isolation_level [topics] timeout_ms 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
  timeout_ms => INT32
Request header version: 2

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
timeout_ms	
The timeout to await a response in milliseconds for requests that require reading from remote storage for topics enabled with tiered storage.
ListOffsets Request (Version: 11) => replica_id isolation_level [topics] timeout_ms 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index current_leader_epoch timestamp 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
  timeout_ms => INT32
Request header version: 2

Field	
Description
replica_id	
The broker ID of the requester, or -1 if this request is being made by a normal consumer.
isolation_level	
This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
topics	
Each topic in the request.
name	
The topic name.
partitions	
Each partition in the request.
partition_index	
The partition index.
current_leader_epoch	
The current leader epoch.
timestamp	
The current timestamp.
timeout_ms	
The timeout to await a response in milliseconds for requests that require reading from remote storage for topics enabled with tiered storage.
Responses:

ListOffsets Response (Version: 1) => [topics] 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code timestamp offset 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
Response header version: 0

Field	
Description
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
ListOffsets Response (Version: 2) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code timestamp offset 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
ListOffsets Response (Version: 3) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code timestamp offset 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
ListOffsets Response (Version: 4) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
ListOffsets Response (Version: 5) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
ListOffsets Response (Version: 6) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
ListOffsets Response (Version: 7) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
ListOffsets Response (Version: 8) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
ListOffsets Response (Version: 9) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
ListOffsets Response (Version: 10) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
ListOffsets Response (Version: 11) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code timestamp offset leader_epoch 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
timestamp	
The timestamp associated with the returned offset.
offset	
The returned offset.
leader_epoch	
The leader epoch associated with the returned offset.
Metadata API (Key: 3):
Requests:

Metadata Request (Version: 0) => [topics] 
  topics => name 
    name => STRING
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
Metadata Request (Version: 1) => [topics] 
  topics => name 
    name => STRING
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
Metadata Request (Version: 2) => [topics] 
  topics => name 
    name => STRING
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
Metadata Request (Version: 3) => [topics] 
  topics => name 
    name => STRING
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
Metadata Request (Version: 4) => [topics] allow_auto_topic_creation 
  topics => name 
    name => STRING
  allow_auto_topic_creation => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
Metadata Request (Version: 5) => [topics] allow_auto_topic_creation 
  topics => name 
    name => STRING
  allow_auto_topic_creation => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
Metadata Request (Version: 6) => [topics] allow_auto_topic_creation 
  topics => name 
    name => STRING
  allow_auto_topic_creation => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
Metadata Request (Version: 7) => [topics] allow_auto_topic_creation 
  topics => name 
    name => STRING
  allow_auto_topic_creation => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
Metadata Request (Version: 8) => [topics] allow_auto_topic_creation include_cluster_authorized_operations include_topic_authorized_operations 
  topics => name 
    name => STRING
  allow_auto_topic_creation => BOOLEAN
  include_cluster_authorized_operations => BOOLEAN
  include_topic_authorized_operations => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
include_cluster_authorized_operations	
Whether to include cluster authorized operations.
include_topic_authorized_operations	
Whether to include topic authorized operations.
Metadata Request (Version: 9) => [topics] allow_auto_topic_creation include_cluster_authorized_operations include_topic_authorized_operations 
  topics => name 
    name => COMPACT_STRING
  allow_auto_topic_creation => BOOLEAN
  include_cluster_authorized_operations => BOOLEAN
  include_topic_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to fetch metadata for.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
include_cluster_authorized_operations	
Whether to include cluster authorized operations.
include_topic_authorized_operations	
Whether to include topic authorized operations.
Metadata Request (Version: 10) => [topics] allow_auto_topic_creation include_cluster_authorized_operations include_topic_authorized_operations 
  topics => topic_id name 
    topic_id => UUID
    name => COMPACT_NULLABLE_STRING
  allow_auto_topic_creation => BOOLEAN
  include_cluster_authorized_operations => BOOLEAN
  include_topic_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to fetch metadata for.
topic_id	
The topic id.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
include_cluster_authorized_operations	
Whether to include cluster authorized operations.
include_topic_authorized_operations	
Whether to include topic authorized operations.
Metadata Request (Version: 11) => [topics] allow_auto_topic_creation include_topic_authorized_operations 
  topics => topic_id name 
    topic_id => UUID
    name => COMPACT_NULLABLE_STRING
  allow_auto_topic_creation => BOOLEAN
  include_topic_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to fetch metadata for.
topic_id	
The topic id.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
include_topic_authorized_operations	
Whether to include topic authorized operations.
Metadata Request (Version: 12) => [topics] allow_auto_topic_creation include_topic_authorized_operations 
  topics => topic_id name 
    topic_id => UUID
    name => COMPACT_NULLABLE_STRING
  allow_auto_topic_creation => BOOLEAN
  include_topic_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to fetch metadata for.
topic_id	
The topic id.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
include_topic_authorized_operations	
Whether to include topic authorized operations.
Metadata Request (Version: 13) => [topics] allow_auto_topic_creation include_topic_authorized_operations 
  topics => topic_id name 
    topic_id => UUID
    name => COMPACT_NULLABLE_STRING
  allow_auto_topic_creation => BOOLEAN
  include_topic_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to fetch metadata for.
topic_id	
The topic id.
name	
The topic name.
allow_auto_topic_creation	
If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
include_topic_authorized_operations	
Whether to include topic authorized operations.
Responses:

Metadata Response (Version: 0) => [brokers] [topics] 
  brokers => node_id host port 
    node_id => INT32
    host => STRING
    port => INT32
  topics => error_code name [partitions] 
    error_code => INT16
    name => STRING
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
Response header version: 0

Field	
Description
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
Metadata Response (Version: 1) => [brokers] controller_id [topics] 
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
Response header version: 0

Field	
Description
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
Metadata Response (Version: 2) => [brokers] cluster_id controller_id [topics] 
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
Response header version: 0

Field	
Description
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
Metadata Response (Version: 3) => throttle_time_ms [brokers] cluster_id controller_id [topics] 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
Metadata Response (Version: 4) => throttle_time_ms [brokers] cluster_id controller_id [topics] 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
Metadata Response (Version: 5) => throttle_time_ms [brokers] cluster_id controller_id [topics] 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
Metadata Response (Version: 6) => throttle_time_ms [brokers] cluster_id controller_id [topics] 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
Metadata Response (Version: 7) => throttle_time_ms [brokers] cluster_id controller_id [topics] 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
Metadata Response (Version: 8) => throttle_time_ms [brokers] cluster_id controller_id [topics] cluster_authorized_operations 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] topic_authorized_operations 
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  cluster_authorized_operations => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
topic_authorized_operations	
32-bit bitfield to represent authorized operations for this topic.
cluster_authorized_operations	
32-bit bitfield to represent authorized operations for this cluster.
Metadata Response (Version: 9) => throttle_time_ms [brokers] cluster_id controller_id [topics] cluster_authorized_operations 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] topic_authorized_operations 
    error_code => INT16
    name => COMPACT_STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  cluster_authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
topic_authorized_operations	
32-bit bitfield to represent authorized operations for this topic.
cluster_authorized_operations	
32-bit bitfield to represent authorized operations for this cluster.
Metadata Response (Version: 10) => throttle_time_ms [brokers] cluster_id controller_id [topics] cluster_authorized_operations 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations 
    error_code => INT16
    name => COMPACT_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  cluster_authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
topic_id	
The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
topic_authorized_operations	
32-bit bitfield to represent authorized operations for this topic.
cluster_authorized_operations	
32-bit bitfield to represent authorized operations for this cluster.
Metadata Response (Version: 11) => throttle_time_ms [brokers] cluster_id controller_id [topics] 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations 
    error_code => INT16
    name => COMPACT_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
topic_id	
The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
topic_authorized_operations	
32-bit bitfield to represent authorized operations for this topic.
Metadata Response (Version: 12) => throttle_time_ms [brokers] cluster_id controller_id [topics] 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations 
    error_code => INT16
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
topic_id	
The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
topic_authorized_operations	
32-bit bitfield to represent authorized operations for this topic.
Metadata Response (Version: 13) => throttle_time_ms [brokers] cluster_id controller_id [topics] error_code 
  throttle_time_ms => INT32
  brokers => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations 
    error_code => INT16
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
brokers	
A list of brokers present in the cluster.
node_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller broker.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
topic_id	
The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
offline_replicas	
The set of offline replicas of this partition.
topic_authorized_operations	
32-bit bitfield to represent authorized operations for this topic.
error_code	
The top-level error code, or 0 if there was no error.
OffsetCommit API (Key: 8):
Requests:

OffsetCommit Request (Version: 2) => group_id generation_id_or_member_epoch member_id retention_time_ms [topics] 
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  retention_time_ms => INT64
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
retention_time_ms	
The time period in ms to retain the offset.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 3) => group_id generation_id_or_member_epoch member_id retention_time_ms [topics] 
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  retention_time_ms => INT64
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
retention_time_ms	
The time period in ms to retain the offset.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 4) => group_id generation_id_or_member_epoch member_id retention_time_ms [topics] 
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  retention_time_ms => INT64
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
retention_time_ms	
The time period in ms to retain the offset.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 5) => group_id generation_id_or_member_epoch member_id [topics] 
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 6) => group_id generation_id_or_member_epoch member_id [topics] 
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of this partition.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 7) => group_id generation_id_or_member_epoch member_id group_instance_id [topics] 
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of this partition.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 8) => group_id generation_id_or_member_epoch member_id group_instance_id [topics] 
  group_id => COMPACT_STRING
  generation_id_or_member_epoch => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of this partition.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 9) => group_id generation_id_or_member_epoch member_id group_instance_id [topics] 
  group_id => COMPACT_STRING
  generation_id_or_member_epoch => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
topics	
The topics to commit offsets for.
name	
The topic name.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of this partition.
committed_metadata	
Any associated metadata the client wants to keep.
OffsetCommit Request (Version: 10) => group_id generation_id_or_member_epoch member_id group_instance_id [topics] 
  group_id => COMPACT_STRING
  generation_id_or_member_epoch => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The unique group identifier.
generation_id_or_member_epoch	
The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
member_id	
The member ID assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
topics	
The topics to commit offsets for.
topic_id	
The topic ID.
partitions	
Each partition to commit offsets for.
partition_index	
The partition index.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of this partition.
committed_metadata	
Any associated metadata the client wants to keep.
Responses:

OffsetCommit Response (Version: 2) => [topics] 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 3) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 4) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 5) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 6) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 7) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 8) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 9) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetCommit Response (Version: 10) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
topic_id	
The topic ID.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
OffsetFetch API (Key: 9):
Requests:

OffsetFetch Request (Version: 1) => group_id [topics] 
  group_id => STRING
  topics => name [partition_indexes] 
    name => STRING
    partition_indexes => INT32
Request header version: 1

Field	
Description
group_id	
The group to fetch offsets for.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
OffsetFetch Request (Version: 2) => group_id [topics] 
  group_id => STRING
  topics => name [partition_indexes] 
    name => STRING
    partition_indexes => INT32
Request header version: 1

Field	
Description
group_id	
The group to fetch offsets for.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
OffsetFetch Request (Version: 3) => group_id [topics] 
  group_id => STRING
  topics => name [partition_indexes] 
    name => STRING
    partition_indexes => INT32
Request header version: 1

Field	
Description
group_id	
The group to fetch offsets for.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
OffsetFetch Request (Version: 4) => group_id [topics] 
  group_id => STRING
  topics => name [partition_indexes] 
    name => STRING
    partition_indexes => INT32
Request header version: 1

Field	
Description
group_id	
The group to fetch offsets for.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
OffsetFetch Request (Version: 5) => group_id [topics] 
  group_id => STRING
  topics => name [partition_indexes] 
    name => STRING
    partition_indexes => INT32
Request header version: 1

Field	
Description
group_id	
The group to fetch offsets for.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
OffsetFetch Request (Version: 6) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => name [partition_indexes] 
    name => COMPACT_STRING
    partition_indexes => INT32
Request header version: 2

Field	
Description
group_id	
The group to fetch offsets for.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
OffsetFetch Request (Version: 7) => group_id [topics] require_stable 
  group_id => COMPACT_STRING
  topics => name [partition_indexes] 
    name => COMPACT_STRING
    partition_indexes => INT32
  require_stable => BOOLEAN
Request header version: 2

Field	
Description
group_id	
The group to fetch offsets for.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
require_stable	
Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
OffsetFetch Request (Version: 8) => [groups] require_stable 
  groups => group_id [topics] 
    group_id => COMPACT_STRING
    topics => name [partition_indexes] 
      name => COMPACT_STRING
      partition_indexes => INT32
  require_stable => BOOLEAN
Request header version: 2

Field	
Description
groups	
Each group we would like to fetch offsets for.
group_id	
The group ID.
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
require_stable	
Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
OffsetFetch Request (Version: 9) => [groups] require_stable 
  groups => group_id member_id member_epoch [topics] 
    group_id => COMPACT_STRING
    member_id => COMPACT_NULLABLE_STRING
    member_epoch => INT32
    topics => name [partition_indexes] 
      name => COMPACT_STRING
      partition_indexes => INT32
  require_stable => BOOLEAN
Request header version: 2

Field	
Description
groups	
Each group we would like to fetch offsets for.
group_id	
The group ID.
member_id	
The member id.
member_epoch	
The member epoch if using the new consumer protocol (KIP-848).
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
name	
The topic name.
partition_indexes	
The partition indexes we would like to fetch offsets for.
require_stable	
Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
OffsetFetch Request (Version: 10) => [groups] require_stable 
  groups => group_id member_id member_epoch [topics] 
    group_id => COMPACT_STRING
    member_id => COMPACT_NULLABLE_STRING
    member_epoch => INT32
    topics => topic_id [partition_indexes] 
      topic_id => UUID
      partition_indexes => INT32
  require_stable => BOOLEAN
Request header version: 2

Field	
Description
groups	
Each group we would like to fetch offsets for.
group_id	
The group ID.
member_id	
The member id.
member_epoch	
The member epoch if using the new consumer protocol (KIP-848).
topics	
Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
topic_id	
The topic ID.
partition_indexes	
The partition indexes we would like to fetch offsets for.
require_stable	
Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
Responses:

OffsetFetch Response (Version: 1) => [topics] 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16
Response header version: 0

Field	
Description
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
metadata	
The partition metadata.
error_code	
The error code, or 0 if there was no error.
OffsetFetch Response (Version: 2) => [topics] error_code 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16
  error_code => INT16
Response header version: 0

Field	
Description
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
metadata	
The partition metadata.
error_code	
The error code, or 0 if there was no error.
error_code	
The top-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 3) => throttle_time_ms [topics] error_code 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
metadata	
The partition metadata.
error_code	
The error code, or 0 if there was no error.
error_code	
The top-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 4) => throttle_time_ms [topics] error_code 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
metadata	
The partition metadata.
error_code	
The error code, or 0 if there was no error.
error_code	
The top-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 5) => throttle_time_ms [topics] error_code 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_leader_epoch metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      metadata => NULLABLE_STRING
      error_code => INT16
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
committed_leader_epoch	
The leader epoch.
metadata	
The partition metadata.
error_code	
The error code, or 0 if there was no error.
error_code	
The top-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 6) => throttle_time_ms [topics] error_code 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      metadata => COMPACT_NULLABLE_STRING
      error_code => INT16
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
committed_leader_epoch	
The leader epoch.
metadata	
The partition metadata.
error_code	
The error code, or 0 if there was no error.
error_code	
The top-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 7) => throttle_time_ms [topics] error_code 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      metadata => COMPACT_NULLABLE_STRING
      error_code => INT16
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
committed_leader_epoch	
The leader epoch.
metadata	
The partition metadata.
error_code	
The error code, or 0 if there was no error.
error_code	
The top-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 8) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => group_id [topics] error_code 
    group_id => COMPACT_STRING
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => partition_index committed_offset committed_leader_epoch metadata error_code 
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        metadata => COMPACT_NULLABLE_STRING
        error_code => INT16
    error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
The responses per group id.
group_id	
The group ID.
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
committed_leader_epoch	
The leader epoch.
metadata	
The partition metadata.
error_code	
The partition-level error code, or 0 if there was no error.
error_code	
The group-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 9) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => group_id [topics] error_code 
    group_id => COMPACT_STRING
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => partition_index committed_offset committed_leader_epoch metadata error_code 
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        metadata => COMPACT_NULLABLE_STRING
        error_code => INT16
    error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
The responses per group id.
group_id	
The group ID.
topics	
The responses per topic.
name	
The topic name.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
committed_leader_epoch	
The leader epoch.
metadata	
The partition metadata.
error_code	
The partition-level error code, or 0 if there was no error.
error_code	
The group-level error code, or 0 if there was no error.
OffsetFetch Response (Version: 10) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => group_id [topics] error_code 
    group_id => COMPACT_STRING
    topics => topic_id [partitions] 
      topic_id => UUID
      partitions => partition_index committed_offset committed_leader_epoch metadata error_code 
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        metadata => COMPACT_NULLABLE_STRING
        error_code => INT16
    error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
The responses per group id.
group_id	
The group ID.
topics	
The responses per topic.
topic_id	
The topic ID.
partitions	
The responses per partition.
partition_index	
The partition index.
committed_offset	
The committed message offset.
committed_leader_epoch	
The leader epoch.
metadata	
The partition metadata.
error_code	
The partition-level error code, or 0 if there was no error.
error_code	
The group-level error code, or 0 if there was no error.
FindCoordinator API (Key: 10):
Requests:

FindCoordinator Request (Version: 0) => key 
  key => STRING
Request header version: 1

Field	
Description
key	
The coordinator key.
FindCoordinator Request (Version: 1) => key key_type 
  key => STRING
  key_type => INT8
Request header version: 1

Field	
Description
key	
The coordinator key.
key_type	
The coordinator key type. (group, transaction, share).
FindCoordinator Request (Version: 2) => key key_type 
  key => STRING
  key_type => INT8
Request header version: 1

Field	
Description
key	
The coordinator key.
key_type	
The coordinator key type. (group, transaction, share).
FindCoordinator Request (Version: 3) => key key_type 
  key => COMPACT_STRING
  key_type => INT8
Request header version: 2

Field	
Description
key	
The coordinator key.
key_type	
The coordinator key type. (group, transaction, share).
FindCoordinator Request (Version: 4) => key_type [coordinator_keys] 
  key_type => INT8
  coordinator_keys => COMPACT_STRING
Request header version: 2

Field	
Description
key_type	
The coordinator key type. (group, transaction, share).
coordinator_keys	
The coordinator keys.
FindCoordinator Request (Version: 5) => key_type [coordinator_keys] 
  key_type => INT8
  coordinator_keys => COMPACT_STRING
Request header version: 2

Field	
Description
key_type	
The coordinator key type. (group, transaction, share).
coordinator_keys	
The coordinator keys.
FindCoordinator Request (Version: 6) => key_type [coordinator_keys] 
  key_type => INT8
  coordinator_keys => COMPACT_STRING
Request header version: 2

Field	
Description
key_type	
The coordinator key type. (group, transaction, share).
coordinator_keys	
The coordinator keys.
Responses:

FindCoordinator Response (Version: 0) => error_code node_id host port 
  error_code => INT16
  node_id => INT32
  host => STRING
  port => INT32
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
node_id	
The node id.
host	
The host name.
port	
The port.
FindCoordinator Response (Version: 1) => throttle_time_ms error_code error_message node_id host port 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  node_id => INT32
  host => STRING
  port => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
node_id	
The node id.
host	
The host name.
port	
The port.
FindCoordinator Response (Version: 2) => throttle_time_ms error_code error_message node_id host port 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  node_id => INT32
  host => STRING
  port => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
node_id	
The node id.
host	
The host name.
port	
The port.
FindCoordinator Response (Version: 3) => throttle_time_ms error_code error_message node_id host port 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  node_id => INT32
  host => COMPACT_STRING
  port => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
node_id	
The node id.
host	
The host name.
port	
The port.
FindCoordinator Response (Version: 4) => throttle_time_ms [coordinators] 
  throttle_time_ms => INT32
  coordinators => key node_id host port error_code error_message 
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
coordinators	
Each coordinator result in the response.
key	
The coordinator key.
node_id	
The node id.
host	
The host name.
port	
The port.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
FindCoordinator Response (Version: 5) => throttle_time_ms [coordinators] 
  throttle_time_ms => INT32
  coordinators => key node_id host port error_code error_message 
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
coordinators	
Each coordinator result in the response.
key	
The coordinator key.
node_id	
The node id.
host	
The host name.
port	
The port.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
FindCoordinator Response (Version: 6) => throttle_time_ms [coordinators] 
  throttle_time_ms => INT32
  coordinators => key node_id host port error_code error_message 
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
coordinators	
Each coordinator result in the response.
key	
The coordinator key.
node_id	
The node id.
host	
The host name.
port	
The port.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
JoinGroup API (Key: 11):
Requests:

JoinGroup Request (Version: 0) => group_id session_timeout_ms member_id protocol_type [protocols] 
  group_id => STRING
  session_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => name metadata 
    name => STRING
    metadata => BYTES
Request header version: 1

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
member_id	
The member id assigned by the group coordinator.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 1) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols] 
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => name metadata 
    name => STRING
    metadata => BYTES
Request header version: 1

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 2) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols] 
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => name metadata 
    name => STRING
    metadata => BYTES
Request header version: 1

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 3) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols] 
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => name metadata 
    name => STRING
    metadata => BYTES
Request header version: 1

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 4) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols] 
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => name metadata 
    name => STRING
    metadata => BYTES
Request header version: 1

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 5) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] 
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
  protocol_type => STRING
  protocols => name metadata 
    name => STRING
    metadata => BYTES
Request header version: 1

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 6) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] 
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => name metadata 
    name => COMPACT_STRING
    metadata => COMPACT_BYTES
Request header version: 2

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 7) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] 
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => name metadata 
    name => COMPACT_STRING
    metadata => COMPACT_BYTES
Request header version: 2

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
JoinGroup Request (Version: 8) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] reason 
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => name metadata 
    name => COMPACT_STRING
    metadata => COMPACT_BYTES
  reason => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
reason	
The reason why the member (re-)joins the group.
JoinGroup Request (Version: 9) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] reason 
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => name metadata 
    name => COMPACT_STRING
    metadata => COMPACT_BYTES
  reason => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The group identifier.
session_timeout_ms	
The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
rebalance_timeout_ms	
The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
member_id	
The member id assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
protocol_type	
The unique name the for class of protocols implemented by the group we want to join.
protocols	
The list of protocols that the member supports.
name	
The protocol name.
metadata	
The protocol metadata.
reason	
The reason why the member (re-)joins the group.
Responses:

JoinGroup Response (Version: 0) => error_code generation_id protocol_name leader member_id [members] 
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => member_id metadata 
    member_id => STRING
    metadata => BYTES
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
metadata	
The group member metadata.
JoinGroup Response (Version: 1) => error_code generation_id protocol_name leader member_id [members] 
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => member_id metadata 
    member_id => STRING
    metadata => BYTES
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
metadata	
The group member metadata.
JoinGroup Response (Version: 2) => throttle_time_ms error_code generation_id protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => member_id metadata 
    member_id => STRING
    metadata => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
metadata	
The group member metadata.
JoinGroup Response (Version: 3) => throttle_time_ms error_code generation_id protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => member_id metadata 
    member_id => STRING
    metadata => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
metadata	
The group member metadata.
JoinGroup Response (Version: 4) => throttle_time_ms error_code generation_id protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => member_id metadata 
    member_id => STRING
    metadata => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
metadata	
The group member metadata.
JoinGroup Response (Version: 5) => throttle_time_ms error_code generation_id protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => member_id group_instance_id metadata 
    member_id => STRING
    group_instance_id => NULLABLE_STRING
    metadata => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
metadata	
The group member metadata.
JoinGroup Response (Version: 6) => throttle_time_ms error_code generation_id protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => COMPACT_STRING
  leader => COMPACT_STRING
  member_id => COMPACT_STRING
  members => member_id group_instance_id metadata 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
metadata	
The group member metadata.
JoinGroup Response (Version: 7) => throttle_time_ms error_code generation_id protocol_type protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  leader => COMPACT_STRING
  member_id => COMPACT_STRING
  members => member_id group_instance_id metadata 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_type	
The group protocol name.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
metadata	
The group member metadata.
JoinGroup Response (Version: 8) => throttle_time_ms error_code generation_id protocol_type protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  leader => COMPACT_STRING
  member_id => COMPACT_STRING
  members => member_id group_instance_id metadata 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_type	
The group protocol name.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
metadata	
The group member metadata.
JoinGroup Response (Version: 9) => throttle_time_ms error_code generation_id protocol_type protocol_name leader skip_assignment member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  leader => COMPACT_STRING
  skip_assignment => BOOLEAN
  member_id => COMPACT_STRING
  members => member_id group_instance_id metadata 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
generation_id	
The generation ID of the group.
protocol_type	
The group protocol name.
protocol_name	
The group protocol selected by the coordinator.
leader	
The leader of the group.
skip_assignment	
True if the leader must skip running the assignment.
member_id	
The member ID assigned by the group coordinator.
members	
The group members.
member_id	
The group member ID.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
metadata	
The group member metadata.
Heartbeat API (Key: 12):
Requests:

Heartbeat Request (Version: 0) => group_id generation_id member_id 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
Request header version: 1

Field	
Description
group_id	
The group id.
generation_id	
The generation of the group.
member_id	
The member ID.
Heartbeat Request (Version: 1) => group_id generation_id member_id 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
Request header version: 1

Field	
Description
group_id	
The group id.
generation_id	
The generation of the group.
member_id	
The member ID.
Heartbeat Request (Version: 2) => group_id generation_id member_id 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
Request header version: 1

Field	
Description
group_id	
The group id.
generation_id	
The generation of the group.
member_id	
The member ID.
Heartbeat Request (Version: 3) => group_id generation_id member_id group_instance_id 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The group id.
generation_id	
The generation of the group.
member_id	
The member ID.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
Heartbeat Request (Version: 4) => group_id generation_id member_id group_instance_id 
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The group id.
generation_id	
The generation of the group.
member_id	
The member ID.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
Responses:

Heartbeat Response (Version: 0) => error_code 
  error_code => INT16
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
Heartbeat Response (Version: 1) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
Heartbeat Response (Version: 2) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
Heartbeat Response (Version: 3) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
Heartbeat Response (Version: 4) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
LeaveGroup API (Key: 13):
Requests:

LeaveGroup Request (Version: 0) => group_id member_id 
  group_id => STRING
  member_id => STRING
Request header version: 1

Field	
Description
group_id	
The ID of the group to leave.
member_id	
The member ID to remove from the group.
LeaveGroup Request (Version: 1) => group_id member_id 
  group_id => STRING
  member_id => STRING
Request header version: 1

Field	
Description
group_id	
The ID of the group to leave.
member_id	
The member ID to remove from the group.
LeaveGroup Request (Version: 2) => group_id member_id 
  group_id => STRING
  member_id => STRING
Request header version: 1

Field	
Description
group_id	
The ID of the group to leave.
member_id	
The member ID to remove from the group.
LeaveGroup Request (Version: 3) => group_id [members] 
  group_id => STRING
  members => member_id group_instance_id 
    member_id => STRING
    group_instance_id => NULLABLE_STRING
Request header version: 1

Field	
Description
group_id	
The ID of the group to leave.
members	
List of leaving member identities.
member_id	
The member ID to remove from the group.
group_instance_id	
The group instance ID to remove from the group.
LeaveGroup Request (Version: 4) => group_id [members] 
  group_id => COMPACT_STRING
  members => member_id group_instance_id 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The ID of the group to leave.
members	
List of leaving member identities.
member_id	
The member ID to remove from the group.
group_instance_id	
The group instance ID to remove from the group.
LeaveGroup Request (Version: 5) => group_id [members] 
  group_id => COMPACT_STRING
  members => member_id group_instance_id reason 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    reason => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
group_id	
The ID of the group to leave.
members	
List of leaving member identities.
member_id	
The member ID to remove from the group.
group_instance_id	
The group instance ID to remove from the group.
reason	
The reason why the member left the group.
Responses:

LeaveGroup Response (Version: 0) => error_code 
  error_code => INT16
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
LeaveGroup Response (Version: 1) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
LeaveGroup Response (Version: 2) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
LeaveGroup Response (Version: 3) => throttle_time_ms error_code [members] 
  throttle_time_ms => INT32
  error_code => INT16
  members => member_id group_instance_id error_code 
    member_id => STRING
    group_instance_id => NULLABLE_STRING
    error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
members	
List of leaving member responses.
member_id	
The member ID to remove from the group.
group_instance_id	
The group instance ID to remove from the group.
error_code	
The error code, or 0 if there was no error.
LeaveGroup Response (Version: 4) => throttle_time_ms error_code [members] 
  throttle_time_ms => INT32
  error_code => INT16
  members => member_id group_instance_id error_code 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
members	
List of leaving member responses.
member_id	
The member ID to remove from the group.
group_instance_id	
The group instance ID to remove from the group.
error_code	
The error code, or 0 if there was no error.
LeaveGroup Response (Version: 5) => throttle_time_ms error_code [members] 
  throttle_time_ms => INT32
  error_code => INT16
  members => member_id group_instance_id error_code 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
members	
List of leaving member responses.
member_id	
The member ID to remove from the group.
group_instance_id	
The group instance ID to remove from the group.
error_code	
The error code, or 0 if there was no error.
SyncGroup API (Key: 14):
Requests:

SyncGroup Request (Version: 0) => group_id generation_id member_id [assignments] 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  assignments => member_id assignment 
    member_id => STRING
    assignment => BYTES
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id	
The generation of the group.
member_id	
The member ID assigned by the group.
assignments	
Each assignment.
member_id	
The ID of the member to assign.
assignment	
The member assignment.
SyncGroup Request (Version: 1) => group_id generation_id member_id [assignments] 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  assignments => member_id assignment 
    member_id => STRING
    assignment => BYTES
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id	
The generation of the group.
member_id	
The member ID assigned by the group.
assignments	
Each assignment.
member_id	
The ID of the member to assign.
assignment	
The member assignment.
SyncGroup Request (Version: 2) => group_id generation_id member_id [assignments] 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  assignments => member_id assignment 
    member_id => STRING
    assignment => BYTES
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id	
The generation of the group.
member_id	
The member ID assigned by the group.
assignments	
Each assignment.
member_id	
The ID of the member to assign.
assignment	
The member assignment.
SyncGroup Request (Version: 3) => group_id generation_id member_id group_instance_id [assignments] 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
  assignments => member_id assignment 
    member_id => STRING
    assignment => BYTES
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
generation_id	
The generation of the group.
member_id	
The member ID assigned by the group.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
assignments	
Each assignment.
member_id	
The ID of the member to assign.
assignment	
The member assignment.
SyncGroup Request (Version: 4) => group_id generation_id member_id group_instance_id [assignments] 
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  assignments => member_id assignment 
    member_id => COMPACT_STRING
    assignment => COMPACT_BYTES
Request header version: 2

Field	
Description
group_id	
The unique group identifier.
generation_id	
The generation of the group.
member_id	
The member ID assigned by the group.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
assignments	
Each assignment.
member_id	
The ID of the member to assign.
assignment	
The member assignment.
SyncGroup Request (Version: 5) => group_id generation_id member_id group_instance_id protocol_type protocol_name [assignments] 
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  assignments => member_id assignment 
    member_id => COMPACT_STRING
    assignment => COMPACT_BYTES
Request header version: 2

Field	
Description
group_id	
The unique group identifier.
generation_id	
The generation of the group.
member_id	
The member ID assigned by the group.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
protocol_type	
The group protocol type.
protocol_name	
The group protocol name.
assignments	
Each assignment.
member_id	
The ID of the member to assign.
assignment	
The member assignment.
Responses:

SyncGroup Response (Version: 0) => error_code assignment 
  error_code => INT16
  assignment => BYTES
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
assignment	
The member assignment.
SyncGroup Response (Version: 1) => throttle_time_ms error_code assignment 
  throttle_time_ms => INT32
  error_code => INT16
  assignment => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
assignment	
The member assignment.
SyncGroup Response (Version: 2) => throttle_time_ms error_code assignment 
  throttle_time_ms => INT32
  error_code => INT16
  assignment => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
assignment	
The member assignment.
SyncGroup Response (Version: 3) => throttle_time_ms error_code assignment 
  throttle_time_ms => INT32
  error_code => INT16
  assignment => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
assignment	
The member assignment.
SyncGroup Response (Version: 4) => throttle_time_ms error_code assignment 
  throttle_time_ms => INT32
  error_code => INT16
  assignment => COMPACT_BYTES
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
assignment	
The member assignment.
SyncGroup Response (Version: 5) => throttle_time_ms error_code protocol_type protocol_name assignment 
  throttle_time_ms => INT32
  error_code => INT16
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  assignment => COMPACT_BYTES
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
protocol_type	
The group protocol type.
protocol_name	
The group protocol name.
assignment	
The member assignment.
DescribeGroups API (Key: 15):
Requests:

DescribeGroups Request (Version: 0) => [groups] 
  groups => STRING
Request header version: 1

Field	
Description
groups	
The names of the groups to describe.
DescribeGroups Request (Version: 1) => [groups] 
  groups => STRING
Request header version: 1

Field	
Description
groups	
The names of the groups to describe.
DescribeGroups Request (Version: 2) => [groups] 
  groups => STRING
Request header version: 1

Field	
Description
groups	
The names of the groups to describe.
DescribeGroups Request (Version: 3) => [groups] include_authorized_operations 
  groups => STRING
  include_authorized_operations => BOOLEAN
Request header version: 1

Field	
Description
groups	
The names of the groups to describe.
include_authorized_operations	
Whether to include authorized operations.
DescribeGroups Request (Version: 4) => [groups] include_authorized_operations 
  groups => STRING
  include_authorized_operations => BOOLEAN
Request header version: 1

Field	
Description
groups	
The names of the groups to describe.
include_authorized_operations	
Whether to include authorized operations.
DescribeGroups Request (Version: 5) => [groups] include_authorized_operations 
  groups => COMPACT_STRING
  include_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
groups	
The names of the groups to describe.
include_authorized_operations	
Whether to include authorized operations.
DescribeGroups Request (Version: 6) => [groups] include_authorized_operations 
  groups => COMPACT_STRING
  include_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
groups	
The names of the groups to describe.
include_authorized_operations	
Whether to include authorized operations.
Responses:

DescribeGroups Response (Version: 0) => [groups] 
  groups => error_code group_id group_state protocol_type protocol_data [members] 
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => member_id client_id client_host member_metadata member_assignment 
      member_id => STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
Response header version: 0

Field	
Description
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
protocol_type	
The group protocol type, or the empty string.
protocol_data	
The group protocol data, or the empty string.
members	
The group members.
member_id	
The member id.
client_id	
The client ID used in the member's latest join group request.
client_host	
The client host.
member_metadata	
The metadata corresponding to the current group protocol in use.
member_assignment	
The current assignment provided by the group leader.
DescribeGroups Response (Version: 1) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code group_id group_state protocol_type protocol_data [members] 
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => member_id client_id client_host member_metadata member_assignment 
      member_id => STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
protocol_type	
The group protocol type, or the empty string.
protocol_data	
The group protocol data, or the empty string.
members	
The group members.
member_id	
The member id.
client_id	
The client ID used in the member's latest join group request.
client_host	
The client host.
member_metadata	
The metadata corresponding to the current group protocol in use.
member_assignment	
The current assignment provided by the group leader.
DescribeGroups Response (Version: 2) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code group_id group_state protocol_type protocol_data [members] 
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => member_id client_id client_host member_metadata member_assignment 
      member_id => STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
protocol_type	
The group protocol type, or the empty string.
protocol_data	
The group protocol data, or the empty string.
members	
The group members.
member_id	
The member id.
client_id	
The client ID used in the member's latest join group request.
client_host	
The client host.
member_metadata	
The metadata corresponding to the current group protocol in use.
member_assignment	
The current assignment provided by the group leader.
DescribeGroups Response (Version: 3) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code group_id group_state protocol_type protocol_data [members] authorized_operations 
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => member_id client_id client_host member_metadata member_assignment 
      member_id => STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
    authorized_operations => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
protocol_type	
The group protocol type, or the empty string.
protocol_data	
The group protocol data, or the empty string.
members	
The group members.
member_id	
The member id.
client_id	
The client ID used in the member's latest join group request.
client_host	
The client host.
member_metadata	
The metadata corresponding to the current group protocol in use.
member_assignment	
The current assignment provided by the group leader.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
DescribeGroups Response (Version: 4) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code group_id group_state protocol_type protocol_data [members] authorized_operations 
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => member_id group_instance_id client_id client_host member_metadata member_assignment 
      member_id => STRING
      group_instance_id => NULLABLE_STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
    authorized_operations => INT32
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
protocol_type	
The group protocol type, or the empty string.
protocol_data	
The group protocol data, or the empty string.
members	
The group members.
member_id	
The member id.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
client_id	
The client ID used in the member's latest join group request.
client_host	
The client host.
member_metadata	
The metadata corresponding to the current group protocol in use.
member_assignment	
The current assignment provided by the group leader.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
DescribeGroups Response (Version: 5) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code group_id group_state protocol_type protocol_data [members] authorized_operations 
    error_code => INT16
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    protocol_type => COMPACT_STRING
    protocol_data => COMPACT_STRING
    members => member_id group_instance_id client_id client_host member_metadata member_assignment 
      member_id => COMPACT_STRING
      group_instance_id => COMPACT_NULLABLE_STRING
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      member_metadata => COMPACT_BYTES
      member_assignment => COMPACT_BYTES
    authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
protocol_type	
The group protocol type, or the empty string.
protocol_data	
The group protocol data, or the empty string.
members	
The group members.
member_id	
The member id.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
client_id	
The client ID used in the member's latest join group request.
client_host	
The client host.
member_metadata	
The metadata corresponding to the current group protocol in use.
member_assignment	
The current assignment provided by the group leader.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
DescribeGroups Response (Version: 6) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code error_message group_id group_state protocol_type protocol_data [members] authorized_operations 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    protocol_type => COMPACT_STRING
    protocol_data => COMPACT_STRING
    members => member_id group_instance_id client_id client_host member_metadata member_assignment 
      member_id => COMPACT_STRING
      group_instance_id => COMPACT_NULLABLE_STRING
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      member_metadata => COMPACT_BYTES
      member_assignment => COMPACT_BYTES
    authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
error_message	
The describe error message, or null if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
protocol_type	
The group protocol type, or the empty string.
protocol_data	
The group protocol data, or the empty string.
members	
The group members.
member_id	
The member id.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
client_id	
The client ID used in the member's latest join group request.
client_host	
The client host.
member_metadata	
The metadata corresponding to the current group protocol in use.
member_assignment	
The current assignment provided by the group leader.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
ListGroups API (Key: 16):
Requests:

ListGroups Request (Version: 0) => 
Request header version: 1

ListGroups Request (Version: 1) => 
Request header version: 1

ListGroups Request (Version: 2) => 
Request header version: 1

ListGroups Request (Version: 3) => 
Request header version: 2

ListGroups Request (Version: 4) => [states_filter] 
  states_filter => COMPACT_STRING
Request header version: 2

Field	
Description
states_filter	
The states of the groups we want to list. If empty, all groups are returned with their state.
ListGroups Request (Version: 5) => [states_filter] [types_filter] 
  states_filter => COMPACT_STRING
  types_filter => COMPACT_STRING
Request header version: 2

Field	
Description
states_filter	
The states of the groups we want to list. If empty, all groups are returned with their state.
types_filter	
The types of the groups we want to list. If empty, all groups are returned with their type.
Responses:

ListGroups Response (Version: 0) => error_code [groups] 
  error_code => INT16
  groups => group_id protocol_type 
    group_id => STRING
    protocol_type => STRING
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
groups	
Each group in the response.
group_id	
The group ID.
protocol_type	
The group protocol type.
ListGroups Response (Version: 1) => throttle_time_ms error_code [groups] 
  throttle_time_ms => INT32
  error_code => INT16
  groups => group_id protocol_type 
    group_id => STRING
    protocol_type => STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
groups	
Each group in the response.
group_id	
The group ID.
protocol_type	
The group protocol type.
ListGroups Response (Version: 2) => throttle_time_ms error_code [groups] 
  throttle_time_ms => INT32
  error_code => INT16
  groups => group_id protocol_type 
    group_id => STRING
    protocol_type => STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
groups	
Each group in the response.
group_id	
The group ID.
protocol_type	
The group protocol type.
ListGroups Response (Version: 3) => throttle_time_ms error_code [groups] 
  throttle_time_ms => INT32
  error_code => INT16
  groups => group_id protocol_type 
    group_id => COMPACT_STRING
    protocol_type => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
groups	
Each group in the response.
group_id	
The group ID.
protocol_type	
The group protocol type.
ListGroups Response (Version: 4) => throttle_time_ms error_code [groups] 
  throttle_time_ms => INT32
  error_code => INT16
  groups => group_id protocol_type group_state 
    group_id => COMPACT_STRING
    protocol_type => COMPACT_STRING
    group_state => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
groups	
Each group in the response.
group_id	
The group ID.
protocol_type	
The group protocol type.
group_state	
The group state name.
ListGroups Response (Version: 5) => throttle_time_ms error_code [groups] 
  throttle_time_ms => INT32
  error_code => INT16
  groups => group_id protocol_type group_state group_type 
    group_id => COMPACT_STRING
    protocol_type => COMPACT_STRING
    group_state => COMPACT_STRING
    group_type => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
groups	
Each group in the response.
group_id	
The group ID.
protocol_type	
The group protocol type.
group_state	
The group state name.
group_type	
The group type name.
SaslHandshake API (Key: 17):
Requests:

SaslHandshake Request (Version: 0) => mechanism 
  mechanism => STRING
Request header version: 1

Field	
Description
mechanism	
The SASL mechanism chosen by the client.
SaslHandshake Request (Version: 1) => mechanism 
  mechanism => STRING
Request header version: 1

Field	
Description
mechanism	
The SASL mechanism chosen by the client.
Responses:

SaslHandshake Response (Version: 0) => error_code [mechanisms] 
  error_code => INT16
  mechanisms => STRING
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
mechanisms	
The mechanisms enabled in the server.
SaslHandshake Response (Version: 1) => error_code [mechanisms] 
  error_code => INT16
  mechanisms => STRING
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
mechanisms	
The mechanisms enabled in the server.
ApiVersions API (Key: 18):
Requests:

ApiVersions Request (Version: 0) => 
Request header version: 1

ApiVersions Request (Version: 1) => 
Request header version: 1

ApiVersions Request (Version: 2) => 
Request header version: 1

ApiVersions Request (Version: 3) => client_software_name client_software_version 
  client_software_name => COMPACT_STRING
  client_software_version => COMPACT_STRING
Request header version: 2

Field	
Description
client_software_name	
The name of the client.
client_software_version	
The version of the client.
ApiVersions Request (Version: 4) => client_software_name client_software_version 
  client_software_name => COMPACT_STRING
  client_software_version => COMPACT_STRING
Request header version: 2

Field	
Description
client_software_name	
The name of the client.
client_software_version	
The version of the client.
Responses:

ApiVersions Response (Version: 0) => error_code [api_keys] 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
Response header version: 0

Field	
Description
error_code	
The top-level error code.
api_keys	
The APIs supported by the broker.
api_key	
The API index.
min_version	
The minimum supported version, inclusive.
max_version	
The maximum supported version, inclusive.
ApiVersions Response (Version: 1) => error_code [api_keys] throttle_time_ms 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
error_code	
The top-level error code.
api_keys	
The APIs supported by the broker.
api_key	
The API index.
min_version	
The minimum supported version, inclusive.
max_version	
The maximum supported version, inclusive.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
ApiVersions Response (Version: 2) => error_code [api_keys] throttle_time_ms 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
error_code	
The top-level error code.
api_keys	
The APIs supported by the broker.
api_key	
The API index.
min_version	
The minimum supported version, inclusive.
max_version	
The maximum supported version, inclusive.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms [supported_features]<tag: 0> finalized_features_epoch<tag: 1> [finalized_features]<tag: 2> zk_migration_ready<tag: 3> 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
  supported_features<tag: 0> => name min_version max_version 
    name => COMPACT_STRING
    min_version => INT16
    max_version => INT16
  finalized_features_epoch<tag: 1> => INT64
  finalized_features<tag: 2> => name max_version_level min_version_level 
    name => COMPACT_STRING
    max_version_level => INT16
    min_version_level => INT16
  zk_migration_ready<tag: 3> => BOOLEAN
Response header version: 0

Field	
Description
error_code	
The top-level error code.
api_keys	
The APIs supported by the broker.
api_key	
The API index.
min_version	
The minimum supported version, inclusive.
max_version	
The maximum supported version, inclusive.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
supported_features<tag: 0>	
Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
Field	Description
name	The name of the feature.
min_version	The minimum supported version for the feature.
max_version	The maximum supported version for the feature.
finalized_features_epoch<tag: 1>	
The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
finalized_features<tag: 2>	
List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
Field	Description
name	The name of the feature.
max_version_level	The cluster-wide finalized max version level for the feature.
min_version_level	The cluster-wide finalized min version level for the feature.
zk_migration_ready<tag: 3>	
Set by a KRaft controller if the required configurations for ZK migration are present.
ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms [supported_features]<tag: 0> finalized_features_epoch<tag: 1> [finalized_features]<tag: 2> zk_migration_ready<tag: 3> 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
  supported_features<tag: 0> => name min_version max_version 
    name => COMPACT_STRING
    min_version => INT16
    max_version => INT16
  finalized_features_epoch<tag: 1> => INT64
  finalized_features<tag: 2> => name max_version_level min_version_level 
    name => COMPACT_STRING
    max_version_level => INT16
    min_version_level => INT16
  zk_migration_ready<tag: 3> => BOOLEAN
Response header version: 0

Field	
Description
error_code	
The top-level error code.
api_keys	
The APIs supported by the broker.
api_key	
The API index.
min_version	
The minimum supported version, inclusive.
max_version	
The maximum supported version, inclusive.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
supported_features<tag: 0>	
Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
Field	Description
name	The name of the feature.
min_version	The minimum supported version for the feature.
max_version	The maximum supported version for the feature.
finalized_features_epoch<tag: 1>	
The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
finalized_features<tag: 2>	
List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
Field	Description
name	The name of the feature.
max_version_level	The cluster-wide finalized max version level for the feature.
min_version_level	The cluster-wide finalized min version level for the feature.
zk_migration_ready<tag: 3>	
Set by a KRaft controller if the required configurations for ZK migration are present.
CreateTopics API (Key: 19):
Requests:

CreateTopics Request (Version: 2) => [topics] timeout_ms validate_only 
  topics => name num_partitions replication_factor [assignments] [configs] 
    name => STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] 
      partition_index => INT32
      broker_ids => INT32
    configs => name value 
      name => STRING
      value => NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to create.
name	
The topic name.
num_partitions	
The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
replication_factor	
The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
assignments	
The manual partition assignment, or the empty array if we are using automatic assignment.
partition_index	
The partition index.
broker_ids	
The brokers to place the partition on.
configs	
The custom topic configurations to set.
name	
The configuration name.
value	
The configuration value.
timeout_ms	
How long to wait in milliseconds before timing out the request.
validate_only	
If true, check that the topics can be created as specified, but don't create anything.
CreateTopics Request (Version: 3) => [topics] timeout_ms validate_only 
  topics => name num_partitions replication_factor [assignments] [configs] 
    name => STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] 
      partition_index => INT32
      broker_ids => INT32
    configs => name value 
      name => STRING
      value => NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to create.
name	
The topic name.
num_partitions	
The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
replication_factor	
The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
assignments	
The manual partition assignment, or the empty array if we are using automatic assignment.
partition_index	
The partition index.
broker_ids	
The brokers to place the partition on.
configs	
The custom topic configurations to set.
name	
The configuration name.
value	
The configuration value.
timeout_ms	
How long to wait in milliseconds before timing out the request.
validate_only	
If true, check that the topics can be created as specified, but don't create anything.
CreateTopics Request (Version: 4) => [topics] timeout_ms validate_only 
  topics => name num_partitions replication_factor [assignments] [configs] 
    name => STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] 
      partition_index => INT32
      broker_ids => INT32
    configs => name value 
      name => STRING
      value => NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
topics	
The topics to create.
name	
The topic name.
num_partitions	
The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
replication_factor	
The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
assignments	
The manual partition assignment, or the empty array if we are using automatic assignment.
partition_index	
The partition index.
broker_ids	
The brokers to place the partition on.
configs	
The custom topic configurations to set.
name	
The configuration name.
value	
The configuration value.
timeout_ms	
How long to wait in milliseconds before timing out the request.
validate_only	
If true, check that the topics can be created as specified, but don't create anything.
CreateTopics Request (Version: 5) => [topics] timeout_ms validate_only 
  topics => name num_partitions replication_factor [assignments] [configs] 
    name => COMPACT_STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] 
      partition_index => INT32
      broker_ids => INT32
    configs => name value 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to create.
name	
The topic name.
num_partitions	
The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
replication_factor	
The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
assignments	
The manual partition assignment, or the empty array if we are using automatic assignment.
partition_index	
The partition index.
broker_ids	
The brokers to place the partition on.
configs	
The custom topic configurations to set.
name	
The configuration name.
value	
The configuration value.
timeout_ms	
How long to wait in milliseconds before timing out the request.
validate_only	
If true, check that the topics can be created as specified, but don't create anything.
CreateTopics Request (Version: 6) => [topics] timeout_ms validate_only 
  topics => name num_partitions replication_factor [assignments] [configs] 
    name => COMPACT_STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] 
      partition_index => INT32
      broker_ids => INT32
    configs => name value 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to create.
name	
The topic name.
num_partitions	
The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
replication_factor	
The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
assignments	
The manual partition assignment, or the empty array if we are using automatic assignment.
partition_index	
The partition index.
broker_ids	
The brokers to place the partition on.
configs	
The custom topic configurations to set.
name	
The configuration name.
value	
The configuration value.
timeout_ms	
How long to wait in milliseconds before timing out the request.
validate_only	
If true, check that the topics can be created as specified, but don't create anything.
CreateTopics Request (Version: 7) => [topics] timeout_ms validate_only 
  topics => name num_partitions replication_factor [assignments] [configs] 
    name => COMPACT_STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] 
      partition_index => INT32
      broker_ids => INT32
    configs => name value 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
topics	
The topics to create.
name	
The topic name.
num_partitions	
The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
replication_factor	
The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
assignments	
The manual partition assignment, or the empty array if we are using automatic assignment.
partition_index	
The partition index.
broker_ids	
The brokers to place the partition on.
configs	
The custom topic configurations to set.
name	
The configuration name.
value	
The configuration value.
timeout_ms	
How long to wait in milliseconds before timing out the request.
validate_only	
If true, check that the topics can be created as specified, but don't create anything.
Responses:

CreateTopics Response (Version: 2) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name error_code error_message 
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Results for each topic we tried to create.
name	
The topic name.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
CreateTopics Response (Version: 3) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name error_code error_message 
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Results for each topic we tried to create.
name	
The topic name.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
CreateTopics Response (Version: 4) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name error_code error_message 
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Results for each topic we tried to create.
name	
The topic name.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
CreateTopics Response (Version: 5) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name error_code error_message num_partitions replication_factor [configs] topic_config_error_code<tag: 0> 
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    num_partitions => INT32
    replication_factor => INT16
    configs => name value read_only config_source is_sensitive 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
    topic_config_error_code<tag: 0> => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Results for each topic we tried to create.
name	
The topic name.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
num_partitions	
Number of partitions of the topic.
replication_factor	
Replication factor of the topic.
configs	
Configuration of the topic.
name	
The configuration name.
value	
The configuration value.
read_only	
True if the configuration is read-only.
config_source	
The configuration source.
is_sensitive	
True if this configuration is sensitive.
topic_config_error_code<tag: 0>	
Optional topic config error returned if configs are not returned in the response.
CreateTopics Response (Version: 6) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name error_code error_message num_partitions replication_factor [configs] topic_config_error_code<tag: 0> 
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    num_partitions => INT32
    replication_factor => INT16
    configs => name value read_only config_source is_sensitive 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
    topic_config_error_code<tag: 0> => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Results for each topic we tried to create.
name	
The topic name.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
num_partitions	
Number of partitions of the topic.
replication_factor	
Replication factor of the topic.
configs	
Configuration of the topic.
name	
The configuration name.
value	
The configuration value.
read_only	
True if the configuration is read-only.
config_source	
The configuration source.
is_sensitive	
True if this configuration is sensitive.
topic_config_error_code<tag: 0>	
Optional topic config error returned if configs are not returned in the response.
CreateTopics Response (Version: 7) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name topic_id error_code error_message num_partitions replication_factor [configs] topic_config_error_code<tag: 0> 
    name => COMPACT_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    num_partitions => INT32
    replication_factor => INT16
    configs => name value read_only config_source is_sensitive 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
    topic_config_error_code<tag: 0> => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Results for each topic we tried to create.
name	
The topic name.
topic_id	
The unique topic ID.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
num_partitions	
Number of partitions of the topic.
replication_factor	
Replication factor of the topic.
configs	
Configuration of the topic.
name	
The configuration name.
value	
The configuration value.
read_only	
True if the configuration is read-only.
config_source	
The configuration source.
is_sensitive	
True if this configuration is sensitive.
topic_config_error_code<tag: 0>	
Optional topic config error returned if configs are not returned in the response.
DeleteTopics API (Key: 20):
Requests:

DeleteTopics Request (Version: 1) => [topic_names] timeout_ms 
  topic_names => STRING
  timeout_ms => INT32
Request header version: 1

Field	
Description
topic_names	
The names of the topics to delete.
timeout_ms	
The length of time in milliseconds to wait for the deletions to complete.
DeleteTopics Request (Version: 2) => [topic_names] timeout_ms 
  topic_names => STRING
  timeout_ms => INT32
Request header version: 1

Field	
Description
topic_names	
The names of the topics to delete.
timeout_ms	
The length of time in milliseconds to wait for the deletions to complete.
DeleteTopics Request (Version: 3) => [topic_names] timeout_ms 
  topic_names => STRING
  timeout_ms => INT32
Request header version: 1

Field	
Description
topic_names	
The names of the topics to delete.
timeout_ms	
The length of time in milliseconds to wait for the deletions to complete.
DeleteTopics Request (Version: 4) => [topic_names] timeout_ms 
  topic_names => COMPACT_STRING
  timeout_ms => INT32
Request header version: 2

Field	
Description
topic_names	
The names of the topics to delete.
timeout_ms	
The length of time in milliseconds to wait for the deletions to complete.
DeleteTopics Request (Version: 5) => [topic_names] timeout_ms 
  topic_names => COMPACT_STRING
  timeout_ms => INT32
Request header version: 2

Field	
Description
topic_names	
The names of the topics to delete.
timeout_ms	
The length of time in milliseconds to wait for the deletions to complete.
DeleteTopics Request (Version: 6) => [topics] timeout_ms 
  topics => name topic_id 
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
  timeout_ms => INT32
Request header version: 2

Field	
Description
topics	
The name or topic ID of the topic.
name	
The topic name.
topic_id	
The unique topic ID.
timeout_ms	
The length of time in milliseconds to wait for the deletions to complete.
Responses:

DeleteTopics Response (Version: 1) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => name error_code 
    name => STRING
    error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The results for each topic we tried to delete.
name	
The topic name.
error_code	
The deletion error, or 0 if the deletion succeeded.
DeleteTopics Response (Version: 2) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => name error_code 
    name => STRING
    error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The results for each topic we tried to delete.
name	
The topic name.
error_code	
The deletion error, or 0 if the deletion succeeded.
DeleteTopics Response (Version: 3) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => name error_code 
    name => STRING
    error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The results for each topic we tried to delete.
name	
The topic name.
error_code	
The deletion error, or 0 if the deletion succeeded.
DeleteTopics Response (Version: 4) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => name error_code 
    name => COMPACT_STRING
    error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The results for each topic we tried to delete.
name	
The topic name.
error_code	
The deletion error, or 0 if the deletion succeeded.
DeleteTopics Response (Version: 5) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => name error_code error_message 
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The results for each topic we tried to delete.
name	
The topic name.
error_code	
The deletion error, or 0 if the deletion succeeded.
error_message	
The error message, or null if there was no error.
DeleteTopics Response (Version: 6) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => name topic_id error_code error_message 
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The results for each topic we tried to delete.
name	
The topic name.
topic_id	
The unique topic ID.
error_code	
The deletion error, or 0 if the deletion succeeded.
error_message	
The error message, or null if there was no error.
DeleteRecords API (Key: 21):
Requests:

DeleteRecords Request (Version: 0) => [topics] timeout_ms 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index offset 
      partition_index => INT32
      offset => INT64
  timeout_ms => INT32
Request header version: 1

Field	
Description
topics	
Each topic that we want to delete records from.
name	
The topic name.
partitions	
Each partition that we want to delete records from.
partition_index	
The partition index.
offset	
The deletion offset. -1 means that records should be truncated to the high watermark.
timeout_ms	
How long to wait for the deletion to complete, in milliseconds.
DeleteRecords Request (Version: 1) => [topics] timeout_ms 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index offset 
      partition_index => INT32
      offset => INT64
  timeout_ms => INT32
Request header version: 1

Field	
Description
topics	
Each topic that we want to delete records from.
name	
The topic name.
partitions	
Each partition that we want to delete records from.
partition_index	
The partition index.
offset	
The deletion offset. -1 means that records should be truncated to the high watermark.
timeout_ms	
How long to wait for the deletion to complete, in milliseconds.
DeleteRecords Request (Version: 2) => [topics] timeout_ms 
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index offset 
      partition_index => INT32
      offset => INT64
  timeout_ms => INT32
Request header version: 2

Field	
Description
topics	
Each topic that we want to delete records from.
name	
The topic name.
partitions	
Each partition that we want to delete records from.
partition_index	
The partition index.
offset	
The deletion offset. -1 means that records should be truncated to the high watermark.
timeout_ms	
How long to wait for the deletion to complete, in milliseconds.
Responses:

DeleteRecords Response (Version: 0) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index low_watermark error_code 
      partition_index => INT32
      low_watermark => INT64
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic that we wanted to delete records from.
name	
The topic name.
partitions	
Each partition that we wanted to delete records from.
partition_index	
The partition index.
low_watermark	
The partition low water mark.
error_code	
The deletion error code, or 0 if the deletion succeeded.
DeleteRecords Response (Version: 1) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index low_watermark error_code 
      partition_index => INT32
      low_watermark => INT64
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic that we wanted to delete records from.
name	
The topic name.
partitions	
Each partition that we wanted to delete records from.
partition_index	
The partition index.
low_watermark	
The partition low water mark.
error_code	
The deletion error code, or 0 if the deletion succeeded.
DeleteRecords Response (Version: 2) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index low_watermark error_code 
      partition_index => INT32
      low_watermark => INT64
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic that we wanted to delete records from.
name	
The topic name.
partitions	
Each partition that we wanted to delete records from.
partition_index	
The partition index.
low_watermark	
The partition low water mark.
error_code	
The deletion error code, or 0 if the deletion succeeded.
InitProducerId API (Key: 22):
Requests:

InitProducerId Request (Version: 0) => transactional_id transaction_timeout_ms 
  transactional_id => NULLABLE_STRING
  transaction_timeout_ms => INT32
Request header version: 1

Field	
Description
transactional_id	
The transactional id, or null if the producer is not transactional.
transaction_timeout_ms	
The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
InitProducerId Request (Version: 1) => transactional_id transaction_timeout_ms 
  transactional_id => NULLABLE_STRING
  transaction_timeout_ms => INT32
Request header version: 1

Field	
Description
transactional_id	
The transactional id, or null if the producer is not transactional.
transaction_timeout_ms	
The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
InitProducerId Request (Version: 2) => transactional_id transaction_timeout_ms 
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
Request header version: 2

Field	
Description
transactional_id	
The transactional id, or null if the producer is not transactional.
transaction_timeout_ms	
The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
InitProducerId Request (Version: 3) => transactional_id transaction_timeout_ms producer_id producer_epoch 
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16
Request header version: 2

Field	
Description
transactional_id	
The transactional id, or null if the producer is not transactional.
transaction_timeout_ms	
The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
producer_id	
The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
producer_epoch	
The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
InitProducerId Request (Version: 4) => transactional_id transaction_timeout_ms producer_id producer_epoch 
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16
Request header version: 2

Field	
Description
transactional_id	
The transactional id, or null if the producer is not transactional.
transaction_timeout_ms	
The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
producer_id	
The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
producer_epoch	
The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
InitProducerId Request (Version: 5) => transactional_id transaction_timeout_ms producer_id producer_epoch 
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16
Request header version: 2

Field	
Description
transactional_id	
The transactional id, or null if the producer is not transactional.
transaction_timeout_ms	
The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
producer_id	
The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
producer_epoch	
The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
InitProducerId Request (Version: 6) => transactional_id transaction_timeout_ms producer_id producer_epoch enable2_pc keep_prepared_txn 
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16
  enable2_pc => BOOLEAN
  keep_prepared_txn => BOOLEAN
This version of the request is unstable.

Request header version: 2

Field	
Description
transactional_id	
The transactional id, or null if the producer is not transactional.
transaction_timeout_ms	
The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
producer_id	
The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
producer_epoch	
The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
enable2_pc	
True if the client wants to enable two-phase commit (2PC) protocol for transactions.
keep_prepared_txn	
True if the client wants to keep the currently ongoing transaction instead of aborting it.
Responses:

InitProducerId Response (Version: 0) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The current producer id.
producer_epoch	
The current epoch associated with the producer id.
InitProducerId Response (Version: 1) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The current producer id.
producer_epoch	
The current epoch associated with the producer id.
InitProducerId Response (Version: 2) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The current producer id.
producer_epoch	
The current epoch associated with the producer id.
InitProducerId Response (Version: 3) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The current producer id.
producer_epoch	
The current epoch associated with the producer id.
InitProducerId Response (Version: 4) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The current producer id.
producer_epoch	
The current epoch associated with the producer id.
InitProducerId Response (Version: 5) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The current producer id.
producer_epoch	
The current epoch associated with the producer id.
InitProducerId Response (Version: 6) => throttle_time_ms error_code producer_id producer_epoch ongoing_txn_producer_id ongoing_txn_producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
  ongoing_txn_producer_id => INT64
  ongoing_txn_producer_epoch => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The current producer id.
producer_epoch	
The current epoch associated with the producer id.
ongoing_txn_producer_id	
The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
ongoing_txn_producer_epoch	
The epoch associated with the producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
OffsetForLeaderEpoch API (Key: 23):
Requests:

OffsetForLeaderEpoch Request (Version: 2) => [topics] 
  topics => topic [partitions] 
    topic => STRING
    partitions => partition current_leader_epoch leader_epoch 
      partition => INT32
      current_leader_epoch => INT32
      leader_epoch => INT32
Request header version: 1

Field	
Description
topics	
Each topic to get offsets for.
topic	
The topic name.
partitions	
Each partition to get offsets for.
partition	
The partition index.
current_leader_epoch	
An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
leader_epoch	
The epoch to look up an offset for.
OffsetForLeaderEpoch Request (Version: 3) => replica_id [topics] 
  replica_id => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition current_leader_epoch leader_epoch 
      partition => INT32
      current_leader_epoch => INT32
      leader_epoch => INT32
Request header version: 1

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
topics	
Each topic to get offsets for.
topic	
The topic name.
partitions	
Each partition to get offsets for.
partition	
The partition index.
current_leader_epoch	
An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
leader_epoch	
The epoch to look up an offset for.
OffsetForLeaderEpoch Request (Version: 4) => replica_id [topics] 
  replica_id => INT32
  topics => topic [partitions] 
    topic => COMPACT_STRING
    partitions => partition current_leader_epoch leader_epoch 
      partition => INT32
      current_leader_epoch => INT32
      leader_epoch => INT32
Request header version: 2

Field	
Description
replica_id	
The broker ID of the follower, of -1 if this request is from a consumer.
topics	
Each topic to get offsets for.
topic	
The topic name.
partitions	
Each partition to get offsets for.
partition	
The partition index.
current_leader_epoch	
An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
leader_epoch	
The epoch to look up an offset for.
Responses:

OffsetForLeaderEpoch Response (Version: 2) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => error_code partition leader_epoch end_offset 
      error_code => INT16
      partition => INT32
      leader_epoch => INT32
      end_offset => INT64
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic we fetched offsets for.
topic	
The topic name.
partitions	
Each partition in the topic we fetched offsets for.
error_code	
The error code 0, or if there was no error.
partition	
The partition index.
leader_epoch	
The leader epoch of the partition.
end_offset	
The end offset of the epoch.
OffsetForLeaderEpoch Response (Version: 3) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => error_code partition leader_epoch end_offset 
      error_code => INT16
      partition => INT32
      leader_epoch => INT32
      end_offset => INT64
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic we fetched offsets for.
topic	
The topic name.
partitions	
Each partition in the topic we fetched offsets for.
error_code	
The error code 0, or if there was no error.
partition	
The partition index.
leader_epoch	
The leader epoch of the partition.
end_offset	
The end offset of the epoch.
OffsetForLeaderEpoch Response (Version: 4) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => topic [partitions] 
    topic => COMPACT_STRING
    partitions => error_code partition leader_epoch end_offset 
      error_code => INT16
      partition => INT32
      leader_epoch => INT32
      end_offset => INT64
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic we fetched offsets for.
topic	
The topic name.
partitions	
Each partition in the topic we fetched offsets for.
error_code	
The error code 0, or if there was no error.
partition	
The partition index.
leader_epoch	
The leader epoch of the partition.
end_offset	
The end offset of the epoch.
AddPartitionsToTxn API (Key: 24):
Requests:

AddPartitionsToTxn Request (Version: 0) => v3_and_below_transactional_id v3_and_below_producer_id v3_and_below_producer_epoch [v3_and_below_topics] 
  v3_and_below_transactional_id => STRING
  v3_and_below_producer_id => INT64
  v3_and_below_producer_epoch => INT16
  v3_and_below_topics => name [partitions] 
    name => STRING
    partitions => INT32
Request header version: 1

Field	
Description
v3_and_below_transactional_id	
The transactional id corresponding to the transaction.
v3_and_below_producer_id	
Current producer id in use by the transactional id.
v3_and_below_producer_epoch	
Current epoch associated with the producer id.
v3_and_below_topics	
The partitions to add to the transaction.
name	
The name of the topic.
partitions	
The partition indexes to add to the transaction.
AddPartitionsToTxn Request (Version: 1) => v3_and_below_transactional_id v3_and_below_producer_id v3_and_below_producer_epoch [v3_and_below_topics] 
  v3_and_below_transactional_id => STRING
  v3_and_below_producer_id => INT64
  v3_and_below_producer_epoch => INT16
  v3_and_below_topics => name [partitions] 
    name => STRING
    partitions => INT32
Request header version: 1

Field	
Description
v3_and_below_transactional_id	
The transactional id corresponding to the transaction.
v3_and_below_producer_id	
Current producer id in use by the transactional id.
v3_and_below_producer_epoch	
Current epoch associated with the producer id.
v3_and_below_topics	
The partitions to add to the transaction.
name	
The name of the topic.
partitions	
The partition indexes to add to the transaction.
AddPartitionsToTxn Request (Version: 2) => v3_and_below_transactional_id v3_and_below_producer_id v3_and_below_producer_epoch [v3_and_below_topics] 
  v3_and_below_transactional_id => STRING
  v3_and_below_producer_id => INT64
  v3_and_below_producer_epoch => INT16
  v3_and_below_topics => name [partitions] 
    name => STRING
    partitions => INT32
Request header version: 1

Field	
Description
v3_and_below_transactional_id	
The transactional id corresponding to the transaction.
v3_and_below_producer_id	
Current producer id in use by the transactional id.
v3_and_below_producer_epoch	
Current epoch associated with the producer id.
v3_and_below_topics	
The partitions to add to the transaction.
name	
The name of the topic.
partitions	
The partition indexes to add to the transaction.
AddPartitionsToTxn Request (Version: 3) => v3_and_below_transactional_id v3_and_below_producer_id v3_and_below_producer_epoch [v3_and_below_topics] 
  v3_and_below_transactional_id => COMPACT_STRING
  v3_and_below_producer_id => INT64
  v3_and_below_producer_epoch => INT16
  v3_and_below_topics => name [partitions] 
    name => COMPACT_STRING
    partitions => INT32
Request header version: 2

Field	
Description
v3_and_below_transactional_id	
The transactional id corresponding to the transaction.
v3_and_below_producer_id	
Current producer id in use by the transactional id.
v3_and_below_producer_epoch	
Current epoch associated with the producer id.
v3_and_below_topics	
The partitions to add to the transaction.
name	
The name of the topic.
partitions	
The partition indexes to add to the transaction.
AddPartitionsToTxn Request (Version: 4) => [transactions] 
  transactions => transactional_id producer_id producer_epoch verify_only [topics] 
    transactional_id => COMPACT_STRING
    producer_id => INT64
    producer_epoch => INT16
    verify_only => BOOLEAN
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => INT32
Request header version: 2

Field	
Description
transactions	
List of transactions to add partitions to.
transactional_id	
The transactional id corresponding to the transaction.
producer_id	
Current producer id in use by the transactional id.
producer_epoch	
Current epoch associated with the producer id.
verify_only	
Boolean to signify if we want to check if the partition is in the transaction rather than add it.
topics	
The partitions to add to the transaction.
name	
The name of the topic.
partitions	
The partition indexes to add to the transaction.
AddPartitionsToTxn Request (Version: 5) => [transactions] 
  transactions => transactional_id producer_id producer_epoch verify_only [topics] 
    transactional_id => COMPACT_STRING
    producer_id => INT64
    producer_epoch => INT16
    verify_only => BOOLEAN
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => INT32
Request header version: 2

Field	
Description
transactions	
List of transactions to add partitions to.
transactional_id	
The transactional id corresponding to the transaction.
producer_id	
Current producer id in use by the transactional id.
producer_epoch	
Current epoch associated with the producer id.
verify_only	
Boolean to signify if we want to check if the partition is in the transaction rather than add it.
topics	
The partitions to add to the transaction.
name	
The name of the topic.
partitions	
The partition indexes to add to the transaction.
Responses:

AddPartitionsToTxn Response (Version: 0) => throttle_time_ms [results_by_topic_v3_and_below] 
  throttle_time_ms => INT32
  results_by_topic_v3_and_below => name [results_by_partition] 
    name => STRING
    results_by_partition => partition_index partition_error_code 
      partition_index => INT32
      partition_error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results_by_topic_v3_and_below	
The results for each topic.
name	
The topic name.
results_by_partition	
The results for each partition.
partition_index	
The partition indexes.
partition_error_code	
The response error code.
AddPartitionsToTxn Response (Version: 1) => throttle_time_ms [results_by_topic_v3_and_below] 
  throttle_time_ms => INT32
  results_by_topic_v3_and_below => name [results_by_partition] 
    name => STRING
    results_by_partition => partition_index partition_error_code 
      partition_index => INT32
      partition_error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results_by_topic_v3_and_below	
The results for each topic.
name	
The topic name.
results_by_partition	
The results for each partition.
partition_index	
The partition indexes.
partition_error_code	
The response error code.
AddPartitionsToTxn Response (Version: 2) => throttle_time_ms [results_by_topic_v3_and_below] 
  throttle_time_ms => INT32
  results_by_topic_v3_and_below => name [results_by_partition] 
    name => STRING
    results_by_partition => partition_index partition_error_code 
      partition_index => INT32
      partition_error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results_by_topic_v3_and_below	
The results for each topic.
name	
The topic name.
results_by_partition	
The results for each partition.
partition_index	
The partition indexes.
partition_error_code	
The response error code.
AddPartitionsToTxn Response (Version: 3) => throttle_time_ms [results_by_topic_v3_and_below] 
  throttle_time_ms => INT32
  results_by_topic_v3_and_below => name [results_by_partition] 
    name => COMPACT_STRING
    results_by_partition => partition_index partition_error_code 
      partition_index => INT32
      partition_error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results_by_topic_v3_and_below	
The results for each topic.
name	
The topic name.
results_by_partition	
The results for each partition.
partition_index	
The partition indexes.
partition_error_code	
The response error code.
AddPartitionsToTxn Response (Version: 4) => throttle_time_ms error_code [results_by_transaction] 
  throttle_time_ms => INT32
  error_code => INT16
  results_by_transaction => transactional_id [topic_results] 
    transactional_id => COMPACT_STRING
    topic_results => name [results_by_partition] 
      name => COMPACT_STRING
      results_by_partition => partition_index partition_error_code 
        partition_index => INT32
        partition_error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The response top level error code.
results_by_transaction	
Results categorized by transactional ID.
transactional_id	
The transactional id corresponding to the transaction.
topic_results	
The results for each topic.
name	
The topic name.
results_by_partition	
The results for each partition.
partition_index	
The partition indexes.
partition_error_code	
The response error code.
AddPartitionsToTxn Response (Version: 5) => throttle_time_ms error_code [results_by_transaction] 
  throttle_time_ms => INT32
  error_code => INT16
  results_by_transaction => transactional_id [topic_results] 
    transactional_id => COMPACT_STRING
    topic_results => name [results_by_partition] 
      name => COMPACT_STRING
      results_by_partition => partition_index partition_error_code 
        partition_index => INT32
        partition_error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The response top level error code.
results_by_transaction	
Results categorized by transactional ID.
transactional_id	
The transactional id corresponding to the transaction.
topic_results	
The results for each topic.
name	
The topic name.
results_by_partition	
The results for each partition.
partition_index	
The partition indexes.
partition_error_code	
The response error code.
AddOffsetsToTxn API (Key: 25):
Requests:

AddOffsetsToTxn Request (Version: 0) => transactional_id producer_id producer_epoch group_id 
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => STRING
Request header version: 1

Field	
Description
transactional_id	
The transactional id corresponding to the transaction.
producer_id	
Current producer id in use by the transactional id.
producer_epoch	
Current epoch associated with the producer id.
group_id	
The unique group identifier.
AddOffsetsToTxn Request (Version: 1) => transactional_id producer_id producer_epoch group_id 
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => STRING
Request header version: 1

Field	
Description
transactional_id	
The transactional id corresponding to the transaction.
producer_id	
Current producer id in use by the transactional id.
producer_epoch	
Current epoch associated with the producer id.
group_id	
The unique group identifier.
AddOffsetsToTxn Request (Version: 2) => transactional_id producer_id producer_epoch group_id 
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => STRING
Request header version: 1

Field	
Description
transactional_id	
The transactional id corresponding to the transaction.
producer_id	
Current producer id in use by the transactional id.
producer_epoch	
Current epoch associated with the producer id.
group_id	
The unique group identifier.
AddOffsetsToTxn Request (Version: 3) => transactional_id producer_id producer_epoch group_id 
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => COMPACT_STRING
Request header version: 2

Field	
Description
transactional_id	
The transactional id corresponding to the transaction.
producer_id	
Current producer id in use by the transactional id.
producer_epoch	
Current epoch associated with the producer id.
group_id	
The unique group identifier.
AddOffsetsToTxn Request (Version: 4) => transactional_id producer_id producer_epoch group_id 
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => COMPACT_STRING
Request header version: 2

Field	
Description
transactional_id	
The transactional id corresponding to the transaction.
producer_id	
Current producer id in use by the transactional id.
producer_epoch	
Current epoch associated with the producer id.
group_id	
The unique group identifier.
Responses:

AddOffsetsToTxn Response (Version: 0) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The response error code, or 0 if there was no error.
AddOffsetsToTxn Response (Version: 1) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The response error code, or 0 if there was no error.
AddOffsetsToTxn Response (Version: 2) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The response error code, or 0 if there was no error.
AddOffsetsToTxn Response (Version: 3) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The response error code, or 0 if there was no error.
AddOffsetsToTxn Response (Version: 4) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The response error code, or 0 if there was no error.
EndTxn API (Key: 26):
Requests:

EndTxn Request (Version: 0) => transactional_id producer_id producer_epoch committed 
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN
Request header version: 1

Field	
Description
transactional_id	
The ID of the transaction to end.
producer_id	
The producer ID.
producer_epoch	
The current epoch associated with the producer.
committed	
True if the transaction was committed, false if it was aborted.
EndTxn Request (Version: 1) => transactional_id producer_id producer_epoch committed 
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN
Request header version: 1

Field	
Description
transactional_id	
The ID of the transaction to end.
producer_id	
The producer ID.
producer_epoch	
The current epoch associated with the producer.
committed	
True if the transaction was committed, false if it was aborted.
EndTxn Request (Version: 2) => transactional_id producer_id producer_epoch committed 
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN
Request header version: 1

Field	
Description
transactional_id	
The ID of the transaction to end.
producer_id	
The producer ID.
producer_epoch	
The current epoch associated with the producer.
committed	
True if the transaction was committed, false if it was aborted.
EndTxn Request (Version: 3) => transactional_id producer_id producer_epoch committed 
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN
Request header version: 2

Field	
Description
transactional_id	
The ID of the transaction to end.
producer_id	
The producer ID.
producer_epoch	
The current epoch associated with the producer.
committed	
True if the transaction was committed, false if it was aborted.
EndTxn Request (Version: 4) => transactional_id producer_id producer_epoch committed 
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN
Request header version: 2

Field	
Description
transactional_id	
The ID of the transaction to end.
producer_id	
The producer ID.
producer_epoch	
The current epoch associated with the producer.
committed	
True if the transaction was committed, false if it was aborted.
EndTxn Request (Version: 5) => transactional_id producer_id producer_epoch committed 
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN
Request header version: 2

Field	
Description
transactional_id	
The ID of the transaction to end.
producer_id	
The producer ID.
producer_epoch	
The current epoch associated with the producer.
committed	
True if the transaction was committed, false if it was aborted.
Responses:

EndTxn Response (Version: 0) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
EndTxn Response (Version: 1) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
EndTxn Response (Version: 2) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
EndTxn Response (Version: 3) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
EndTxn Response (Version: 4) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
EndTxn Response (Version: 5) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
producer_id	
The producer ID.
producer_epoch	
The current epoch associated with the producer.
WriteTxnMarkers API (Key: 27):
Requests:

WriteTxnMarkers Request (Version: 1) => [markers] 
  markers => producer_id producer_epoch transaction_result [topics] coordinator_epoch 
    producer_id => INT64
    producer_epoch => INT16
    transaction_result => BOOLEAN
    topics => name [partition_indexes] 
      name => COMPACT_STRING
      partition_indexes => INT32
    coordinator_epoch => INT32
Request header version: 2

Field	
Description
markers	
The transaction markers to be written.
producer_id	
The current producer ID.
producer_epoch	
The current epoch associated with the producer ID.
transaction_result	
The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
topics	
Each topic that we want to write transaction marker(s) for.
name	
The topic name.
partition_indexes	
The indexes of the partitions to write transaction markers for.
coordinator_epoch	
Epoch associated with the transaction state partition hosted by this transaction coordinator.
WriteTxnMarkers Request (Version: 2) => [markers] 
  markers => producer_id producer_epoch transaction_result [topics] coordinator_epoch transaction_version 
    producer_id => INT64
    producer_epoch => INT16
    transaction_result => BOOLEAN
    topics => name [partition_indexes] 
      name => COMPACT_STRING
      partition_indexes => INT32
    coordinator_epoch => INT32
    transaction_version => INT8
Request header version: 2

Field	
Description
markers	
The transaction markers to be written.
producer_id	
The current producer ID.
producer_epoch	
The current epoch associated with the producer ID.
transaction_result	
The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
topics	
Each topic that we want to write transaction marker(s) for.
name	
The topic name.
partition_indexes	
The indexes of the partitions to write transaction markers for.
coordinator_epoch	
Epoch associated with the transaction state partition hosted by this transaction coordinator.
transaction_version	
Transaction version of the marker. Ex: 0/1 = legacy (TV0/TV1), 2 = TV2 etc.
Responses:

WriteTxnMarkers Response (Version: 1) => [markers] 
  markers => producer_id [topics] 
    producer_id => INT64
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => partition_index error_code 
        partition_index => INT32
        error_code => INT16
Response header version: 1

Field	
Description
markers	
The results for writing makers.
producer_id	
The current producer ID in use by the transactional ID.
topics	
The results by topic.
name	
The topic name.
partitions	
The results by partition.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
WriteTxnMarkers Response (Version: 2) => [markers] 
  markers => producer_id [topics] 
    producer_id => INT64
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => partition_index error_code 
        partition_index => INT32
        error_code => INT16
Response header version: 1

Field	
Description
markers	
The results for writing makers.
producer_id	
The current producer ID in use by the transactional ID.
topics	
The results by topic.
name	
The topic name.
partitions	
The results by partition.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
TxnOffsetCommit API (Key: 28):
Requests:

TxnOffsetCommit Request (Version: 0) => transactional_id group_id producer_id producer_epoch [topics] 
  transactional_id => STRING
  group_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
transactional_id	
The ID of the transaction.
group_id	
The ID of the group.
producer_id	
The current producer ID in use by the transactional ID.
producer_epoch	
The current epoch associated with the producer ID.
topics	
Each topic that we want to commit offsets for.
name	
The topic name.
partitions	
The partitions inside the topic that we want to commit offsets for.
partition_index	
The index of the partition within the topic.
committed_offset	
The message offset to be committed.
committed_metadata	
Any associated metadata the client wants to keep.
TxnOffsetCommit Request (Version: 1) => transactional_id group_id producer_id producer_epoch [topics] 
  transactional_id => STRING
  group_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
transactional_id	
The ID of the transaction.
group_id	
The ID of the group.
producer_id	
The current producer ID in use by the transactional ID.
producer_epoch	
The current epoch associated with the producer ID.
topics	
Each topic that we want to commit offsets for.
name	
The topic name.
partitions	
The partitions inside the topic that we want to commit offsets for.
partition_index	
The index of the partition within the topic.
committed_offset	
The message offset to be committed.
committed_metadata	
Any associated metadata the client wants to keep.
TxnOffsetCommit Request (Version: 2) => transactional_id group_id producer_id producer_epoch [topics] 
  transactional_id => STRING
  group_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => NULLABLE_STRING
Request header version: 1

Field	
Description
transactional_id	
The ID of the transaction.
group_id	
The ID of the group.
producer_id	
The current producer ID in use by the transactional ID.
producer_epoch	
The current epoch associated with the producer ID.
topics	
Each topic that we want to commit offsets for.
name	
The topic name.
partitions	
The partitions inside the topic that we want to commit offsets for.
partition_index	
The index of the partition within the topic.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of the last consumed record.
committed_metadata	
Any associated metadata the client wants to keep.
TxnOffsetCommit Request (Version: 3) => transactional_id group_id producer_id producer_epoch generation_id member_id group_instance_id [topics] 
  transactional_id => COMPACT_STRING
  group_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
transactional_id	
The ID of the transaction.
group_id	
The ID of the group.
producer_id	
The current producer ID in use by the transactional ID.
producer_epoch	
The current epoch associated with the producer ID.
generation_id	
The generation of the consumer.
member_id	
The member ID assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
topics	
Each topic that we want to commit offsets for.
name	
The topic name.
partitions	
The partitions inside the topic that we want to commit offsets for.
partition_index	
The index of the partition within the topic.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of the last consumed record.
committed_metadata	
Any associated metadata the client wants to keep.
TxnOffsetCommit Request (Version: 4) => transactional_id group_id producer_id producer_epoch generation_id member_id group_instance_id [topics] 
  transactional_id => COMPACT_STRING
  group_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
transactional_id	
The ID of the transaction.
group_id	
The ID of the group.
producer_id	
The current producer ID in use by the transactional ID.
producer_epoch	
The current epoch associated with the producer ID.
generation_id	
The generation of the consumer.
member_id	
The member ID assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
topics	
Each topic that we want to commit offsets for.
name	
The topic name.
partitions	
The partitions inside the topic that we want to commit offsets for.
partition_index	
The index of the partition within the topic.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of the last consumed record.
committed_metadata	
Any associated metadata the client wants to keep.
TxnOffsetCommit Request (Version: 5) => transactional_id group_id producer_id producer_epoch generation_id member_id group_instance_id [topics] 
  transactional_id => COMPACT_STRING
  group_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
transactional_id	
The ID of the transaction.
group_id	
The ID of the group.
producer_id	
The current producer ID in use by the transactional ID.
producer_epoch	
The current epoch associated with the producer ID.
generation_id	
The generation of the consumer.
member_id	
The member ID assigned by the group coordinator.
group_instance_id	
The unique identifier of the consumer instance provided by end user.
topics	
Each topic that we want to commit offsets for.
name	
The topic name.
partitions	
The partitions inside the topic that we want to commit offsets for.
partition_index	
The index of the partition within the topic.
committed_offset	
The message offset to be committed.
committed_leader_epoch	
The leader epoch of the last consumed record.
committed_metadata	
Any associated metadata the client wants to keep.
Responses:

TxnOffsetCommit Response (Version: 0) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
TxnOffsetCommit Response (Version: 1) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
TxnOffsetCommit Response (Version: 2) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
TxnOffsetCommit Response (Version: 3) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
TxnOffsetCommit Response (Version: 4) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
TxnOffsetCommit Response (Version: 5) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
DescribeAcls API (Key: 29):
Requests:

DescribeAcls Request (Version: 1) => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type 
  resource_type_filter => INT8
  resource_name_filter => NULLABLE_STRING
  pattern_type_filter => INT8
  principal_filter => NULLABLE_STRING
  host_filter => NULLABLE_STRING
  operation => INT8
  permission_type => INT8
Request header version: 1

Field	
Description
resource_type_filter	
The resource type.
resource_name_filter	
The resource name, or null to match any resource name.
pattern_type_filter	
The resource pattern to match.
principal_filter	
The principal to match, or null to match any principal.
host_filter	
The host to match, or null to match any host.
operation	
The operation to match.
permission_type	
The permission type to match.
DescribeAcls Request (Version: 2) => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type 
  resource_type_filter => INT8
  resource_name_filter => COMPACT_NULLABLE_STRING
  pattern_type_filter => INT8
  principal_filter => COMPACT_NULLABLE_STRING
  host_filter => COMPACT_NULLABLE_STRING
  operation => INT8
  permission_type => INT8
Request header version: 2

Field	
Description
resource_type_filter	
The resource type.
resource_name_filter	
The resource name, or null to match any resource name.
pattern_type_filter	
The resource pattern to match.
principal_filter	
The principal to match, or null to match any principal.
host_filter	
The host to match, or null to match any host.
operation	
The operation to match.
permission_type	
The permission type to match.
DescribeAcls Request (Version: 3) => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type 
  resource_type_filter => INT8
  resource_name_filter => COMPACT_NULLABLE_STRING
  pattern_type_filter => INT8
  principal_filter => COMPACT_NULLABLE_STRING
  host_filter => COMPACT_NULLABLE_STRING
  operation => INT8
  permission_type => INT8
Request header version: 2

Field	
Description
resource_type_filter	
The resource type.
resource_name_filter	
The resource name, or null to match any resource name.
pattern_type_filter	
The resource pattern to match.
principal_filter	
The principal to match, or null to match any principal.
host_filter	
The host to match, or null to match any host.
operation	
The operation to match.
permission_type	
The permission type to match.
Responses:

DescribeAcls Response (Version: 1) => throttle_time_ms error_code error_message [resources] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  resources => resource_type resource_name pattern_type [acls] 
    resource_type => INT8
    resource_name => STRING
    pattern_type => INT8
    acls => principal host operation permission_type 
      principal => STRING
      host => STRING
      operation => INT8
      permission_type => INT8
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
resources	
Each Resource that is referenced in an ACL.
resource_type	
The resource type.
resource_name	
The resource name.
pattern_type	
The resource pattern type.
acls	
The ACLs.
principal	
The ACL principal.
host	
The ACL host.
operation	
The ACL operation.
permission_type	
The ACL permission type.
DescribeAcls Response (Version: 2) => throttle_time_ms error_code error_message [resources] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  resources => resource_type resource_name pattern_type [acls] 
    resource_type => INT8
    resource_name => COMPACT_STRING
    pattern_type => INT8
    acls => principal host operation permission_type 
      principal => COMPACT_STRING
      host => COMPACT_STRING
      operation => INT8
      permission_type => INT8
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
resources	
Each Resource that is referenced in an ACL.
resource_type	
The resource type.
resource_name	
The resource name.
pattern_type	
The resource pattern type.
acls	
The ACLs.
principal	
The ACL principal.
host	
The ACL host.
operation	
The ACL operation.
permission_type	
The ACL permission type.
DescribeAcls Response (Version: 3) => throttle_time_ms error_code error_message [resources] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  resources => resource_type resource_name pattern_type [acls] 
    resource_type => INT8
    resource_name => COMPACT_STRING
    pattern_type => INT8
    acls => principal host operation permission_type 
      principal => COMPACT_STRING
      host => COMPACT_STRING
      operation => INT8
      permission_type => INT8
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
resources	
Each Resource that is referenced in an ACL.
resource_type	
The resource type.
resource_name	
The resource name.
pattern_type	
The resource pattern type.
acls	
The ACLs.
principal	
The ACL principal.
host	
The ACL host.
operation	
The ACL operation.
permission_type	
The ACL permission type.
CreateAcls API (Key: 30):
Requests:

CreateAcls Request (Version: 1) => [creations] 
  creations => resource_type resource_name resource_pattern_type principal host operation permission_type 
    resource_type => INT8
    resource_name => STRING
    resource_pattern_type => INT8
    principal => STRING
    host => STRING
    operation => INT8
    permission_type => INT8
Request header version: 1

Field	
Description
creations	
The ACLs that we want to create.
resource_type	
The type of the resource.
resource_name	
The resource name for the ACL.
resource_pattern_type	
The pattern type for the ACL.
principal	
The principal for the ACL.
host	
The host for the ACL.
operation	
The operation type for the ACL (read, write, etc.).
permission_type	
The permission type for the ACL (allow, deny, etc.).
CreateAcls Request (Version: 2) => [creations] 
  creations => resource_type resource_name resource_pattern_type principal host operation permission_type 
    resource_type => INT8
    resource_name => COMPACT_STRING
    resource_pattern_type => INT8
    principal => COMPACT_STRING
    host => COMPACT_STRING
    operation => INT8
    permission_type => INT8
Request header version: 2

Field	
Description
creations	
The ACLs that we want to create.
resource_type	
The type of the resource.
resource_name	
The resource name for the ACL.
resource_pattern_type	
The pattern type for the ACL.
principal	
The principal for the ACL.
host	
The host for the ACL.
operation	
The operation type for the ACL (read, write, etc.).
permission_type	
The permission type for the ACL (allow, deny, etc.).
CreateAcls Request (Version: 3) => [creations] 
  creations => resource_type resource_name resource_pattern_type principal host operation permission_type 
    resource_type => INT8
    resource_name => COMPACT_STRING
    resource_pattern_type => INT8
    principal => COMPACT_STRING
    host => COMPACT_STRING
    operation => INT8
    permission_type => INT8
Request header version: 2

Field	
Description
creations	
The ACLs that we want to create.
resource_type	
The type of the resource.
resource_name	
The resource name for the ACL.
resource_pattern_type	
The pattern type for the ACL.
principal	
The principal for the ACL.
host	
The host for the ACL.
operation	
The operation type for the ACL (read, write, etc.).
permission_type	
The permission type for the ACL (allow, deny, etc.).
Responses:

CreateAcls Response (Version: 1) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code error_message 
    error_code => INT16
    error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each ACL creation.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
CreateAcls Response (Version: 2) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code error_message 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each ACL creation.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
CreateAcls Response (Version: 3) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code error_message 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each ACL creation.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
DeleteAcls API (Key: 31):
Requests:

DeleteAcls Request (Version: 1) => [filters] 
  filters => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type 
    resource_type_filter => INT8
    resource_name_filter => NULLABLE_STRING
    pattern_type_filter => INT8
    principal_filter => NULLABLE_STRING
    host_filter => NULLABLE_STRING
    operation => INT8
    permission_type => INT8
Request header version: 1

Field	
Description
filters	
The filters to use when deleting ACLs.
resource_type_filter	
The resource type.
resource_name_filter	
The resource name, or null to match any resource name.
pattern_type_filter	
The pattern type.
principal_filter	
The principal filter, or null to accept all principals.
host_filter	
The host filter, or null to accept all hosts.
operation	
The ACL operation.
permission_type	
The permission type.
DeleteAcls Request (Version: 2) => [filters] 
  filters => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type 
    resource_type_filter => INT8
    resource_name_filter => COMPACT_NULLABLE_STRING
    pattern_type_filter => INT8
    principal_filter => COMPACT_NULLABLE_STRING
    host_filter => COMPACT_NULLABLE_STRING
    operation => INT8
    permission_type => INT8
Request header version: 2

Field	
Description
filters	
The filters to use when deleting ACLs.
resource_type_filter	
The resource type.
resource_name_filter	
The resource name, or null to match any resource name.
pattern_type_filter	
The pattern type.
principal_filter	
The principal filter, or null to accept all principals.
host_filter	
The host filter, or null to accept all hosts.
operation	
The ACL operation.
permission_type	
The permission type.
DeleteAcls Request (Version: 3) => [filters] 
  filters => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type 
    resource_type_filter => INT8
    resource_name_filter => COMPACT_NULLABLE_STRING
    pattern_type_filter => INT8
    principal_filter => COMPACT_NULLABLE_STRING
    host_filter => COMPACT_NULLABLE_STRING
    operation => INT8
    permission_type => INT8
Request header version: 2

Field	
Description
filters	
The filters to use when deleting ACLs.
resource_type_filter	
The resource type.
resource_name_filter	
The resource name, or null to match any resource name.
pattern_type_filter	
The pattern type.
principal_filter	
The principal filter, or null to accept all principals.
host_filter	
The host filter, or null to accept all hosts.
operation	
The ACL operation.
permission_type	
The permission type.
Responses:

DeleteAcls Response (Version: 1) => throttle_time_ms [filter_results] 
  throttle_time_ms => INT32
  filter_results => error_code error_message [matching_acls] 
    error_code => INT16
    error_message => NULLABLE_STRING
    matching_acls => error_code error_message resource_type resource_name pattern_type principal host operation permission_type 
      error_code => INT16
      error_message => NULLABLE_STRING
      resource_type => INT8
      resource_name => STRING
      pattern_type => INT8
      principal => STRING
      host => STRING
      operation => INT8
      permission_type => INT8
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
filter_results	
The results for each filter.
error_code	
The error code, or 0 if the filter succeeded.
error_message	
The error message, or null if the filter succeeded.
matching_acls	
The ACLs which matched this filter.
error_code	
The deletion error code, or 0 if the deletion succeeded.
error_message	
The deletion error message, or null if the deletion succeeded.
resource_type	
The ACL resource type.
resource_name	
The ACL resource name.
pattern_type	
The ACL resource pattern type.
principal	
The ACL principal.
host	
The ACL host.
operation	
The ACL operation.
permission_type	
The ACL permission type.
DeleteAcls Response (Version: 2) => throttle_time_ms [filter_results] 
  throttle_time_ms => INT32
  filter_results => error_code error_message [matching_acls] 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    matching_acls => error_code error_message resource_type resource_name pattern_type principal host operation permission_type 
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      resource_type => INT8
      resource_name => COMPACT_STRING
      pattern_type => INT8
      principal => COMPACT_STRING
      host => COMPACT_STRING
      operation => INT8
      permission_type => INT8
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
filter_results	
The results for each filter.
error_code	
The error code, or 0 if the filter succeeded.
error_message	
The error message, or null if the filter succeeded.
matching_acls	
The ACLs which matched this filter.
error_code	
The deletion error code, or 0 if the deletion succeeded.
error_message	
The deletion error message, or null if the deletion succeeded.
resource_type	
The ACL resource type.
resource_name	
The ACL resource name.
pattern_type	
The ACL resource pattern type.
principal	
The ACL principal.
host	
The ACL host.
operation	
The ACL operation.
permission_type	
The ACL permission type.
DeleteAcls Response (Version: 3) => throttle_time_ms [filter_results] 
  throttle_time_ms => INT32
  filter_results => error_code error_message [matching_acls] 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    matching_acls => error_code error_message resource_type resource_name pattern_type principal host operation permission_type 
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      resource_type => INT8
      resource_name => COMPACT_STRING
      pattern_type => INT8
      principal => COMPACT_STRING
      host => COMPACT_STRING
      operation => INT8
      permission_type => INT8
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
filter_results	
The results for each filter.
error_code	
The error code, or 0 if the filter succeeded.
error_message	
The error message, or null if the filter succeeded.
matching_acls	
The ACLs which matched this filter.
error_code	
The deletion error code, or 0 if the deletion succeeded.
error_message	
The deletion error message, or null if the deletion succeeded.
resource_type	
The ACL resource type.
resource_name	
The ACL resource name.
pattern_type	
The ACL resource pattern type.
principal	
The ACL principal.
host	
The ACL host.
operation	
The ACL operation.
permission_type	
The ACL permission type.
DescribeConfigs API (Key: 32):
Requests:

DescribeConfigs Request (Version: 1) => [resources] include_synonyms 
  resources => resource_type resource_name [configuration_keys] 
    resource_type => INT8
    resource_name => STRING
    configuration_keys => STRING
  include_synonyms => BOOLEAN
Request header version: 1

Field	
Description
resources	
The resources whose configurations we want to describe.
resource_type	
The resource type.
resource_name	
The resource name.
configuration_keys	
The configuration keys to list, or null to list all configuration keys.
include_synonyms	
True if we should include all synonyms.
DescribeConfigs Request (Version: 2) => [resources] include_synonyms 
  resources => resource_type resource_name [configuration_keys] 
    resource_type => INT8
    resource_name => STRING
    configuration_keys => STRING
  include_synonyms => BOOLEAN
Request header version: 1

Field	
Description
resources	
The resources whose configurations we want to describe.
resource_type	
The resource type.
resource_name	
The resource name.
configuration_keys	
The configuration keys to list, or null to list all configuration keys.
include_synonyms	
True if we should include all synonyms.
DescribeConfigs Request (Version: 3) => [resources] include_synonyms include_documentation 
  resources => resource_type resource_name [configuration_keys] 
    resource_type => INT8
    resource_name => STRING
    configuration_keys => STRING
  include_synonyms => BOOLEAN
  include_documentation => BOOLEAN
Request header version: 1

Field	
Description
resources	
The resources whose configurations we want to describe.
resource_type	
The resource type.
resource_name	
The resource name.
configuration_keys	
The configuration keys to list, or null to list all configuration keys.
include_synonyms	
True if we should include all synonyms.
include_documentation	
True if we should include configuration documentation.
DescribeConfigs Request (Version: 4) => [resources] include_synonyms include_documentation 
  resources => resource_type resource_name [configuration_keys] 
    resource_type => INT8
    resource_name => COMPACT_STRING
    configuration_keys => COMPACT_STRING
  include_synonyms => BOOLEAN
  include_documentation => BOOLEAN
Request header version: 2

Field	
Description
resources	
The resources whose configurations we want to describe.
resource_type	
The resource type.
resource_name	
The resource name.
configuration_keys	
The configuration keys to list, or null to list all configuration keys.
include_synonyms	
True if we should include all synonyms.
include_documentation	
True if we should include configuration documentation.
Responses:

DescribeConfigs Response (Version: 1) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code error_message resource_type resource_name [configs] 
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
    configs => name value read_only config_source is_sensitive [synonyms] 
      name => STRING
      value => NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
      synonyms => name value source 
        name => STRING
        value => NULLABLE_STRING
        source => INT8
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each resource.
error_code	
The error code, or 0 if we were able to successfully describe the configurations.
error_message	
The error message, or null if we were able to successfully describe the configurations.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
Each listed configuration.
name	
The configuration name.
value	
The configuration value.
read_only	
True if the configuration is read-only.
config_source	
The configuration source.
is_sensitive	
True if this configuration is sensitive.
synonyms	
The synonyms for this configuration key.
name	
The synonym name.
value	
The synonym value.
source	
The synonym source.
DescribeConfigs Response (Version: 2) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code error_message resource_type resource_name [configs] 
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
    configs => name value read_only config_source is_sensitive [synonyms] 
      name => STRING
      value => NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
      synonyms => name value source 
        name => STRING
        value => NULLABLE_STRING
        source => INT8
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each resource.
error_code	
The error code, or 0 if we were able to successfully describe the configurations.
error_message	
The error message, or null if we were able to successfully describe the configurations.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
Each listed configuration.
name	
The configuration name.
value	
The configuration value.
read_only	
True if the configuration is read-only.
config_source	
The configuration source.
is_sensitive	
True if this configuration is sensitive.
synonyms	
The synonyms for this configuration key.
name	
The synonym name.
value	
The synonym value.
source	
The synonym source.
DescribeConfigs Response (Version: 3) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code error_message resource_type resource_name [configs] 
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
    configs => name value read_only config_source is_sensitive [synonyms] config_type documentation 
      name => STRING
      value => NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
      synonyms => name value source 
        name => STRING
        value => NULLABLE_STRING
        source => INT8
      config_type => INT8
      documentation => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each resource.
error_code	
The error code, or 0 if we were able to successfully describe the configurations.
error_message	
The error message, or null if we were able to successfully describe the configurations.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
Each listed configuration.
name	
The configuration name.
value	
The configuration value.
read_only	
True if the configuration is read-only.
config_source	
The configuration source.
is_sensitive	
True if this configuration is sensitive.
synonyms	
The synonyms for this configuration key.
name	
The synonym name.
value	
The synonym value.
source	
The synonym source.
config_type	
The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
documentation	
The configuration documentation.
DescribeConfigs Response (Version: 4) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code error_message resource_type resource_name [configs] 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    resource_type => INT8
    resource_name => COMPACT_STRING
    configs => name value read_only config_source is_sensitive [synonyms] config_type documentation 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
      synonyms => name value source 
        name => COMPACT_STRING
        value => COMPACT_NULLABLE_STRING
        source => INT8
      config_type => INT8
      documentation => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each resource.
error_code	
The error code, or 0 if we were able to successfully describe the configurations.
error_message	
The error message, or null if we were able to successfully describe the configurations.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
Each listed configuration.
name	
The configuration name.
value	
The configuration value.
read_only	
True if the configuration is read-only.
config_source	
The configuration source.
is_sensitive	
True if this configuration is sensitive.
synonyms	
The synonyms for this configuration key.
name	
The synonym name.
value	
The synonym value.
source	
The synonym source.
config_type	
The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
documentation	
The configuration documentation.
AlterConfigs API (Key: 33):
Requests:

AlterConfigs Request (Version: 0) => [resources] validate_only 
  resources => resource_type resource_name [configs] 
    resource_type => INT8
    resource_name => STRING
    configs => name value 
      name => STRING
      value => NULLABLE_STRING
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
resources	
The updates for each resource.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
The configurations.
name	
The configuration key name.
value	
The value to set for the configuration key.
validate_only	
True if we should validate the request, but not change the configurations.
AlterConfigs Request (Version: 1) => [resources] validate_only 
  resources => resource_type resource_name [configs] 
    resource_type => INT8
    resource_name => STRING
    configs => name value 
      name => STRING
      value => NULLABLE_STRING
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
resources	
The updates for each resource.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
The configurations.
name	
The configuration key name.
value	
The value to set for the configuration key.
validate_only	
True if we should validate the request, but not change the configurations.
AlterConfigs Request (Version: 2) => [resources] validate_only 
  resources => resource_type resource_name [configs] 
    resource_type => INT8
    resource_name => COMPACT_STRING
    configs => name value 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
resources	
The updates for each resource.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
The configurations.
name	
The configuration key name.
value	
The value to set for the configuration key.
validate_only	
True if we should validate the request, but not change the configurations.
Responses:

AlterConfigs Response (Version: 0) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => error_code error_message resource_type resource_name 
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The responses for each resource.
error_code	
The resource error code.
error_message	
The resource error message, or null if there was no error.
resource_type	
The resource type.
resource_name	
The resource name.
AlterConfigs Response (Version: 1) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => error_code error_message resource_type resource_name 
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The responses for each resource.
error_code	
The resource error code.
error_message	
The resource error message, or null if there was no error.
resource_type	
The resource type.
resource_name	
The resource name.
AlterConfigs Response (Version: 2) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => error_code error_message resource_type resource_name 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    resource_type => INT8
    resource_name => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The responses for each resource.
error_code	
The resource error code.
error_message	
The resource error message, or null if there was no error.
resource_type	
The resource type.
resource_name	
The resource name.
AlterReplicaLogDirs API (Key: 34):
Requests:

AlterReplicaLogDirs Request (Version: 1) => [dirs] 
  dirs => path [topics] 
    path => STRING
    topics => name [partitions] 
      name => STRING
      partitions => INT32
Request header version: 1

Field	
Description
dirs	
The alterations to make for each directory.
path	
The absolute directory path.
topics	
The topics to add to the directory.
name	
The topic name.
partitions	
The partition indexes.
AlterReplicaLogDirs Request (Version: 2) => [dirs] 
  dirs => path [topics] 
    path => COMPACT_STRING
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => INT32
Request header version: 2

Field	
Description
dirs	
The alterations to make for each directory.
path	
The absolute directory path.
topics	
The topics to add to the directory.
name	
The topic name.
partitions	
The partition indexes.
Responses:

AlterReplicaLogDirs Response (Version: 1) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => topic_name [partitions] 
    topic_name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each topic.
topic_name	
The name of the topic.
partitions	
The results for each partition.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
AlterReplicaLogDirs Response (Version: 2) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for each topic.
topic_name	
The name of the topic.
partitions	
The results for each partition.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
DescribeLogDirs API (Key: 35):
Requests:

DescribeLogDirs Request (Version: 1) => [topics] 
  topics => topic [partitions] 
    topic => STRING
    partitions => INT32
Request header version: 1

Field	
Description
topics	
Each topic that we want to describe log directories for, or null for all topics.
topic	
The topic name.
partitions	
The partition indexes.
DescribeLogDirs Request (Version: 2) => [topics] 
  topics => topic [partitions] 
    topic => COMPACT_STRING
    partitions => INT32
Request header version: 2

Field	
Description
topics	
Each topic that we want to describe log directories for, or null for all topics.
topic	
The topic name.
partitions	
The partition indexes.
DescribeLogDirs Request (Version: 3) => [topics] 
  topics => topic [partitions] 
    topic => COMPACT_STRING
    partitions => INT32
Request header version: 2

Field	
Description
topics	
Each topic that we want to describe log directories for, or null for all topics.
topic	
The topic name.
partitions	
The partition indexes.
DescribeLogDirs Request (Version: 4) => [topics] 
  topics => topic [partitions] 
    topic => COMPACT_STRING
    partitions => INT32
Request header version: 2

Field	
Description
topics	
Each topic that we want to describe log directories for, or null for all topics.
topic	
The topic name.
partitions	
The partition indexes.
Responses:

DescribeLogDirs Response (Version: 1) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code log_dir [topics] 
    error_code => INT16
    log_dir => STRING
    topics => name [partitions] 
      name => STRING
      partitions => partition_index partition_size offset_lag is_future_key 
        partition_index => INT32
        partition_size => INT64
        offset_lag => INT64
        is_future_key => BOOLEAN
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The log directories.
error_code	
The error code, or 0 if there was no error.
log_dir	
The absolute log directory path.
topics	
The topics.
name	
The topic name.
partitions	
The partitions.
partition_index	
The partition index.
partition_size	
The size of the log segments in this partition in bytes.
offset_lag	
The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
is_future_key	
True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
DescribeLogDirs Response (Version: 2) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => error_code log_dir [topics] 
    error_code => INT16
    log_dir => COMPACT_STRING
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => partition_index partition_size offset_lag is_future_key 
        partition_index => INT32
        partition_size => INT64
        offset_lag => INT64
        is_future_key => BOOLEAN
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The log directories.
error_code	
The error code, or 0 if there was no error.
log_dir	
The absolute log directory path.
topics	
The topics.
name	
The topic name.
partitions	
The partitions.
partition_index	
The partition index.
partition_size	
The size of the log segments in this partition in bytes.
offset_lag	
The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
is_future_key	
True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
DescribeLogDirs Response (Version: 3) => throttle_time_ms error_code [results] 
  throttle_time_ms => INT32
  error_code => INT16
  results => error_code log_dir [topics] 
    error_code => INT16
    log_dir => COMPACT_STRING
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => partition_index partition_size offset_lag is_future_key 
        partition_index => INT32
        partition_size => INT64
        offset_lag => INT64
        is_future_key => BOOLEAN
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
results	
The log directories.
error_code	
The error code, or 0 if there was no error.
log_dir	
The absolute log directory path.
topics	
The topics.
name	
The topic name.
partitions	
The partitions.
partition_index	
The partition index.
partition_size	
The size of the log segments in this partition in bytes.
offset_lag	
The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
is_future_key	
True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
DescribeLogDirs Response (Version: 4) => throttle_time_ms error_code [results] 
  throttle_time_ms => INT32
  error_code => INT16
  results => error_code log_dir [topics] total_bytes usable_bytes 
    error_code => INT16
    log_dir => COMPACT_STRING
    topics => name [partitions] 
      name => COMPACT_STRING
      partitions => partition_index partition_size offset_lag is_future_key 
        partition_index => INT32
        partition_size => INT64
        offset_lag => INT64
        is_future_key => BOOLEAN
    total_bytes => INT64
    usable_bytes => INT64
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
results	
The log directories.
error_code	
The error code, or 0 if there was no error.
log_dir	
The absolute log directory path.
topics	
The topics.
name	
The topic name.
partitions	
The partitions.
partition_index	
The partition index.
partition_size	
The size of the log segments in this partition in bytes.
offset_lag	
The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
is_future_key	
True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
total_bytes	
The total size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
usable_bytes	
The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
SaslAuthenticate API (Key: 36):
Requests:

SaslAuthenticate Request (Version: 0) => auth_bytes 
  auth_bytes => BYTES
Request header version: 1

Field	
Description
auth_bytes	
The SASL authentication bytes from the client, as defined by the SASL mechanism.
SaslAuthenticate Request (Version: 1) => auth_bytes 
  auth_bytes => BYTES
Request header version: 1

Field	
Description
auth_bytes	
The SASL authentication bytes from the client, as defined by the SASL mechanism.
SaslAuthenticate Request (Version: 2) => auth_bytes 
  auth_bytes => COMPACT_BYTES
Request header version: 2

Field	
Description
auth_bytes	
The SASL authentication bytes from the client, as defined by the SASL mechanism.
Responses:

SaslAuthenticate Response (Version: 0) => error_code error_message auth_bytes 
  error_code => INT16
  error_message => NULLABLE_STRING
  auth_bytes => BYTES
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
auth_bytes	
The SASL authentication bytes from the server, as defined by the SASL mechanism.
SaslAuthenticate Response (Version: 1) => error_code error_message auth_bytes session_lifetime_ms 
  error_code => INT16
  error_message => NULLABLE_STRING
  auth_bytes => BYTES
  session_lifetime_ms => INT64
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
auth_bytes	
The SASL authentication bytes from the server, as defined by the SASL mechanism.
session_lifetime_ms	
Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
SaslAuthenticate Response (Version: 2) => error_code error_message auth_bytes session_lifetime_ms 
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  auth_bytes => COMPACT_BYTES
  session_lifetime_ms => INT64
Response header version: 1

Field	
Description
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
auth_bytes	
The SASL authentication bytes from the server, as defined by the SASL mechanism.
session_lifetime_ms	
Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
CreatePartitions API (Key: 37):
Requests:

CreatePartitions Request (Version: 0) => [topics] timeout_ms validate_only 
  topics => name count [assignments] 
    name => STRING
    count => INT32
    assignments => [broker_ids] 
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
topics	
Each topic that we want to create new partitions inside.
name	
The topic name.
count	
The new partition count.
assignments	
The new partition assignments.
broker_ids	
The assigned broker IDs.
timeout_ms	
The time in ms to wait for the partitions to be created.
validate_only	
If true, then validate the request, but don't actually increase the number of partitions.
CreatePartitions Request (Version: 1) => [topics] timeout_ms validate_only 
  topics => name count [assignments] 
    name => STRING
    count => INT32
    assignments => [broker_ids] 
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
topics	
Each topic that we want to create new partitions inside.
name	
The topic name.
count	
The new partition count.
assignments	
The new partition assignments.
broker_ids	
The assigned broker IDs.
timeout_ms	
The time in ms to wait for the partitions to be created.
validate_only	
If true, then validate the request, but don't actually increase the number of partitions.
CreatePartitions Request (Version: 2) => [topics] timeout_ms validate_only 
  topics => name count [assignments] 
    name => COMPACT_STRING
    count => INT32
    assignments => [broker_ids] 
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
topics	
Each topic that we want to create new partitions inside.
name	
The topic name.
count	
The new partition count.
assignments	
The new partition assignments.
broker_ids	
The assigned broker IDs.
timeout_ms	
The time in ms to wait for the partitions to be created.
validate_only	
If true, then validate the request, but don't actually increase the number of partitions.
CreatePartitions Request (Version: 3) => [topics] timeout_ms validate_only 
  topics => name count [assignments] 
    name => COMPACT_STRING
    count => INT32
    assignments => [broker_ids] 
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
topics	
Each topic that we want to create new partitions inside.
name	
The topic name.
count	
The new partition count.
assignments	
The new partition assignments.
broker_ids	
The assigned broker IDs.
timeout_ms	
The time in ms to wait for the partitions to be created.
validate_only	
If true, then validate the request, but don't actually increase the number of partitions.
Responses:

CreatePartitions Response (Version: 0) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => name error_code error_message 
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The partition creation results for each topic.
name	
The topic name.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
CreatePartitions Response (Version: 1) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => name error_code error_message 
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The partition creation results for each topic.
name	
The topic name.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
CreatePartitions Response (Version: 2) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => name error_code error_message 
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The partition creation results for each topic.
name	
The topic name.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
CreatePartitions Response (Version: 3) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => name error_code error_message 
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The partition creation results for each topic.
name	
The topic name.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
CreateDelegationToken API (Key: 38):
Requests:

CreateDelegationToken Request (Version: 1) => [renewers] max_lifetime_ms 
  renewers => principal_type principal_name 
    principal_type => STRING
    principal_name => STRING
  max_lifetime_ms => INT64
Request header version: 1

Field	
Description
renewers	
A list of those who are allowed to renew this token before it expires.
principal_type	
The type of the Kafka principal.
principal_name	
The name of the Kafka principal.
max_lifetime_ms	
The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
CreateDelegationToken Request (Version: 2) => [renewers] max_lifetime_ms 
  renewers => principal_type principal_name 
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
  max_lifetime_ms => INT64
Request header version: 2

Field	
Description
renewers	
A list of those who are allowed to renew this token before it expires.
principal_type	
The type of the Kafka principal.
principal_name	
The name of the Kafka principal.
max_lifetime_ms	
The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
CreateDelegationToken Request (Version: 3) => owner_principal_type owner_principal_name [renewers] max_lifetime_ms 
  owner_principal_type => COMPACT_NULLABLE_STRING
  owner_principal_name => COMPACT_NULLABLE_STRING
  renewers => principal_type principal_name 
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
  max_lifetime_ms => INT64
Request header version: 2

Field	
Description
owner_principal_type	
The principal type of the owner of the token. If it's null it defaults to the token request principal.
owner_principal_name	
The principal name of the owner of the token. If it's null it defaults to the token request principal.
renewers	
A list of those who are allowed to renew this token before it expires.
principal_type	
The type of the Kafka principal.
principal_name	
The name of the Kafka principal.
max_lifetime_ms	
The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
Responses:

CreateDelegationToken Response (Version: 1) => error_code principal_type principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms 
  error_code => INT16
  principal_type => STRING
  principal_name => STRING
  issue_timestamp_ms => INT64
  expiry_timestamp_ms => INT64
  max_timestamp_ms => INT64
  token_id => STRING
  hmac => BYTES
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
error_code	
The top-level error, or zero if there was no error.
principal_type	
The principal type of the token owner.
principal_name	
The name of the token owner.
issue_timestamp_ms	
When this token was generated.
expiry_timestamp_ms	
When this token expires.
max_timestamp_ms	
The maximum lifetime of this token.
token_id	
The token UUID.
hmac	
HMAC of the delegation token.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
CreateDelegationToken Response (Version: 2) => error_code principal_type principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms 
  error_code => INT16
  principal_type => COMPACT_STRING
  principal_name => COMPACT_STRING
  issue_timestamp_ms => INT64
  expiry_timestamp_ms => INT64
  max_timestamp_ms => INT64
  token_id => COMPACT_STRING
  hmac => COMPACT_BYTES
  throttle_time_ms => INT32
Response header version: 1

Field	
Description
error_code	
The top-level error, or zero if there was no error.
principal_type	
The principal type of the token owner.
principal_name	
The name of the token owner.
issue_timestamp_ms	
When this token was generated.
expiry_timestamp_ms	
When this token expires.
max_timestamp_ms	
The maximum lifetime of this token.
token_id	
The token UUID.
hmac	
HMAC of the delegation token.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
CreateDelegationToken Response (Version: 3) => error_code principal_type principal_name token_requester_principal_type token_requester_principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms 
  error_code => INT16
  principal_type => COMPACT_STRING
  principal_name => COMPACT_STRING
  token_requester_principal_type => COMPACT_STRING
  token_requester_principal_name => COMPACT_STRING
  issue_timestamp_ms => INT64
  expiry_timestamp_ms => INT64
  max_timestamp_ms => INT64
  token_id => COMPACT_STRING
  hmac => COMPACT_BYTES
  throttle_time_ms => INT32
Response header version: 1

Field	
Description
error_code	
The top-level error, or zero if there was no error.
principal_type	
The principal type of the token owner.
principal_name	
The name of the token owner.
token_requester_principal_type	
The principal type of the requester of the token.
token_requester_principal_name	
The principal type of the requester of the token.
issue_timestamp_ms	
When this token was generated.
expiry_timestamp_ms	
When this token expires.
max_timestamp_ms	
The maximum lifetime of this token.
token_id	
The token UUID.
hmac	
HMAC of the delegation token.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
RenewDelegationToken API (Key: 39):
Requests:

RenewDelegationToken Request (Version: 1) => hmac renew_period_ms 
  hmac => BYTES
  renew_period_ms => INT64
Request header version: 1

Field	
Description
hmac	
The HMAC of the delegation token to be renewed.
renew_period_ms	
The renewal time period in milliseconds.
RenewDelegationToken Request (Version: 2) => hmac renew_period_ms 
  hmac => COMPACT_BYTES
  renew_period_ms => INT64
Request header version: 2

Field	
Description
hmac	
The HMAC of the delegation token to be renewed.
renew_period_ms	
The renewal time period in milliseconds.
Responses:

RenewDelegationToken Response (Version: 1) => error_code expiry_timestamp_ms throttle_time_ms 
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
expiry_timestamp_ms	
The timestamp in milliseconds at which this token expires.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
RenewDelegationToken Response (Version: 2) => error_code expiry_timestamp_ms throttle_time_ms 
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
Response header version: 1

Field	
Description
error_code	
The error code, or 0 if there was no error.
expiry_timestamp_ms	
The timestamp in milliseconds at which this token expires.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
ExpireDelegationToken API (Key: 40):
Requests:

ExpireDelegationToken Request (Version: 1) => hmac expiry_time_period_ms 
  hmac => BYTES
  expiry_time_period_ms => INT64
Request header version: 1

Field	
Description
hmac	
The HMAC of the delegation token to be expired.
expiry_time_period_ms	
The expiry time period in milliseconds.
ExpireDelegationToken Request (Version: 2) => hmac expiry_time_period_ms 
  hmac => COMPACT_BYTES
  expiry_time_period_ms => INT64
Request header version: 2

Field	
Description
hmac	
The HMAC of the delegation token to be expired.
expiry_time_period_ms	
The expiry time period in milliseconds.
Responses:

ExpireDelegationToken Response (Version: 1) => error_code expiry_timestamp_ms throttle_time_ms 
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
expiry_timestamp_ms	
The timestamp in milliseconds at which this token expires.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
ExpireDelegationToken Response (Version: 2) => error_code expiry_timestamp_ms throttle_time_ms 
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
Response header version: 1

Field	
Description
error_code	
The error code, or 0 if there was no error.
expiry_timestamp_ms	
The timestamp in milliseconds at which this token expires.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
DescribeDelegationToken API (Key: 41):
Requests:

DescribeDelegationToken Request (Version: 1) => [owners] 
  owners => principal_type principal_name 
    principal_type => STRING
    principal_name => STRING
Request header version: 1

Field	
Description
owners	
Each owner that we want to describe delegation tokens for, or null to describe all tokens.
principal_type	
The owner principal type.
principal_name	
The owner principal name.
DescribeDelegationToken Request (Version: 2) => [owners] 
  owners => principal_type principal_name 
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
Request header version: 2

Field	
Description
owners	
Each owner that we want to describe delegation tokens for, or null to describe all tokens.
principal_type	
The owner principal type.
principal_name	
The owner principal name.
DescribeDelegationToken Request (Version: 3) => [owners] 
  owners => principal_type principal_name 
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
Request header version: 2

Field	
Description
owners	
Each owner that we want to describe delegation tokens for, or null to describe all tokens.
principal_type	
The owner principal type.
principal_name	
The owner principal name.
Responses:

DescribeDelegationToken Response (Version: 1) => error_code [tokens] throttle_time_ms 
  error_code => INT16
  tokens => principal_type principal_name issue_timestamp expiry_timestamp max_timestamp token_id hmac [renewers] 
    principal_type => STRING
    principal_name => STRING
    issue_timestamp => INT64
    expiry_timestamp => INT64
    max_timestamp => INT64
    token_id => STRING
    hmac => BYTES
    renewers => principal_type principal_name 
      principal_type => STRING
      principal_name => STRING
  throttle_time_ms => INT32
Response header version: 0

Field	
Description
error_code	
The error code, or 0 if there was no error.
tokens	
The tokens.
principal_type	
The token principal type.
principal_name	
The token principal name.
issue_timestamp	
The token issue timestamp in milliseconds.
expiry_timestamp	
The token expiry timestamp in milliseconds.
max_timestamp	
The token maximum timestamp length in milliseconds.
token_id	
The token ID.
hmac	
The token HMAC.
renewers	
Those who are able to renew this token before it expires.
principal_type	
The renewer principal type.
principal_name	
The renewer principal name.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
DescribeDelegationToken Response (Version: 2) => error_code [tokens] throttle_time_ms 
  error_code => INT16
  tokens => principal_type principal_name issue_timestamp expiry_timestamp max_timestamp token_id hmac [renewers] 
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
    issue_timestamp => INT64
    expiry_timestamp => INT64
    max_timestamp => INT64
    token_id => COMPACT_STRING
    hmac => COMPACT_BYTES
    renewers => principal_type principal_name 
      principal_type => COMPACT_STRING
      principal_name => COMPACT_STRING
  throttle_time_ms => INT32
Response header version: 1

Field	
Description
error_code	
The error code, or 0 if there was no error.
tokens	
The tokens.
principal_type	
The token principal type.
principal_name	
The token principal name.
issue_timestamp	
The token issue timestamp in milliseconds.
expiry_timestamp	
The token expiry timestamp in milliseconds.
max_timestamp	
The token maximum timestamp length in milliseconds.
token_id	
The token ID.
hmac	
The token HMAC.
renewers	
Those who are able to renew this token before it expires.
principal_type	
The renewer principal type.
principal_name	
The renewer principal name.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
DescribeDelegationToken Response (Version: 3) => error_code [tokens] throttle_time_ms 
  error_code => INT16
  tokens => principal_type principal_name token_requester_principal_type token_requester_principal_name issue_timestamp expiry_timestamp max_timestamp token_id hmac [renewers] 
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
    token_requester_principal_type => COMPACT_STRING
    token_requester_principal_name => COMPACT_STRING
    issue_timestamp => INT64
    expiry_timestamp => INT64
    max_timestamp => INT64
    token_id => COMPACT_STRING
    hmac => COMPACT_BYTES
    renewers => principal_type principal_name 
      principal_type => COMPACT_STRING
      principal_name => COMPACT_STRING
  throttle_time_ms => INT32
Response header version: 1

Field	
Description
error_code	
The error code, or 0 if there was no error.
tokens	
The tokens.
principal_type	
The token principal type.
principal_name	
The token principal name.
token_requester_principal_type	
The principal type of the requester of the token.
token_requester_principal_name	
The principal type of the requester of the token.
issue_timestamp	
The token issue timestamp in milliseconds.
expiry_timestamp	
The token expiry timestamp in milliseconds.
max_timestamp	
The token maximum timestamp length in milliseconds.
token_id	
The token ID.
hmac	
The token HMAC.
renewers	
Those who are able to renew this token before it expires.
principal_type	
The renewer principal type.
principal_name	
The renewer principal name.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
DeleteGroups API (Key: 42):
Requests:

DeleteGroups Request (Version: 0) => [groups_names] 
  groups_names => STRING
Request header version: 1

Field	
Description
groups_names	
The group names to delete.
DeleteGroups Request (Version: 1) => [groups_names] 
  groups_names => STRING
Request header version: 1

Field	
Description
groups_names	
The group names to delete.
DeleteGroups Request (Version: 2) => [groups_names] 
  groups_names => COMPACT_STRING
Request header version: 2

Field	
Description
groups_names	
The group names to delete.
Responses:

DeleteGroups Response (Version: 0) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => group_id error_code 
    group_id => STRING
    error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The deletion results.
group_id	
The group id.
error_code	
The deletion error, or 0 if the deletion succeeded.
DeleteGroups Response (Version: 1) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => group_id error_code 
    group_id => STRING
    error_code => INT16
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The deletion results.
group_id	
The group id.
error_code	
The deletion error, or 0 if the deletion succeeded.
DeleteGroups Response (Version: 2) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => group_id error_code 
    group_id => COMPACT_STRING
    error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The deletion results.
group_id	
The group id.
error_code	
The deletion error, or 0 if the deletion succeeded.
ElectLeaders API (Key: 43):
Requests:

ElectLeaders Request (Version: 0) => [topic_partitions] timeout_ms 
  topic_partitions => topic [partitions] 
    topic => STRING
    partitions => INT32
  timeout_ms => INT32
Request header version: 1

Field	
Description
topic_partitions	
The topic partitions to elect leaders.
topic	
The name of a topic.
partitions	
The partitions of this topic whose leader should be elected.
timeout_ms	
The time in ms to wait for the election to complete.
ElectLeaders Request (Version: 1) => election_type [topic_partitions] timeout_ms 
  election_type => INT8
  topic_partitions => topic [partitions] 
    topic => STRING
    partitions => INT32
  timeout_ms => INT32
Request header version: 1

Field	
Description
election_type	
Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica.
topic_partitions	
The topic partitions to elect leaders.
topic	
The name of a topic.
partitions	
The partitions of this topic whose leader should be elected.
timeout_ms	
The time in ms to wait for the election to complete.
ElectLeaders Request (Version: 2) => election_type [topic_partitions] timeout_ms 
  election_type => INT8
  topic_partitions => topic [partitions] 
    topic => COMPACT_STRING
    partitions => INT32
  timeout_ms => INT32
Request header version: 2

Field	
Description
election_type	
Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica.
topic_partitions	
The topic partitions to elect leaders.
topic	
The name of a topic.
partitions	
The partitions of this topic whose leader should be elected.
timeout_ms	
The time in ms to wait for the election to complete.
Responses:

ElectLeaders Response (Version: 0) => throttle_time_ms [replica_election_results] 
  throttle_time_ms => INT32
  replica_election_results => topic [partition_result] 
    topic => STRING
    partition_result => partition_id error_code error_message 
      partition_id => INT32
      error_code => INT16
      error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
replica_election_results	
The election results, or an empty array if the requester did not have permission and the request asks for all partitions.
topic	
The topic name.
partition_result	
The results for each partition.
partition_id	
The partition id.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
ElectLeaders Response (Version: 1) => throttle_time_ms error_code [replica_election_results] 
  throttle_time_ms => INT32
  error_code => INT16
  replica_election_results => topic [partition_result] 
    topic => STRING
    partition_result => partition_id error_code error_message 
      partition_id => INT32
      error_code => INT16
      error_message => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
replica_election_results	
The election results, or an empty array if the requester did not have permission and the request asks for all partitions.
topic	
The topic name.
partition_result	
The results for each partition.
partition_id	
The partition id.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
ElectLeaders Response (Version: 2) => throttle_time_ms error_code [replica_election_results] 
  throttle_time_ms => INT32
  error_code => INT16
  replica_election_results => topic [partition_result] 
    topic => COMPACT_STRING
    partition_result => partition_id error_code error_message 
      partition_id => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
replica_election_results	
The election results, or an empty array if the requester did not have permission and the request asks for all partitions.
topic	
The topic name.
partition_result	
The results for each partition.
partition_id	
The partition id.
error_code	
The result error, or zero if there was no error.
error_message	
The result message, or null if there was no error.
IncrementalAlterConfigs API (Key: 44):
Requests:

IncrementalAlterConfigs Request (Version: 0) => [resources] validate_only 
  resources => resource_type resource_name [configs] 
    resource_type => INT8
    resource_name => STRING
    configs => name config_operation value 
      name => STRING
      config_operation => INT8
      value => NULLABLE_STRING
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
resources	
The incremental updates for each resource.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
The configurations.
name	
The configuration key name.
config_operation	
The type (Set, Delete, Append, Subtract) of operation.
value	
The value to set for the configuration key.
validate_only	
True if we should validate the request, but not change the configurations.
IncrementalAlterConfigs Request (Version: 1) => [resources] validate_only 
  resources => resource_type resource_name [configs] 
    resource_type => INT8
    resource_name => COMPACT_STRING
    configs => name config_operation value 
      name => COMPACT_STRING
      config_operation => INT8
      value => COMPACT_NULLABLE_STRING
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
resources	
The incremental updates for each resource.
resource_type	
The resource type.
resource_name	
The resource name.
configs	
The configurations.
name	
The configuration key name.
config_operation	
The type (Set, Delete, Append, Subtract) of operation.
value	
The value to set for the configuration key.
validate_only	
True if we should validate the request, but not change the configurations.
Responses:

IncrementalAlterConfigs Response (Version: 0) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => error_code error_message resource_type resource_name 
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
Response header version: 0

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The responses for each resource.
error_code	
The resource error code.
error_message	
The resource error message, or null if there was no error.
resource_type	
The resource type.
resource_name	
The resource name.
IncrementalAlterConfigs Response (Version: 1) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => error_code error_message resource_type resource_name 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    resource_type => INT8
    resource_name => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
responses	
The responses for each resource.
error_code	
The resource error code.
error_message	
The resource error message, or null if there was no error.
resource_type	
The resource type.
resource_name	
The resource name.
AlterPartitionReassignments API (Key: 45):
Requests:

AlterPartitionReassignments Request (Version: 0) => timeout_ms [topics] 
  timeout_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index [replicas] 
      partition_index => INT32
      replicas => INT32
Request header version: 2

Field	
Description
timeout_ms	
The time in ms to wait for the request to complete.
topics	
The topics to reassign.
name	
The topic name.
partitions	
The partitions to reassign.
partition_index	
The partition index.
replicas	
The replicas to place the partitions on, or null to cancel a pending reassignment for this partition.
AlterPartitionReassignments Request (Version: 1) => timeout_ms allow_replication_factor_change [topics] 
  timeout_ms => INT32
  allow_replication_factor_change => BOOLEAN
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index [replicas] 
      partition_index => INT32
      replicas => INT32
Request header version: 2

Field	
Description
timeout_ms	
The time in ms to wait for the request to complete.
allow_replication_factor_change	
The option indicating whether changing the replication factor of any given partition as part of this request is a valid move.
topics	
The topics to reassign.
name	
The topic name.
partitions	
The partitions to reassign.
partition_index	
The partition index.
replicas	
The replicas to place the partitions on, or null to cancel a pending reassignment for this partition.
Responses:

AlterPartitionReassignments Response (Version: 0) => throttle_time_ms error_code error_message [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code error_message 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
responses	
The responses to topics to reassign.
name	
The topic name.
partitions	
The responses to partitions to reassign.
partition_index	
The partition index.
error_code	
The error code for this partition, or 0 if there was no error.
error_message	
The error message for this partition, or null if there was no error.
AlterPartitionReassignments Response (Version: 1) => throttle_time_ms allow_replication_factor_change error_code error_message [responses] 
  throttle_time_ms => INT32
  allow_replication_factor_change => BOOLEAN
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code error_message 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
allow_replication_factor_change	
The option indicating whether changing the replication factor of any given partition as part of the request was allowed.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
responses	
The responses to topics to reassign.
name	
The topic name.
partitions	
The responses to partitions to reassign.
partition_index	
The partition index.
error_code	
The error code for this partition, or 0 if there was no error.
error_message	
The error message for this partition, or null if there was no error.
ListPartitionReassignments API (Key: 46):
Requests:

ListPartitionReassignments Request (Version: 0) => timeout_ms [topics] 
  timeout_ms => INT32
  topics => name [partition_indexes] 
    name => COMPACT_STRING
    partition_indexes => INT32
Request header version: 2

Field	
Description
timeout_ms	
The time in ms to wait for the request to complete.
topics	
The topics to list partition reassignments for, or null to list everything.
name	
The topic name.
partition_indexes	
The partitions to list partition reassignments for.
Responses:

ListPartitionReassignments Response (Version: 0) => throttle_time_ms error_code error_message [topics] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index [replicas] [adding_replicas] [removing_replicas] 
      partition_index => INT32
      replicas => INT32
      adding_replicas => INT32
      removing_replicas => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
topics	
The ongoing reassignments for each topic.
name	
The topic name.
partitions	
The ongoing reassignments for each partition.
partition_index	
The index of the partition.
replicas	
The current replica set.
adding_replicas	
The set of replicas we are currently adding.
removing_replicas	
The set of replicas we are currently removing.
OffsetDelete API (Key: 47):
Requests:

OffsetDelete Request (Version: 0) => group_id [topics] 
  group_id => STRING
  topics => name [partitions] 
    name => STRING
    partitions => partition_index 
      partition_index => INT32
Request header version: 1

Field	
Description
group_id	
The unique group identifier.
topics	
The topics to delete offsets for.
name	
The topic name.
partitions	
Each partition to delete offsets for.
partition_index	
The partition index.
Responses:

OffsetDelete Response (Version: 0) => error_code throttle_time_ms [topics] 
  error_code => INT16
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
Response header version: 0

Field	
Description
error_code	
The top-level error code, or 0 if there was no error.
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
The responses for each topic.
name	
The topic name.
partitions	
The responses for each partition in the topic.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
DescribeClientQuotas API (Key: 48):
Requests:

DescribeClientQuotas Request (Version: 0) => [components] strict 
  components => entity_type match_type match 
    entity_type => STRING
    match_type => INT8
    match => NULLABLE_STRING
  strict => BOOLEAN
Request header version: 1

Field	
Description
components	
Filter components to apply to quota entities.
entity_type	
The entity type that the filter component applies to.
match_type	
How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
match	
The string to match against, or null if unused for the match type.
strict	
Whether the match is strict, i.e. should exclude entities with unspecified entity types.
DescribeClientQuotas Request (Version: 1) => [components] strict 
  components => entity_type match_type match 
    entity_type => COMPACT_STRING
    match_type => INT8
    match => COMPACT_NULLABLE_STRING
  strict => BOOLEAN
Request header version: 2

Field	
Description
components	
Filter components to apply to quota entities.
entity_type	
The entity type that the filter component applies to.
match_type	
How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
match	
The string to match against, or null if unused for the match type.
strict	
Whether the match is strict, i.e. should exclude entities with unspecified entity types.
Responses:

DescribeClientQuotas Response (Version: 0) => throttle_time_ms error_code error_message [entries] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  entries => [entity] [values] 
    entity => entity_type entity_name 
      entity_type => STRING
      entity_name => NULLABLE_STRING
    values => key value 
      key => STRING
      value => FLOAT64
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or `0` if the quota description succeeded.
error_message	
The error message, or `null` if the quota description succeeded.
entries	
A result entry.
entity	
The quota entity description.
entity_type	
The entity type.
entity_name	
The entity name, or null if the default.
values	
The quota values for the entity.
key	
The quota configuration key.
value	
The quota configuration value.
DescribeClientQuotas Response (Version: 1) => throttle_time_ms error_code error_message [entries] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  entries => [entity] [values] 
    entity => entity_type entity_name 
      entity_type => COMPACT_STRING
      entity_name => COMPACT_NULLABLE_STRING
    values => key value 
      key => COMPACT_STRING
      value => FLOAT64
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or `0` if the quota description succeeded.
error_message	
The error message, or `null` if the quota description succeeded.
entries	
A result entry.
entity	
The quota entity description.
entity_type	
The entity type.
entity_name	
The entity name, or null if the default.
values	
The quota values for the entity.
key	
The quota configuration key.
value	
The quota configuration value.
AlterClientQuotas API (Key: 49):
Requests:

AlterClientQuotas Request (Version: 0) => [entries] validate_only 
  entries => [entity] [ops] 
    entity => entity_type entity_name 
      entity_type => STRING
      entity_name => NULLABLE_STRING
    ops => key value remove 
      key => STRING
      value => FLOAT64
      remove => BOOLEAN
  validate_only => BOOLEAN
Request header version: 1

Field	
Description
entries	
The quota configuration entries to alter.
entity	
The quota entity to alter.
entity_type	
The entity type.
entity_name	
The name of the entity, or null if the default.
ops	
An individual quota configuration entry to alter.
key	
The quota configuration key.
value	
The value to set, otherwise ignored if the value is to be removed.
remove	
Whether the quota configuration value should be removed, otherwise set.
validate_only	
Whether the alteration should be validated, but not performed.
AlterClientQuotas Request (Version: 1) => [entries] validate_only 
  entries => [entity] [ops] 
    entity => entity_type entity_name 
      entity_type => COMPACT_STRING
      entity_name => COMPACT_NULLABLE_STRING
    ops => key value remove 
      key => COMPACT_STRING
      value => FLOAT64
      remove => BOOLEAN
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
entries	
The quota configuration entries to alter.
entity	
The quota entity to alter.
entity_type	
The entity type.
entity_name	
The name of the entity, or null if the default.
ops	
An individual quota configuration entry to alter.
key	
The quota configuration key.
value	
The value to set, otherwise ignored if the value is to be removed.
remove	
Whether the quota configuration value should be removed, otherwise set.
validate_only	
Whether the alteration should be validated, but not performed.
Responses:

AlterClientQuotas Response (Version: 0) => throttle_time_ms [entries] 
  throttle_time_ms => INT32
  entries => error_code error_message [entity] 
    error_code => INT16
    error_message => NULLABLE_STRING
    entity => entity_type entity_name 
      entity_type => STRING
      entity_name => NULLABLE_STRING
Response header version: 0

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
entries	
The quota configuration entries to alter.
error_code	
The error code, or `0` if the quota alteration succeeded.
error_message	
The error message, or `null` if the quota alteration succeeded.
entity	
The quota entity to alter.
entity_type	
The entity type.
entity_name	
The name of the entity, or null if the default.
AlterClientQuotas Response (Version: 1) => throttle_time_ms [entries] 
  throttle_time_ms => INT32
  entries => error_code error_message [entity] 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    entity => entity_type entity_name 
      entity_type => COMPACT_STRING
      entity_name => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
entries	
The quota configuration entries to alter.
error_code	
The error code, or `0` if the quota alteration succeeded.
error_message	
The error message, or `null` if the quota alteration succeeded.
entity	
The quota entity to alter.
entity_type	
The entity type.
entity_name	
The name of the entity, or null if the default.
DescribeUserScramCredentials API (Key: 50):
Requests:

DescribeUserScramCredentials Request (Version: 0) => [users] 
  users => name 
    name => COMPACT_STRING
Request header version: 2

Field	
Description
users	
The users to describe, or null/empty to describe all users.
name	
The user name.
Responses:

DescribeUserScramCredentials Response (Version: 0) => throttle_time_ms error_code error_message [results] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  results => user error_code error_message [credential_infos] 
    user => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    credential_infos => mechanism iterations 
      mechanism => INT8
      iterations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The message-level error code, 0 except for user authorization or infrastructure issues.
error_message	
The message-level error message, if any.
results	
The results for descriptions, one per user.
user	
The user name.
error_code	
The user-level error code.
error_message	
The user-level error message, if any.
credential_infos	
The mechanism and related information associated with the user's SCRAM credentials.
mechanism	
The SCRAM mechanism.
iterations	
The number of iterations used in the SCRAM credential.
AlterUserScramCredentials API (Key: 51):
Requests:

AlterUserScramCredentials Request (Version: 0) => [deletions] [upsertions] 
  deletions => name mechanism 
    name => COMPACT_STRING
    mechanism => INT8
  upsertions => name mechanism iterations salt salted_password 
    name => COMPACT_STRING
    mechanism => INT8
    iterations => INT32
    salt => COMPACT_BYTES
    salted_password => COMPACT_BYTES
Request header version: 2

Field	
Description
deletions	
The SCRAM credentials to remove.
name	
The user name.
mechanism	
The SCRAM mechanism.
upsertions	
The SCRAM credentials to update/insert.
name	
The user name.
mechanism	
The SCRAM mechanism.
iterations	
The number of iterations.
salt	
A random salt generated by the client.
salted_password	
The salted password.
Responses:

AlterUserScramCredentials Response (Version: 0) => throttle_time_ms [results] 
  throttle_time_ms => INT32
  results => user error_code error_message 
    user => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
results	
The results for deletions and alterations, one per affected user.
user	
The user name.
error_code	
The error code.
error_message	
The error message, if any.
DescribeQuorum API (Key: 55):
Requests:

DescribeQuorum Request (Version: 0) => [topics] 
  topics => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index 
      partition_index => INT32
Request header version: 2

Field	
Description
topics	
The topics to describe.
topic_name	
The topic name.
partitions	
The partitions to describe.
partition_index	
The partition index.
DescribeQuorum Request (Version: 1) => [topics] 
  topics => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index 
      partition_index => INT32
Request header version: 2

Field	
Description
topics	
The topics to describe.
topic_name	
The topic name.
partitions	
The partitions to describe.
partition_index	
The partition index.
DescribeQuorum Request (Version: 2) => [topics] 
  topics => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index 
      partition_index => INT32
Request header version: 2

Field	
Description
topics	
The topics to describe.
topic_name	
The topic name.
partitions	
The partitions to describe.
partition_index	
The partition index.
Responses:

DescribeQuorum Response (Version: 0) => error_code [topics] 
  error_code => INT16
  topics => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index error_code leader_id leader_epoch high_watermark [current_voters] [observers] 
      partition_index => INT32
      error_code => INT16
      leader_id => INT32
      leader_epoch => INT32
      high_watermark => INT64
      current_voters => replica_id log_end_offset 
        replica_id => INT32
        log_end_offset => INT64
      observers => replica_id log_end_offset 
        replica_id => INT32
        log_end_offset => INT64
Response header version: 1

Field	
Description
error_code	
The top level error code.
topics	
The response from the describe quorum API.
topic_name	
The topic name.
partitions	
The partition data.
partition_index	
The partition index.
error_code	
The partition error code.
leader_id	
The ID of the current leader or -1 if the leader is unknown.
leader_epoch	
The latest known leader epoch.
high_watermark	
The high water mark.
current_voters	
The current voters of the partition.
replica_id	
The ID of the replica.
log_end_offset	
The last known log end offset of the follower or -1 if it is unknown.
observers	
The observers of the partition.
DescribeQuorum Response (Version: 1) => error_code [topics] 
  error_code => INT16
  topics => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index error_code leader_id leader_epoch high_watermark [current_voters] [observers] 
      partition_index => INT32
      error_code => INT16
      leader_id => INT32
      leader_epoch => INT32
      high_watermark => INT64
      current_voters => replica_id log_end_offset last_fetch_timestamp last_caught_up_timestamp 
        replica_id => INT32
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
      observers => replica_id log_end_offset last_fetch_timestamp last_caught_up_timestamp 
        replica_id => INT32
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
Response header version: 1

Field	
Description
error_code	
The top level error code.
topics	
The response from the describe quorum API.
topic_name	
The topic name.
partitions	
The partition data.
partition_index	
The partition index.
error_code	
The partition error code.
leader_id	
The ID of the current leader or -1 if the leader is unknown.
leader_epoch	
The latest known leader epoch.
high_watermark	
The high water mark.
current_voters	
The current voters of the partition.
replica_id	
The ID of the replica.
log_end_offset	
The last known log end offset of the follower or -1 if it is unknown.
last_fetch_timestamp	
The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter.
last_caught_up_timestamp	
The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter.
observers	
The observers of the partition.
DescribeQuorum Response (Version: 2) => error_code error_message [topics] [nodes] 
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  topics => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index error_code error_message leader_id leader_epoch high_watermark [current_voters] [observers] 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      leader_id => INT32
      leader_epoch => INT32
      high_watermark => INT64
      current_voters => replica_id replica_directory_id log_end_offset last_fetch_timestamp last_caught_up_timestamp 
        replica_id => INT32
        replica_directory_id => UUID
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
      observers => replica_id replica_directory_id log_end_offset last_fetch_timestamp last_caught_up_timestamp 
        replica_id => INT32
        replica_directory_id => UUID
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
  nodes => node_id [listeners] 
    node_id => INT32
    listeners => name host port 
      name => COMPACT_STRING
      host => COMPACT_STRING
      port => UINT16
Response header version: 1

Field	
Description
error_code	
The top level error code.
error_message	
The error message, or null if there was no error.
topics	
The response from the describe quorum API.
topic_name	
The topic name.
partitions	
The partition data.
partition_index	
The partition index.
error_code	
The partition error code.
error_message	
The error message, or null if there was no error.
leader_id	
The ID of the current leader or -1 if the leader is unknown.
leader_epoch	
The latest known leader epoch.
high_watermark	
The high water mark.
current_voters	
The current voters of the partition.
replica_id	
The ID of the replica.
replica_directory_id	
The replica directory ID of the replica.
log_end_offset	
The last known log end offset of the follower or -1 if it is unknown.
last_fetch_timestamp	
The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter.
last_caught_up_timestamp	
The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter.
observers	
The observers of the partition.
nodes	
The nodes in the quorum.
node_id	
The ID of the associated node.
listeners	
The listeners of this controller.
name	
The name of the endpoint.
host	
The hostname.
port	
The port.
UpdateFeatures API (Key: 57):
Requests:

UpdateFeatures Request (Version: 0) => timeout_ms [feature_updates] 
  timeout_ms => INT32
  feature_updates => feature max_version_level allow_downgrade 
    feature => COMPACT_STRING
    max_version_level => INT16
    allow_downgrade => BOOLEAN
Request header version: 2

Field	
Description
timeout_ms	
How long to wait in milliseconds before timing out the request.
feature_updates	
The list of updates to finalized features.
feature	
The name of the finalized feature to be updated.
max_version_level	
The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
allow_downgrade	
DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
UpdateFeatures Request (Version: 1) => timeout_ms [feature_updates] validate_only 
  timeout_ms => INT32
  feature_updates => feature max_version_level upgrade_type 
    feature => COMPACT_STRING
    max_version_level => INT16
    upgrade_type => INT8
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
timeout_ms	
How long to wait in milliseconds before timing out the request.
feature_updates	
The list of updates to finalized features.
feature	
The name of the finalized feature to be updated.
max_version_level	
The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
upgrade_type	
Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
validate_only	
True if we should validate the request, but not perform the upgrade or downgrade.
UpdateFeatures Request (Version: 2) => timeout_ms [feature_updates] validate_only 
  timeout_ms => INT32
  feature_updates => feature max_version_level upgrade_type 
    feature => COMPACT_STRING
    max_version_level => INT16
    upgrade_type => INT8
  validate_only => BOOLEAN
Request header version: 2

Field	
Description
timeout_ms	
How long to wait in milliseconds before timing out the request.
feature_updates	
The list of updates to finalized features.
feature	
The name of the finalized feature to be updated.
max_version_level	
The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
upgrade_type	
Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
validate_only	
True if we should validate the request, but not perform the upgrade or downgrade.
Responses:

UpdateFeatures Response (Version: 0) => throttle_time_ms error_code error_message [results] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  results => feature error_code error_message 
    feature => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or `0` if there was no top-level error.
error_message	
The top-level error message, or `null` if there was no top-level error.
results	
Results for each feature update.
feature	
The name of the finalized feature.
error_code	
The feature update error code or `0` if the feature update succeeded.
error_message	
The feature update error, or `null` if the feature update succeeded.
UpdateFeatures Response (Version: 1) => throttle_time_ms error_code error_message [results] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  results => feature error_code error_message 
    feature => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or `0` if there was no top-level error.
error_message	
The top-level error message, or `null` if there was no top-level error.
results	
Results for each feature update.
feature	
The name of the finalized feature.
error_code	
The feature update error code or `0` if the feature update succeeded.
error_message	
The feature update error, or `null` if the feature update succeeded.
UpdateFeatures Response (Version: 2) => throttle_time_ms error_code error_message 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or `0` if there was no top-level error.
error_message	
The top-level error message, or `null` if there was no top-level error.
DescribeCluster API (Key: 60):
Requests:

DescribeCluster Request (Version: 0) => include_cluster_authorized_operations 
  include_cluster_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
include_cluster_authorized_operations	
Whether to include cluster authorized operations.
DescribeCluster Request (Version: 1) => include_cluster_authorized_operations endpoint_type 
  include_cluster_authorized_operations => BOOLEAN
  endpoint_type => INT8
Request header version: 2

Field	
Description
include_cluster_authorized_operations	
Whether to include cluster authorized operations.
endpoint_type	
The endpoint type to describe. 1=brokers, 2=controllers.
DescribeCluster Request (Version: 2) => include_cluster_authorized_operations endpoint_type include_fenced_brokers 
  include_cluster_authorized_operations => BOOLEAN
  endpoint_type => INT8
  include_fenced_brokers => BOOLEAN
Request header version: 2

Field	
Description
include_cluster_authorized_operations	
Whether to include cluster authorized operations.
endpoint_type	
The endpoint type to describe. 1=brokers, 2=controllers.
include_fenced_brokers	
Whether to include fenced brokers when listing brokers.
Responses:

DescribeCluster Response (Version: 0) => throttle_time_ms error_code error_message cluster_id controller_id [brokers] cluster_authorized_operations 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_STRING
  controller_id => INT32
  brokers => broker_id host port rack 
    broker_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller. When handled by a controller, returns the current voter leader ID. When handled by a broker, returns a random alive broker ID as a fallback.
brokers	
Each broker in the response.
broker_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_authorized_operations	
32-bit bitfield to represent authorized operations for this cluster.
DescribeCluster Response (Version: 1) => throttle_time_ms error_code error_message endpoint_type cluster_id controller_id [brokers] cluster_authorized_operations 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  endpoint_type => INT8
  cluster_id => COMPACT_STRING
  controller_id => INT32
  brokers => broker_id host port rack 
    broker_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
endpoint_type	
The endpoint type that was described. 1=brokers, 2=controllers.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller. When handled by a controller, returns the current voter leader ID. When handled by a broker, returns a random alive broker ID as a fallback.
brokers	
Each broker in the response.
broker_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
cluster_authorized_operations	
32-bit bitfield to represent authorized operations for this cluster.
DescribeCluster Response (Version: 2) => throttle_time_ms error_code error_message endpoint_type cluster_id controller_id [brokers] cluster_authorized_operations 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  endpoint_type => INT8
  cluster_id => COMPACT_STRING
  controller_id => INT32
  brokers => broker_id host port rack is_fenced 
    broker_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
    is_fenced => BOOLEAN
  cluster_authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
endpoint_type	
The endpoint type that was described. 1=brokers, 2=controllers.
cluster_id	
The cluster ID that responding broker belongs to.
controller_id	
The ID of the controller. When handled by a controller, returns the current voter leader ID. When handled by a broker, returns a random alive broker ID as a fallback.
brokers	
Each broker in the response.
broker_id	
The broker ID.
host	
The broker hostname.
port	
The broker port.
rack	
The rack of the broker, or null if it has not been assigned to a rack.
is_fenced	
Whether the broker is fenced
cluster_authorized_operations	
32-bit bitfield to represent authorized operations for this cluster.
DescribeProducers API (Key: 61):
Requests:

DescribeProducers Request (Version: 0) => [topics] 
  topics => name [partition_indexes] 
    name => COMPACT_STRING
    partition_indexes => INT32
Request header version: 2

Field	
Description
topics	
The topics to list producers for.
name	
The topic name.
partition_indexes	
The indexes of the partitions to list producers for.
Responses:

DescribeProducers Response (Version: 0) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name [partitions] 
    name => COMPACT_STRING
    partitions => partition_index error_code error_message [active_producers] 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      active_producers => producer_id producer_epoch last_sequence last_timestamp coordinator_epoch current_txn_start_offset 
        producer_id => INT64
        producer_epoch => INT32
        last_sequence => INT32
        last_timestamp => INT64
        coordinator_epoch => INT32
        current_txn_start_offset => INT64
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
name	
The topic name.
partitions	
Each partition in the response.
partition_index	
The partition index.
error_code	
The partition error code, or 0 if there was no error.
error_message	
The partition error message, which may be null if no additional details are available.
active_producers	
The active producers for the partition.
producer_id	
The producer id.
producer_epoch	
The producer epoch.
last_sequence	
The last sequence number sent by the producer.
last_timestamp	
The last timestamp sent by the producer.
coordinator_epoch	
The current epoch of the producer group.
current_txn_start_offset	
The current transaction start offset of the producer.
UnregisterBroker API (Key: 64):
Requests:

UnregisterBroker Request (Version: 0) => broker_id 
  broker_id => INT32
Request header version: 2

Field	
Description
broker_id	
The broker ID to unregister.
Responses:

UnregisterBroker Response (Version: 0) => throttle_time_ms error_code error_message 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The top-level error message, or `null` if there was no top-level error.
DescribeTransactions API (Key: 65):
Requests:

DescribeTransactions Request (Version: 0) => [transactional_ids] 
  transactional_ids => COMPACT_STRING
Request header version: 2

Field	
Description
transactional_ids	
Array of transactionalIds to include in describe results. If empty, then no results will be returned.
Responses:

DescribeTransactions Response (Version: 0) => throttle_time_ms [transaction_states] 
  throttle_time_ms => INT32
  transaction_states => error_code transactional_id transaction_state transaction_timeout_ms transaction_start_time_ms producer_id producer_epoch [topics] 
    error_code => INT16
    transactional_id => COMPACT_STRING
    transaction_state => COMPACT_STRING
    transaction_timeout_ms => INT32
    transaction_start_time_ms => INT64
    producer_id => INT64
    producer_epoch => INT16
    topics => topic [partitions] 
      topic => COMPACT_STRING
      partitions => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
transaction_states	
The current state of the transaction.
error_code	
The error code.
transactional_id	
The transactional id.
transaction_state	
The current transaction state of the producer.
transaction_timeout_ms	
The timeout in milliseconds for the transaction.
transaction_start_time_ms	
The start time of the transaction in milliseconds.
producer_id	
The current producer id associated with the transaction.
producer_epoch	
The current epoch associated with the producer id.
topics	
The set of partitions included in the current transaction (if active). When a transaction is preparing to commit or abort, this will include only partitions which do not have markers.
topic	
The topic name.
partitions	
The partition ids included in the current transaction.
ListTransactions API (Key: 66):
Requests:

ListTransactions Request (Version: 0) => [state_filters] [producer_id_filters] 
  state_filters => COMPACT_STRING
  producer_id_filters => INT64
Request header version: 2

Field	
Description
state_filters	
The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
producer_id_filters	
The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
ListTransactions Request (Version: 1) => [state_filters] [producer_id_filters] duration_filter 
  state_filters => COMPACT_STRING
  producer_id_filters => INT64
  duration_filter => INT64
Request header version: 2

Field	
Description
state_filters	
The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
producer_id_filters	
The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
duration_filter	
Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
ListTransactions Request (Version: 2) => [state_filters] [producer_id_filters] duration_filter transactional_id_pattern 
  state_filters => COMPACT_STRING
  producer_id_filters => INT64
  duration_filter => INT64
  transactional_id_pattern => COMPACT_NULLABLE_STRING
Request header version: 2

Field	
Description
state_filters	
The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
producer_id_filters	
The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
duration_filter	
Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
transactional_id_pattern	
The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
Responses:

ListTransactions Response (Version: 0) => throttle_time_ms error_code [unknown_state_filters] [transaction_states] 
  throttle_time_ms => INT32
  error_code => INT16
  unknown_state_filters => COMPACT_STRING
  transaction_states => transactional_id producer_id transaction_state 
    transactional_id => COMPACT_STRING
    producer_id => INT64
    transaction_state => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
unknown_state_filters	
Set of state filters provided in the request which were unknown to the transaction coordinator.
transaction_states	
The current state of the transaction for the transactional id.
transactional_id	
The transactional id.
producer_id	
The producer id.
transaction_state	
The current transaction state of the producer.
ListTransactions Response (Version: 1) => throttle_time_ms error_code [unknown_state_filters] [transaction_states] 
  throttle_time_ms => INT32
  error_code => INT16
  unknown_state_filters => COMPACT_STRING
  transaction_states => transactional_id producer_id transaction_state 
    transactional_id => COMPACT_STRING
    producer_id => INT64
    transaction_state => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
unknown_state_filters	
Set of state filters provided in the request which were unknown to the transaction coordinator.
transaction_states	
The current state of the transaction for the transactional id.
transactional_id	
The transactional id.
producer_id	
The producer id.
transaction_state	
The current transaction state of the producer.
ListTransactions Response (Version: 2) => throttle_time_ms error_code [unknown_state_filters] [transaction_states] 
  throttle_time_ms => INT32
  error_code => INT16
  unknown_state_filters => COMPACT_STRING
  transaction_states => transactional_id producer_id transaction_state 
    transactional_id => COMPACT_STRING
    producer_id => INT64
    transaction_state => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
unknown_state_filters	
Set of state filters provided in the request which were unknown to the transaction coordinator.
transaction_states	
The current state of the transaction for the transactional id.
transactional_id	
The transactional id.
producer_id	
The producer id.
transaction_state	
The current transaction state of the producer.
ConsumerGroupHeartbeat API (Key: 68):
Requests:

ConsumerGroupHeartbeat Request (Version: 0) => group_id member_id member_epoch instance_id rack_id rebalance_timeout_ms [subscribed_topic_names] server_assignor [topic_partitions] 
  group_id => COMPACT_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  instance_id => COMPACT_NULLABLE_STRING
  rack_id => COMPACT_NULLABLE_STRING
  rebalance_timeout_ms => INT32
  subscribed_topic_names => COMPACT_STRING
  server_assignor => COMPACT_NULLABLE_STRING
  topic_partitions => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member id generated by the consumer. The member id must be kept during the entire lifetime of the consumer process.
member_epoch	
The current member epoch; 0 to join the group; -1 to leave the group; -2 to indicate that the static member will rejoin.
instance_id	
null if not provided or if it didn't change since the last heartbeat; the instance Id otherwise.
rack_id	
null if not provided or if it didn't change since the last heartbeat; the rack ID of consumer otherwise.
rebalance_timeout_ms	
-1 if it didn't change since the last heartbeat; the maximum time in milliseconds that the coordinator will wait on the member to revoke its partitions otherwise.
subscribed_topic_names	
null if it didn't change since the last heartbeat; the subscribed topic names otherwise.
server_assignor	
null if not used or if it didn't change since the last heartbeat; the server side assignor to use otherwise.
topic_partitions	
null if it didn't change since the last heartbeat; the partitions owned by the member.
topic_id	
The topic ID.
partitions	
The partitions.
ConsumerGroupHeartbeat Request (Version: 1) => group_id member_id member_epoch instance_id rack_id rebalance_timeout_ms [subscribed_topic_names] subscribed_topic_regex server_assignor [topic_partitions] 
  group_id => COMPACT_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  instance_id => COMPACT_NULLABLE_STRING
  rack_id => COMPACT_NULLABLE_STRING
  rebalance_timeout_ms => INT32
  subscribed_topic_names => COMPACT_STRING
  subscribed_topic_regex => COMPACT_NULLABLE_STRING
  server_assignor => COMPACT_NULLABLE_STRING
  topic_partitions => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member id generated by the consumer. The member id must be kept during the entire lifetime of the consumer process.
member_epoch	
The current member epoch; 0 to join the group; -1 to leave the group; -2 to indicate that the static member will rejoin.
instance_id	
null if not provided or if it didn't change since the last heartbeat; the instance Id otherwise.
rack_id	
null if not provided or if it didn't change since the last heartbeat; the rack ID of consumer otherwise.
rebalance_timeout_ms	
-1 if it didn't change since the last heartbeat; the maximum time in milliseconds that the coordinator will wait on the member to revoke its partitions otherwise.
subscribed_topic_names	
null if it didn't change since the last heartbeat; the subscribed topic names otherwise.
subscribed_topic_regex	
null if it didn't change since the last heartbeat; the subscribed topic regex otherwise.
server_assignor	
null if not used or if it didn't change since the last heartbeat; the server side assignor to use otherwise.
topic_partitions	
null if it didn't change since the last heartbeat; the partitions owned by the member.
topic_id	
The topic ID.
partitions	
The partitions.
Responses:

ConsumerGroupHeartbeat Response (Version: 0) => throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms assignment 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  member_epoch => INT32
  heartbeat_interval_ms => INT32
  assignment => [topic_partitions] 
    topic_partitions => topic_id [partitions] 
      topic_id => UUID
      partitions => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
member_id	
The member id is generated by the consumer starting from version 1, while in version 0, it can be provided by users or generated by the group coordinator.
member_epoch	
The member epoch.
heartbeat_interval_ms	
The heartbeat interval in milliseconds.
assignment	
null if not provided; the assignment otherwise.
topic_partitions	
The partitions assigned to the member that can be used immediately.
topic_id	
The topic ID.
partitions	
The partitions.
ConsumerGroupHeartbeat Response (Version: 1) => throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms assignment 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  member_epoch => INT32
  heartbeat_interval_ms => INT32
  assignment => [topic_partitions] 
    topic_partitions => topic_id [partitions] 
      topic_id => UUID
      partitions => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
member_id	
The member id is generated by the consumer starting from version 1, while in version 0, it can be provided by users or generated by the group coordinator.
member_epoch	
The member epoch.
heartbeat_interval_ms	
The heartbeat interval in milliseconds.
assignment	
null if not provided; the assignment otherwise.
topic_partitions	
The partitions assigned to the member that can be used immediately.
topic_id	
The topic ID.
partitions	
The partitions.
ConsumerGroupDescribe API (Key: 69):
Requests:

ConsumerGroupDescribe Request (Version: 0) => [group_ids] include_authorized_operations 
  group_ids => COMPACT_STRING
  include_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
group_ids	
The ids of the groups to describe.
include_authorized_operations	
Whether to include authorized operations.
ConsumerGroupDescribe Request (Version: 1) => [group_ids] include_authorized_operations 
  group_ids => COMPACT_STRING
  include_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
group_ids	
The ids of the groups to describe.
include_authorized_operations	
Whether to include authorized operations.
Responses:

ConsumerGroupDescribe Response (Version: 0) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name [members] authorized_operations 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    group_epoch => INT32
    assignment_epoch => INT32
    assignor_name => COMPACT_STRING
    members => member_id instance_id rack_id member_epoch client_id client_host [subscribed_topic_names] subscribed_topic_regex assignment target_assignment 
      member_id => COMPACT_STRING
      instance_id => COMPACT_NULLABLE_STRING
      rack_id => COMPACT_NULLABLE_STRING
      member_epoch => INT32
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      subscribed_topic_names => COMPACT_STRING
      subscribed_topic_regex => COMPACT_NULLABLE_STRING
      assignment => [topic_partitions] 
        topic_partitions => topic_id topic_name [partitions] 
          topic_id => UUID
          topic_name => COMPACT_STRING
          partitions => INT32
      target_assignment => [topic_partitions] 
        topic_partitions => topic_id topic_name [partitions] 
          topic_id => UUID
          topic_name => COMPACT_STRING
          partitions => INT32
    authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
group_epoch	
The group epoch.
assignment_epoch	
The assignment epoch.
assignor_name	
The selected assignor.
members	
The members.
member_id	
The member ID.
instance_id	
The member instance ID.
rack_id	
The member rack ID.
member_epoch	
The current member epoch.
client_id	
The client ID.
client_host	
The client host.
subscribed_topic_names	
The subscribed topic names.
subscribed_topic_regex	
the subscribed topic regex otherwise or null of not provided.
assignment	
The current assignment.
topic_partitions	
The assigned topic-partitions to the member.
topic_id	
The topic ID.
topic_name	
The topic name.
partitions	
The partitions.
target_assignment	
The target assignment.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
ConsumerGroupDescribe Response (Version: 1) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name [members] authorized_operations 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    group_epoch => INT32
    assignment_epoch => INT32
    assignor_name => COMPACT_STRING
    members => member_id instance_id rack_id member_epoch client_id client_host [subscribed_topic_names] subscribed_topic_regex assignment target_assignment member_type 
      member_id => COMPACT_STRING
      instance_id => COMPACT_NULLABLE_STRING
      rack_id => COMPACT_NULLABLE_STRING
      member_epoch => INT32
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      subscribed_topic_names => COMPACT_STRING
      subscribed_topic_regex => COMPACT_NULLABLE_STRING
      assignment => [topic_partitions] 
        topic_partitions => topic_id topic_name [partitions] 
          topic_id => UUID
          topic_name => COMPACT_STRING
          partitions => INT32
      target_assignment => [topic_partitions] 
        topic_partitions => topic_id topic_name [partitions] 
          topic_id => UUID
          topic_name => COMPACT_STRING
          partitions => INT32
      member_type => INT8
    authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
group_epoch	
The group epoch.
assignment_epoch	
The assignment epoch.
assignor_name	
The selected assignor.
members	
The members.
member_id	
The member ID.
instance_id	
The member instance ID.
rack_id	
The member rack ID.
member_epoch	
The current member epoch.
client_id	
The client ID.
client_host	
The client host.
subscribed_topic_names	
The subscribed topic names.
subscribed_topic_regex	
the subscribed topic regex otherwise or null of not provided.
assignment	
The current assignment.
topic_partitions	
The assigned topic-partitions to the member.
topic_id	
The topic ID.
topic_name	
The topic name.
partitions	
The partitions.
target_assignment	
The target assignment.
member_type	
-1 for unknown. 0 for classic member. +1 for consumer member.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
GetTelemetrySubscriptions API (Key: 71):
Requests:

GetTelemetrySubscriptions Request (Version: 0) => client_instance_id 
  client_instance_id => UUID
Request header version: 2

Field	
Description
client_instance_id	
Unique id for this client instance, must be set to 0 on the first request.
Responses:

GetTelemetrySubscriptions Response (Version: 0) => throttle_time_ms error_code client_instance_id subscription_id [accepted_compression_types] push_interval_ms telemetry_max_bytes delta_temporality [requested_metrics] 
  throttle_time_ms => INT32
  error_code => INT16
  client_instance_id => UUID
  subscription_id => INT32
  accepted_compression_types => INT8
  push_interval_ms => INT32
  telemetry_max_bytes => INT32
  delta_temporality => BOOLEAN
  requested_metrics => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
client_instance_id	
Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
subscription_id	
Unique identifier for the current subscription set for this client instance.
accepted_compression_types	
Compression types that broker accepts for the PushTelemetryRequest.
push_interval_ms	
Configured push interval, which is the lowest configured interval in the current subscription set.
telemetry_max_bytes	
The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
delta_temporality	
Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values.
requested_metrics	
Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed.
PushTelemetry API (Key: 72):
Requests:

PushTelemetry Request (Version: 0) => client_instance_id subscription_id terminating compression_type metrics 
  client_instance_id => UUID
  subscription_id => INT32
  terminating => BOOLEAN
  compression_type => INT8
  metrics => COMPACT_BYTES
Request header version: 2

Field	
Description
client_instance_id	
Unique id for this client instance.
subscription_id	
Unique identifier for the current subscription.
terminating	
Client is terminating the connection.
compression_type	
Compression codec used to compress the metrics.
metrics	
Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
Responses:

PushTelemetry Response (Version: 0) => throttle_time_ms error_code 
  throttle_time_ms => INT32
  error_code => INT16
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
ListConfigResources API (Key: 74):
Requests:

ListConfigResources Request (Version: 0) => 
Request header version: 2

ListConfigResources Request (Version: 1) => [resource_types] 
  resource_types => INT8
Request header version: 2

Field	
Description
resource_types	
The list of resource type. If the list is empty, it uses default supported config resource types.
Responses:

ListConfigResources Response (Version: 0) => throttle_time_ms error_code [config_resources] 
  throttle_time_ms => INT32
  error_code => INT16
  config_resources => resource_name 
    resource_name => COMPACT_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
config_resources	
Each config resource in the response.
resource_name	
The resource name.
ListConfigResources Response (Version: 1) => throttle_time_ms error_code [config_resources] 
  throttle_time_ms => INT32
  error_code => INT16
  config_resources => resource_name resource_type 
    resource_name => COMPACT_STRING
    resource_type => INT8
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
config_resources	
Each config resource in the response.
resource_name	
The resource name.
resource_type	
The resource type.
DescribeTopicPartitions API (Key: 75):
Requests:

DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor 
  topics => name 
    name => COMPACT_STRING
  response_partition_limit => INT32
  cursor => topic_name partition_index 
    topic_name => COMPACT_STRING
    partition_index => INT32
Request header version: 2

Field	
Description
topics	
The topics to fetch details for.
name	
The topic name.
response_partition_limit	
The maximum number of partitions included in the response.
cursor	
The first topic and partition index to fetch details for.
topic_name	
The name for the first topic to process.
partition_index	
The partition index to start with.
Responses:

DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor 
  throttle_time_ms => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations 
    error_code => INT16
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      eligible_leader_replicas => INT32
      last_known_elr => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  next_cursor => topic_name partition_index 
    topic_name => COMPACT_STRING
    partition_index => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
topics	
Each topic in the response.
error_code	
The topic error, or 0 if there was no error.
name	
The topic name.
topic_id	
The topic id.
is_internal	
True if the topic is internal.
partitions	
Each partition in the topic.
error_code	
The partition error, or 0 if there was no error.
partition_index	
The partition index.
leader_id	
The ID of the leader broker.
leader_epoch	
The leader epoch of this partition.
replica_nodes	
The set of all nodes that host this partition.
isr_nodes	
The set of nodes that are in sync with the leader for this partition.
eligible_leader_replicas	
The new eligible leader replicas otherwise.
last_known_elr	
The last known ELR.
offline_replicas	
The set of offline replicas of this partition.
topic_authorized_operations	
32-bit bitfield to represent authorized operations for this topic.
next_cursor	
The next topic and partition index to fetch details for.
topic_name	
The name for the first topic to process.
partition_index	
The partition index to start with.
ShareGroupHeartbeat API (Key: 76):
Requests:

ShareGroupHeartbeat Request (Version: 1) => group_id member_id member_epoch rack_id [subscribed_topic_names] 
  group_id => COMPACT_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  rack_id => COMPACT_NULLABLE_STRING
  subscribed_topic_names => COMPACT_STRING
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member id generated by the consumer. The member id must be kept during the entire lifetime of the consumer process.
member_epoch	
The current member epoch; 0 to join the group; -1 to leave the group.
rack_id	
null if not provided or if it didn't change since the last heartbeat; the rack ID of consumer otherwise.
subscribed_topic_names	
null if it didn't change since the last heartbeat; the subscribed topic names otherwise.
Responses:

ShareGroupHeartbeat Response (Version: 1) => throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms assignment 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  member_epoch => INT32
  heartbeat_interval_ms => INT32
  assignment => [topic_partitions] 
    topic_partitions => topic_id [partitions] 
      topic_id => UUID
      partitions => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
member_id	
The member ID is generated by the consumer and provided by the consumer for all requests.
member_epoch	
The member epoch.
heartbeat_interval_ms	
The heartbeat interval in milliseconds.
assignment	
null if not provided; the assignment otherwise.
topic_partitions	
The partitions assigned to the member.
topic_id	
The topic ID.
partitions	
The partitions.
ShareGroupDescribe API (Key: 77):
Requests:

ShareGroupDescribe Request (Version: 1) => [group_ids] include_authorized_operations 
  group_ids => COMPACT_STRING
  include_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
group_ids	
The ids of the groups to describe.
include_authorized_operations	
Whether to include authorized operations.
Responses:

ShareGroupDescribe Response (Version: 1) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name [members] authorized_operations 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    group_epoch => INT32
    assignment_epoch => INT32
    assignor_name => COMPACT_STRING
    members => member_id rack_id member_epoch client_id client_host [subscribed_topic_names] assignment 
      member_id => COMPACT_STRING
      rack_id => COMPACT_NULLABLE_STRING
      member_epoch => INT32
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      subscribed_topic_names => COMPACT_STRING
      assignment => [topic_partitions] 
        topic_partitions => topic_id topic_name [partitions] 
          topic_id => UUID
          topic_name => COMPACT_STRING
          partitions => INT32
    authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
group_epoch	
The group epoch.
assignment_epoch	
The assignment epoch.
assignor_name	
The selected assignor.
members	
The members.
member_id	
The member ID.
rack_id	
The member rack ID.
member_epoch	
The current member epoch.
client_id	
The client ID.
client_host	
The client host.
subscribed_topic_names	
The subscribed topic names.
assignment	
The current assignment.
topic_partitions	
The assigned topic-partitions to the member.
topic_id	
The topic ID.
topic_name	
The topic name.
partitions	
The partitions.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
ShareFetch API (Key: 78):
Requests:

ShareFetch Request (Version: 1) => group_id member_id share_session_epoch max_wait_ms min_bytes max_bytes max_records batch_size [topics] [forgotten_topics_data] 
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  max_records => INT32
  batch_size => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index [acknowledgement_batches] 
      partition_index => INT32
      acknowledgement_batches => first_offset last_offset [acknowledge_types] 
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member ID.
share_session_epoch	
The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
max_records	
The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
batch_size	
The optimal number of records for batches of acquired records and acknowledgements.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition_index	
The partition index.
acknowledgement_batches	
Record batches to acknowledge.
first_offset	
First offset of batch of records to acknowledge.
last_offset	
Last offset (inclusive) of batch of records to acknowledge.
acknowledge_types	
Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
forgotten_topics_data	
The partitions to remove from this share session.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
ShareFetch Request (Version: 2) => group_id member_id share_session_epoch max_wait_ms min_bytes max_bytes max_records batch_size share_acquire_mode is_renew_ack [topics] [forgotten_topics_data] 
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  max_records => INT32
  batch_size => INT32
  share_acquire_mode => INT8
  is_renew_ack => BOOLEAN
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index [acknowledgement_batches] 
      partition_index => INT32
      acknowledgement_batches => first_offset last_offset [acknowledge_types] 
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8
  forgotten_topics_data => topic_id [partitions] 
    topic_id => UUID
    partitions => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member ID.
share_session_epoch	
The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
max_wait_ms	
The maximum time in milliseconds to wait for the response.
min_bytes	
The minimum bytes to accumulate in the response.
max_bytes	
The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
max_records	
The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
batch_size	
The optimal number of records for batches of acquired records and acknowledgements.
share_acquire_mode	
The acquire mode to control the fetch behavior - 0:batch-optimized,1:record-limit.
is_renew_ack	
Whether Renew type acknowledgements present in AcknowledgementBatches.
topics	
The topics to fetch.
topic_id	
The unique topic ID.
partitions	
The partitions to fetch.
partition_index	
The partition index.
acknowledgement_batches	
Record batches to acknowledge.
first_offset	
First offset of batch of records to acknowledge.
last_offset	
Last offset (inclusive) of batch of records to acknowledge.
acknowledge_types	
Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
forgotten_topics_data	
The partitions to remove from this share session.
topic_id	
The unique topic ID.
partitions	
The partitions indexes to forget.
Responses:

ShareFetch Response (Version: 1) => throttle_time_ms error_code error_message acquisition_lock_timeout_ms [responses] [node_endpoints] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  acquisition_lock_timeout_ms => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code error_message acknowledge_error_code acknowledge_error_message current_leader records [acquired_records] 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      acknowledge_error_code => INT16
      acknowledge_error_message => COMPACT_NULLABLE_STRING
      current_leader => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      records => COMPACT_RECORDS
      acquired_records => first_offset last_offset delivery_count 
        first_offset => INT64
        last_offset => INT64
        delivery_count => INT16
  node_endpoints => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level response error code.
error_message	
The top-level error message, or null if there was no error.
acquisition_lock_timeout_ms	
The time in milliseconds for which the acquired records are locked.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The fetch error code, or 0 if there was no fetch error.
error_message	
The fetch error message, or null if there was no fetch error.
acknowledge_error_code	
The acknowledge error code, or 0 if there was no acknowledge error.
acknowledge_error_message	
The acknowledge error message, or null if there was no acknowledge error.
current_leader	
The current leader of the partition.
leader_id	
The ID of the current leader or -1 if the leader is unknown.
leader_epoch	
The latest known leader epoch.
records	
The record data.
acquired_records	
The acquired records.
first_offset	
The earliest offset in this batch of acquired records.
last_offset	
The last offset of this batch of acquired records.
delivery_count	
The delivery count of this batch of acquired records.
node_endpoints	
Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
node_id	
The ID of the associated node.
host	
The node's hostname.
port	
The node's port.
rack	
The rack of the node, or null if it has not been assigned to a rack.
ShareFetch Response (Version: 2) => throttle_time_ms error_code error_message acquisition_lock_timeout_ms [responses] [node_endpoints] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  acquisition_lock_timeout_ms => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code error_message acknowledge_error_code acknowledge_error_message current_leader records [acquired_records] 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      acknowledge_error_code => INT16
      acknowledge_error_message => COMPACT_NULLABLE_STRING
      current_leader => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
      records => COMPACT_RECORDS
      acquired_records => first_offset last_offset delivery_count 
        first_offset => INT64
        last_offset => INT64
        delivery_count => INT16
  node_endpoints => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level response error code.
error_message	
The top-level error message, or null if there was no error.
acquisition_lock_timeout_ms	
The time in milliseconds for which the acquired records are locked.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The fetch error code, or 0 if there was no fetch error.
error_message	
The fetch error message, or null if there was no fetch error.
acknowledge_error_code	
The acknowledge error code, or 0 if there was no acknowledge error.
acknowledge_error_message	
The acknowledge error message, or null if there was no acknowledge error.
current_leader	
The current leader of the partition.
leader_id	
The ID of the current leader or -1 if the leader is unknown.
leader_epoch	
The latest known leader epoch.
records	
The record data.
acquired_records	
The acquired records.
first_offset	
The earliest offset in this batch of acquired records.
last_offset	
The last offset of this batch of acquired records.
delivery_count	
The delivery count of this batch of acquired records.
node_endpoints	
Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
node_id	
The ID of the associated node.
host	
The node's hostname.
port	
The node's port.
rack	
The rack of the node, or null if it has not been assigned to a rack.
ShareAcknowledge API (Key: 79):
Requests:

ShareAcknowledge Request (Version: 1) => group_id member_id share_session_epoch [topics] 
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index [acknowledgement_batches] 
      partition_index => INT32
      acknowledgement_batches => first_offset last_offset [acknowledge_types] 
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member ID.
share_session_epoch	
The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
topics	
The topics containing records to acknowledge.
topic_id	
The unique topic ID.
partitions	
The partitions containing records to acknowledge.
partition_index	
The partition index.
acknowledgement_batches	
Record batches to acknowledge.
first_offset	
First offset of batch of records to acknowledge.
last_offset	
Last offset (inclusive) of batch of records to acknowledge.
acknowledge_types	
Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
ShareAcknowledge Request (Version: 2) => group_id member_id share_session_epoch is_renew_ack [topics] 
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  is_renew_ack => BOOLEAN
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index [acknowledgement_batches] 
      partition_index => INT32
      acknowledgement_batches => first_offset last_offset [acknowledge_types] 
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member ID.
share_session_epoch	
The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
is_renew_ack	
Whether Renew type acknowledgements present in AcknowledgementBatches.
topics	
The topics containing records to acknowledge.
topic_id	
The unique topic ID.
partitions	
The partitions containing records to acknowledge.
partition_index	
The partition index.
acknowledgement_batches	
Record batches to acknowledge.
first_offset	
First offset of batch of records to acknowledge.
last_offset	
Last offset (inclusive) of batch of records to acknowledge.
acknowledge_types	
Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
Responses:

ShareAcknowledge Response (Version: 1) => throttle_time_ms error_code error_message [responses] [node_endpoints] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code error_message current_leader 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      current_leader => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
  node_endpoints => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
error_message	
The top-level error message, or null if there was no error.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
current_leader	
The current leader of the partition.
leader_id	
The ID of the current leader or -1 if the leader is unknown.
leader_epoch	
The latest known leader epoch.
node_endpoints	
Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
node_id	
The ID of the associated node.
host	
The node's hostname.
port	
The node's port.
rack	
The rack of the node, or null if it has not been assigned to a rack.
ShareAcknowledge Response (Version: 2) => throttle_time_ms error_code error_message acquisition_lock_timeout_ms [responses] [node_endpoints] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  acquisition_lock_timeout_ms => INT32
  responses => topic_id [partitions] 
    topic_id => UUID
    partitions => partition_index error_code error_message current_leader 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      current_leader => leader_id leader_epoch 
        leader_id => INT32
        leader_epoch => INT32
  node_endpoints => node_id host port rack 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top level response error code.
error_message	
The top-level error message, or null if there was no error.
acquisition_lock_timeout_ms	
The time in milliseconds for which the acquired records are locked.
responses	
The response topics.
topic_id	
The unique topic ID.
partitions	
The topic partitions.
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
current_leader	
The current leader of the partition.
leader_id	
The ID of the current leader or -1 if the leader is unknown.
leader_epoch	
The latest known leader epoch.
node_endpoints	
Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
node_id	
The ID of the associated node.
host	
The node's hostname.
port	
The node's port.
rack	
The rack of the node, or null if it has not been assigned to a rack.
AddRaftVoter API (Key: 80):
Requests:

AddRaftVoter Request (Version: 0) => cluster_id timeout_ms voter_id voter_directory_id [listeners] 
  cluster_id => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  voter_id => INT32
  voter_directory_id => UUID
  listeners => name host port 
    name => COMPACT_STRING
    host => COMPACT_STRING
    port => UINT16
Request header version: 2

Field	
Description
cluster_id	
The cluster id.
timeout_ms	
The maximum time to wait for the request to complete before returning.
voter_id	
The replica id of the voter getting added to the topic partition.
voter_directory_id	
The directory id of the voter getting added to the topic partition.
listeners	
The endpoints that can be used to communicate with the voter.
name	
The name of the endpoint.
host	
The hostname.
port	
The port.
AddRaftVoter Request (Version: 1) => cluster_id timeout_ms voter_id voter_directory_id [listeners] ack_when_committed 
  cluster_id => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  voter_id => INT32
  voter_directory_id => UUID
  listeners => name host port 
    name => COMPACT_STRING
    host => COMPACT_STRING
    port => UINT16
  ack_when_committed => BOOLEAN
Request header version: 2

Field	
Description
cluster_id	
The cluster id.
timeout_ms	
The maximum time to wait for the request to complete before returning.
voter_id	
The replica id of the voter getting added to the topic partition.
voter_directory_id	
The directory id of the voter getting added to the topic partition.
listeners	
The endpoints that can be used to communicate with the voter.
name	
The name of the endpoint.
host	
The hostname.
port	
The port.
ack_when_committed	
When true, return a response after the new voter set is committed. Otherwise, return after the leader writes the changes locally.
Responses:

AddRaftVoter Response (Version: 0) => throttle_time_ms error_code error_message 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
AddRaftVoter Response (Version: 1) => throttle_time_ms error_code error_message 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
RemoveRaftVoter API (Key: 81):
Requests:

RemoveRaftVoter Request (Version: 0) => cluster_id voter_id voter_directory_id 
  cluster_id => COMPACT_NULLABLE_STRING
  voter_id => INT32
  voter_directory_id => UUID
Request header version: 2

Field	
Description
cluster_id	
The cluster id of the request.
voter_id	
The replica id of the voter getting removed from the topic partition.
voter_directory_id	
The directory id of the voter getting removed from the topic partition.
Responses:

RemoveRaftVoter Response (Version: 0) => throttle_time_ms error_code error_message 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
InitializeShareGroupState API (Key: 83):
Requests:

InitializeShareGroupState Request (Version: 0) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition state_epoch start_offset 
      partition => INT32
      state_epoch => INT32
      start_offset => INT64
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The data for the topics.
topic_id	
The topic identifier.
partitions	
The data for the partitions.
partition	
The partition index.
state_epoch	
The state epoch for this share-partition.
start_offset	
The share-partition start offset, or -1 if the start offset is not being initialized.
Responses:

InitializeShareGroupState Response (Version: 0) => [results] 
  results => topic_id [partitions] 
    topic_id => UUID
    partitions => partition error_code error_message 
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
results	
The initialization results.
topic_id	
The topic identifier.
partitions	
The results for the partitions.
partition	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
ReadShareGroupState API (Key: 84):
Requests:

ReadShareGroupState Request (Version: 0) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition leader_epoch 
      partition => INT32
      leader_epoch => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The data for the topics.
topic_id	
The topic identifier.
partitions	
The data for the partitions.
partition	
The partition index.
leader_epoch	
The leader epoch of the share-partition.
Responses:

ReadShareGroupState Response (Version: 0) => [results] 
  results => topic_id [partitions] 
    topic_id => UUID
    partitions => partition error_code error_message state_epoch start_offset [state_batches] 
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      state_epoch => INT32
      start_offset => INT64
      state_batches => first_offset last_offset delivery_state delivery_count 
        first_offset => INT64
        last_offset => INT64
        delivery_state => INT8
        delivery_count => INT16
Response header version: 1

Field	
Description
results	
The read results.
topic_id	
The topic identifier.
partitions	
The results for the partitions.
partition	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
state_epoch	
The state epoch of the share-partition.
start_offset	
The share-partition start offset, which can be -1 if it is not yet initialized.
state_batches	
The state batches for this share-partition.
first_offset	
The first offset of this state batch.
last_offset	
The last offset of this state batch.
delivery_state	
The delivery state - 0:Available,2:Acked,4:Archived.
delivery_count	
The delivery count.
WriteShareGroupState API (Key: 85):
Requests:

WriteShareGroupState Request (Version: 0) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition state_epoch leader_epoch start_offset [state_batches] 
      partition => INT32
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
      state_batches => first_offset last_offset delivery_state delivery_count 
        first_offset => INT64
        last_offset => INT64
        delivery_state => INT8
        delivery_count => INT16
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The data for the topics.
topic_id	
The topic identifier.
partitions	
The data for the partitions.
partition	
The partition index.
state_epoch	
The state epoch of the share-partition.
leader_epoch	
The leader epoch of the share-partition.
start_offset	
The share-partition start offset, or -1 if the start offset is not being written.
state_batches	
The state batches for the share-partition.
first_offset	
The first offset of this state batch.
last_offset	
The last offset of this state batch.
delivery_state	
The delivery state - 0:Available,2:Acked,4:Archived.
delivery_count	
The delivery count.
WriteShareGroupState Request (Version: 1) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition state_epoch leader_epoch start_offset delivery_complete_count [state_batches] 
      partition => INT32
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
      delivery_complete_count => INT32
      state_batches => first_offset last_offset delivery_state delivery_count 
        first_offset => INT64
        last_offset => INT64
        delivery_state => INT8
        delivery_count => INT16
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The data for the topics.
topic_id	
The topic identifier.
partitions	
The data for the partitions.
partition	
The partition index.
state_epoch	
The state epoch of the share-partition.
leader_epoch	
The leader epoch of the share-partition.
start_offset	
The share-partition start offset, or -1 if the start offset is not being written.
delivery_complete_count	
The number of offsets greater than or equal to share-partition start offset for which delivery has been completed.
state_batches	
The state batches for the share-partition.
first_offset	
The first offset of this state batch.
last_offset	
The last offset of this state batch.
delivery_state	
The delivery state - 0:Available,2:Acked,4:Archived.
delivery_count	
The delivery count.
Responses:

WriteShareGroupState Response (Version: 0) => [results] 
  results => topic_id [partitions] 
    topic_id => UUID
    partitions => partition error_code error_message 
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
results	
The write results.
topic_id	
The topic identifier.
partitions	
The results for the partitions.
partition	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
WriteShareGroupState Response (Version: 1) => [results] 
  results => topic_id [partitions] 
    topic_id => UUID
    partitions => partition error_code error_message 
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
results	
The write results.
topic_id	
The topic identifier.
partitions	
The results for the partitions.
partition	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
DeleteShareGroupState API (Key: 86):
Requests:

DeleteShareGroupState Request (Version: 0) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition 
      partition => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The data for the topics.
topic_id	
The topic identifier.
partitions	
The data for the partitions.
partition	
The partition index.
Responses:

DeleteShareGroupState Response (Version: 0) => [results] 
  results => topic_id [partitions] 
    topic_id => UUID
    partitions => partition error_code error_message 
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
results	
The delete results.
topic_id	
The topic identifier.
partitions	
The results for the partitions.
partition	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
ReadShareGroupStateSummary API (Key: 87):
Requests:

ReadShareGroupStateSummary Request (Version: 0) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition leader_epoch 
      partition => INT32
      leader_epoch => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The data for the topics.
topic_id	
The topic identifier.
partitions	
The data for the partitions.
partition	
The partition index.
leader_epoch	
The leader epoch of the share-partition.
ReadShareGroupStateSummary Request (Version: 1) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_id [partitions] 
    topic_id => UUID
    partitions => partition leader_epoch 
      partition => INT32
      leader_epoch => INT32
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The data for the topics.
topic_id	
The topic identifier.
partitions	
The data for the partitions.
partition	
The partition index.
leader_epoch	
The leader epoch of the share-partition.
Responses:

ReadShareGroupStateSummary Response (Version: 0) => [results] 
  results => topic_id [partitions] 
    topic_id => UUID
    partitions => partition error_code error_message state_epoch leader_epoch start_offset 
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
Response header version: 1

Field	
Description
results	
The read results.
topic_id	
The topic identifier.
partitions	
The results for the partitions.
partition	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
state_epoch	
The state epoch of the share-partition.
leader_epoch	
The leader epoch of the share-partition.
start_offset	
The share-partition start offset.
ReadShareGroupStateSummary Response (Version: 1) => [results] 
  results => topic_id [partitions] 
    topic_id => UUID
    partitions => partition error_code error_message state_epoch leader_epoch start_offset delivery_complete_count 
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
      delivery_complete_count => INT32
Response header version: 1

Field	
Description
results	
The read results.
topic_id	
The topic identifier.
partitions	
The results for the partitions.
partition	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
state_epoch	
The state epoch of the share-partition.
leader_epoch	
The leader epoch of the share-partition.
start_offset	
The share-partition start offset.
delivery_complete_count	
The number of offsets greater than or equal to share-partition start offset for which delivery has been completed.
StreamsGroupHeartbeat API (Key: 88):
Requests:

StreamsGroupHeartbeat Request (Version: 0) => group_id member_id member_epoch endpoint_information_epoch instance_id rack_id rebalance_timeout_ms topology [active_tasks] [standby_tasks] [warmup_tasks] process_id user_endpoint [client_tags] [task_offsets] [task_end_offsets] shutdown_application 
  group_id => COMPACT_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  endpoint_information_epoch => INT32
  instance_id => COMPACT_NULLABLE_STRING
  rack_id => COMPACT_NULLABLE_STRING
  rebalance_timeout_ms => INT32
  topology => epoch [subtopologies] 
    epoch => INT32
    subtopologies => subtopology_id [source_topics] [source_topic_regex] [state_changelog_topics] [repartition_sink_topics] [repartition_source_topics] [copartition_groups] 
      subtopology_id => COMPACT_STRING
      source_topics => COMPACT_STRING
      source_topic_regex => COMPACT_STRING
      state_changelog_topics => name partitions replication_factor [topic_configs] 
        name => COMPACT_STRING
        partitions => INT32
        replication_factor => INT16
        topic_configs => key value 
          key => COMPACT_STRING
          value => COMPACT_STRING
      repartition_sink_topics => COMPACT_STRING
      repartition_source_topics => name partitions replication_factor [topic_configs] 
        name => COMPACT_STRING
        partitions => INT32
        replication_factor => INT16
        topic_configs => key value 
          key => COMPACT_STRING
          value => COMPACT_STRING
      copartition_groups => [source_topics] [source_topic_regex] [repartition_source_topics] 
        source_topics => INT16
        source_topic_regex => INT16
        repartition_source_topics => INT16
  active_tasks => subtopology_id [partitions] 
    subtopology_id => COMPACT_STRING
    partitions => INT32
  standby_tasks => subtopology_id [partitions] 
    subtopology_id => COMPACT_STRING
    partitions => INT32
  warmup_tasks => subtopology_id [partitions] 
    subtopology_id => COMPACT_STRING
    partitions => INT32
  process_id => COMPACT_NULLABLE_STRING
  user_endpoint => host port 
    host => COMPACT_STRING
    port => UINT16
  client_tags => key value 
    key => COMPACT_STRING
    value => COMPACT_STRING
  task_offsets => subtopology_id partition offset 
    subtopology_id => COMPACT_STRING
    partition => INT32
    offset => INT64
  task_end_offsets => subtopology_id partition offset 
    subtopology_id => COMPACT_STRING
    partition => INT32
    offset => INT64
  shutdown_application => BOOLEAN
Request header version: 2

Field	
Description
group_id	
The group identifier.
member_id	
The member ID generated by the streams consumer. The member ID must be kept during the entire lifetime of the streams consumer process.
member_epoch	
The current member epoch; 0 to join the group; -1 to leave the group; -2 to indicate that the static member will rejoin.
endpoint_information_epoch	
The current endpoint epoch of this client, represents the latest endpoint epoch this client received
instance_id	
null if not provided or if it didn't change since the last heartbeat; the instance ID for static membership otherwise.
rack_id	
null if not provided or if it didn't change since the last heartbeat; the rack ID of the member otherwise.
rebalance_timeout_ms	
-1 if it didn't change since the last heartbeat; the maximum time in milliseconds that the coordinator will wait on the member to revoke its tasks otherwise.
topology	
The topology metadata of the streams application. Used to initialize the topology of the group and to check if the topology corresponds to the topology initialized for the group. Only sent when memberEpoch = 0, must be non-empty. Null otherwise.
epoch	
The epoch of the topology. Used to check if the topology corresponds to the topology initialized on the brokers.
subtopologies	
The sub-topologies of the streams application.
subtopology_id	
String to uniquely identify the subtopology. Deterministically generated from the topology
source_topics	
The topics the topology reads from.
source_topic_regex	
The regular expressions identifying topics the subtopology reads from.
state_changelog_topics	
The set of state changelog topics associated with this subtopology. Created automatically.
name	
The name of the topic.
partitions	
The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
replication_factor	
The replication factor of the topic. Can be 0 if the default replication factor should be used.
topic_configs	
Topic-level configurations as key-value pairs.
key	
key of the config
value	
value of the config
repartition_sink_topics	
The repartition topics the subtopology writes to.
repartition_source_topics	
The set of source topics that are internally created repartition topics. Created automatically.
copartition_groups	
A subset of source topics that must be copartitioned.
source_topics	
The topics the topology reads from. Index into the array on the subtopology level.
source_topic_regex	
Regular expressions identifying topics the subtopology reads from. Index into the array on the subtopology level.
repartition_source_topics	
The set of source topics that are internally created repartition topics. Index into the array on the subtopology level.
active_tasks	
Currently owned active tasks for this client. Null if unchanged since last heartbeat.
subtopology_id	
The subtopology identifier.
partitions	
The partitions of the input topics processed by this member.
standby_tasks	
Currently owned standby tasks for this client. Null if unchanged since last heartbeat.
warmup_tasks	
Currently owned warm-up tasks for this client. Null if unchanged since last heartbeat.
process_id	
Identity of the streams instance that may have multiple consumers. Null if unchanged since last heartbeat.
user_endpoint	
User-defined endpoint for Interactive Queries. Null if unchanged since last heartbeat, or if not defined on the client.
host	
host of the endpoint
port	
port of the endpoint
client_tags	
Used for rack-aware assignment algorithm. Null if unchanged since last heartbeat.
task_offsets	
Cumulative changelog offsets for tasks. Only updated when a warm-up task has caught up, and according to the task offset interval. Null if unchanged since last heartbeat.
subtopology_id	
The subtopology identifier.
partition	
The partition.
offset	
The offset.
task_end_offsets	
Cumulative changelog end-offsets for tasks. Only updated when a warm-up task has caught up, and according to the task offset interval. Null if unchanged since last heartbeat.
shutdown_application	
Whether all Streams clients in the group should shut down.
Responses:

StreamsGroupHeartbeat Response (Version: 0) => throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms acceptable_recovery_lag task_offset_interval_ms [status] [active_tasks] [standby_tasks] [warmup_tasks] endpoint_information_epoch [partitions_by_user_endpoint] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  heartbeat_interval_ms => INT32
  acceptable_recovery_lag => INT32
  task_offset_interval_ms => INT32
  status => status_code status_detail 
    status_code => INT8
    status_detail => COMPACT_STRING
  active_tasks => subtopology_id [partitions] 
    subtopology_id => COMPACT_STRING
    partitions => INT32
  standby_tasks => subtopology_id [partitions] 
    subtopology_id => COMPACT_STRING
    partitions => INT32
  warmup_tasks => subtopology_id [partitions] 
    subtopology_id => COMPACT_STRING
    partitions => INT32
  endpoint_information_epoch => INT32
  partitions_by_user_endpoint => user_endpoint [active_partitions] [standby_partitions] 
    user_endpoint => host port 
      host => COMPACT_STRING
      port => UINT16
    active_partitions => topic [partitions] 
      topic => COMPACT_STRING
      partitions => INT32
    standby_partitions => topic [partitions] 
      topic => COMPACT_STRING
      partitions => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error
error_message	
The top-level error message, or null if there was no error.
member_id	
The member id is always generated by the streams consumer.
member_epoch	
The member epoch.
heartbeat_interval_ms	
The heartbeat interval in milliseconds.
acceptable_recovery_lag	
The maximal lag a warm-up task can have to be considered caught-up.
task_offset_interval_ms	
The interval in which the task changelog offsets on a client are updated on the broker. The offsets are sent with the next heartbeat after this time has passed.
status	
Indicate zero or more status for the group.
status_code	
A code to indicate that a particular status is active for the group membership
status_detail	
A string representation of the status.
active_tasks	
Assigned active tasks for this client. Null if unchanged since last heartbeat.
subtopology_id	
The subtopology identifier.
partitions	
The partitions of the input topics processed by this member.
standby_tasks	
Assigned standby tasks for this client. Null if unchanged since last heartbeat.
warmup_tasks	
Assigned warm-up tasks for this client. Null if unchanged since last heartbeat.
endpoint_information_epoch	
The endpoint epoch set in the response
partitions_by_user_endpoint	
Global assignment information used for IQ. Null if unchanged since last heartbeat.
user_endpoint	
User-defined endpoint to connect to the node
host	
host of the endpoint
port	
port of the endpoint
active_partitions	
All topic partitions materialized by active tasks on the node
topic	
topic name
partitions	
partitions
standby_partitions	
All topic partitions materialized by standby tasks on the node
StreamsGroupDescribe API (Key: 89):
Requests:

StreamsGroupDescribe Request (Version: 0) => [group_ids] include_authorized_operations 
  group_ids => COMPACT_STRING
  include_authorized_operations => BOOLEAN
Request header version: 2

Field	
Description
group_ids	
The ids of the groups to describe
include_authorized_operations	
Whether to include authorized operations.
Responses:

StreamsGroupDescribe Response (Version: 0) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => error_code error_message group_id group_state group_epoch assignment_epoch topology [members] authorized_operations 
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    group_epoch => INT32
    assignment_epoch => INT32
    topology => epoch [subtopologies] 
      epoch => INT32
      subtopologies => subtopology_id [source_topics] [repartition_sink_topics] [state_changelog_topics] [repartition_source_topics] 
        subtopology_id => COMPACT_STRING
        source_topics => COMPACT_STRING
        repartition_sink_topics => COMPACT_STRING
        state_changelog_topics => name partitions replication_factor [topic_configs] 
          name => COMPACT_STRING
          partitions => INT32
          replication_factor => INT16
          topic_configs => key value 
            key => COMPACT_STRING
            value => COMPACT_STRING
        repartition_source_topics => name partitions replication_factor [topic_configs] 
          name => COMPACT_STRING
          partitions => INT32
          replication_factor => INT16
          topic_configs => key value 
            key => COMPACT_STRING
            value => COMPACT_STRING
    members => member_id member_epoch instance_id rack_id client_id client_host topology_epoch process_id user_endpoint [client_tags] [task_offsets] [task_end_offsets] assignment target_assignment is_classic 
      member_id => COMPACT_STRING
      member_epoch => INT32
      instance_id => COMPACT_NULLABLE_STRING
      rack_id => COMPACT_NULLABLE_STRING
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      topology_epoch => INT32
      process_id => COMPACT_STRING
      user_endpoint => host port 
        host => COMPACT_STRING
        port => UINT16
      client_tags => key value 
        key => COMPACT_STRING
        value => COMPACT_STRING
      task_offsets => subtopology_id partition offset 
        subtopology_id => COMPACT_STRING
        partition => INT32
        offset => INT64
      task_end_offsets => subtopology_id partition offset 
        subtopology_id => COMPACT_STRING
        partition => INT32
        offset => INT64
      assignment => [active_tasks] [standby_tasks] [warmup_tasks] 
        active_tasks => subtopology_id [partitions] 
          subtopology_id => COMPACT_STRING
          partitions => INT32
        standby_tasks => subtopology_id [partitions] 
          subtopology_id => COMPACT_STRING
          partitions => INT32
        warmup_tasks => subtopology_id [partitions] 
          subtopology_id => COMPACT_STRING
          partitions => INT32
      target_assignment => [active_tasks] [standby_tasks] [warmup_tasks] 
        active_tasks => subtopology_id [partitions] 
          subtopology_id => COMPACT_STRING
          partitions => INT32
        standby_tasks => subtopology_id [partitions] 
          subtopology_id => COMPACT_STRING
          partitions => INT32
        warmup_tasks => subtopology_id [partitions] 
          subtopology_id => COMPACT_STRING
          partitions => INT32
      is_classic => BOOLEAN
    authorized_operations => INT32
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
Each described group.
error_code	
The describe error, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
group_id	
The group ID string.
group_state	
The group state string, or the empty string.
group_epoch	
The group epoch.
assignment_epoch	
The assignment epoch.
topology	
The topology metadata currently initialized for the streams application. Can be null in case of a describe error.
epoch	
The epoch of the currently initialized topology for this group.
subtopologies	
The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
subtopology_id	
String to uniquely identify the subtopology.
source_topics	
The topics the subtopology reads from.
repartition_sink_topics	
The repartition topics the subtopology writes to.
state_changelog_topics	
The set of state changelog topics associated with this subtopology. Created automatically.
name	
The name of the topic.
partitions	
The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
replication_factor	
The replication factor of the topic. Can be 0 if the default replication factor should be used.
topic_configs	
Topic-level configurations as key-value pairs.
key	
key of the config
value	
value of the config
repartition_source_topics	
The set of source topics that are internally created repartition topics. Created automatically.
members	
The members.
member_id	
The member ID.
member_epoch	
The member epoch.
instance_id	
The member instance ID for static membership.
rack_id	
The rack ID.
client_id	
The client ID.
client_host	
The client host.
topology_epoch	
The epoch of the topology on the client.
process_id	
Identity of the streams instance that may have multiple clients.
user_endpoint	
User-defined endpoint for Interactive Queries. Null if not defined for this client.
host	
host of the endpoint
port	
port of the endpoint
client_tags	
Used for rack-aware assignment algorithm.
task_offsets	
Cumulative changelog offsets for tasks.
subtopology_id	
The subtopology identifier.
partition	
The partition.
offset	
The offset.
task_end_offsets	
Cumulative changelog end offsets for tasks.
assignment	
The current assignment.
active_tasks	
Active tasks for this client.
subtopology_id	
The subtopology identifier.
partitions	
The partitions of the input topics processed by this member.
standby_tasks	
Standby tasks for this client.
warmup_tasks	
Warm-up tasks for this client.
target_assignment	
The target assignment.
is_classic	
True for classic members that have not been upgraded yet.
authorized_operations	
32-bit bitfield to represent authorized operations for this group.
DescribeShareGroupOffsets API (Key: 90):
Requests:

DescribeShareGroupOffsets Request (Version: 0) => [groups] 
  groups => group_id [topics] 
    group_id => COMPACT_STRING
    topics => topic_name [partitions] 
      topic_name => COMPACT_STRING
      partitions => INT32
Request header version: 2

Field	
Description
groups	
The groups to describe offsets for.
group_id	
The group identifier.
topics	
The topics to describe offsets for, or null for all topic-partitions.
topic_name	
The topic name.
partitions	
The partitions.
DescribeShareGroupOffsets Request (Version: 1) => [groups] 
  groups => group_id [topics] 
    group_id => COMPACT_STRING
    topics => topic_name [partitions] 
      topic_name => COMPACT_STRING
      partitions => INT32
Request header version: 2

Field	
Description
groups	
The groups to describe offsets for.
group_id	
The group identifier.
topics	
The topics to describe offsets for, or null for all topic-partitions.
topic_name	
The topic name.
partitions	
The partitions.
Responses:

DescribeShareGroupOffsets Response (Version: 0) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => group_id [topics] error_code error_message 
    group_id => COMPACT_STRING
    topics => topic_name topic_id [partitions] 
      topic_name => COMPACT_STRING
      topic_id => UUID
      partitions => partition_index start_offset leader_epoch error_code error_message 
        partition_index => INT32
        start_offset => INT64
        leader_epoch => INT32
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
The results for each group.
group_id	
The group identifier.
topics	
The results for each topic.
topic_name	
The topic name.
topic_id	
The unique topic ID.
partitions	
partition_index	
The partition index.
start_offset	
The share-partition start offset.
leader_epoch	
The leader epoch of the partition.
error_code	
The partition-level error code, or 0 if there was no error.
error_message	
The partition-level error message, or null if there was no error.
error_code	
The group-level error code, or 0 if there was no error.
error_message	
The group-level error message, or null if there was no error.
DescribeShareGroupOffsets Response (Version: 1) => throttle_time_ms [groups] 
  throttle_time_ms => INT32
  groups => group_id [topics] error_code error_message 
    group_id => COMPACT_STRING
    topics => topic_name topic_id [partitions] 
      topic_name => COMPACT_STRING
      topic_id => UUID
      partitions => partition_index start_offset leader_epoch lag error_code error_message 
        partition_index => INT32
        start_offset => INT64
        leader_epoch => INT32
        lag => INT64
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
groups	
The results for each group.
group_id	
The group identifier.
topics	
The results for each topic.
topic_name	
The topic name.
topic_id	
The unique topic ID.
partitions	
partition_index	
The partition index.
start_offset	
The share-partition start offset.
leader_epoch	
The leader epoch of the partition.
lag	
The share-partition lag.
error_code	
The partition-level error code, or 0 if there was no error.
error_message	
The partition-level error message, or null if there was no error.
error_code	
The group-level error code, or 0 if there was no error.
error_message	
The group-level error message, or null if there was no error.
AlterShareGroupOffsets API (Key: 91):
Requests:

AlterShareGroupOffsets Request (Version: 0) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_name [partitions] 
    topic_name => COMPACT_STRING
    partitions => partition_index start_offset 
      partition_index => INT32
      start_offset => INT64
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The topics to alter offsets for.
topic_name	
The topic name.
partitions	
Each partition to alter offsets for.
partition_index	
The partition index.
start_offset	
The share-partition start offset.
Responses:

AlterShareGroupOffsets Response (Version: 0) => throttle_time_ms error_code error_message [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => topic_name topic_id [partitions] 
    topic_name => COMPACT_STRING
    topic_id => UUID
    partitions => partition_index error_code error_message 
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
responses	
The results for each topic.
topic_name	
The topic name.
topic_id	
The unique topic ID.
partitions	
partition_index	
The partition index.
error_code	
The error code, or 0 if there was no error.
error_message	
The error message, or null if there was no error.
DeleteShareGroupOffsets API (Key: 92):
Requests:

DeleteShareGroupOffsets Request (Version: 0) => group_id [topics] 
  group_id => COMPACT_STRING
  topics => topic_name 
    topic_name => COMPACT_STRING
Request header version: 2

Field	
Description
group_id	
The group identifier.
topics	
The topics to delete offsets for.
topic_name	
The topic name.
Responses:

DeleteShareGroupOffsets Response (Version: 0) => throttle_time_ms error_code error_message [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => topic_name topic_id error_code error_message 
    topic_name => COMPACT_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
Response header version: 1

Field	
Description
throttle_time_ms	
The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
error_code	
The top-level error code, or 0 if there was no error.
error_message	
The top-level error message, or null if there was no error.
responses	
The results for each topic.
topic_name	
The topic name.
topic_id	
The unique topic ID.
error_code	
The topic-level error code, or 0 if there was no error.
error_message	
The topic-level error message, or null if there was no error.
Some Common Philosophical Questions
Some people have asked why we don’t use HTTP. There are a number of reasons, the best is that client implementors can make use of some of the more advanced TCP features–the ability to multiplex requests, the ability to simultaneously poll many connections, etc. We have also found HTTP libraries in many languages to be surprisingly shabby.

Others have asked if maybe we shouldn’t support many different protocols. Prior experience with this was that it makes it very hard to add and test new features if they have to be ported across many protocol implementations. Our feeling is that most users don’t really see multiple protocols as a feature, they just want a good reliable client in the language of their choice.

Another question is why we don’t adopt XMPP, STOMP, AMQP or an existing protocol. The answer to this varies by protocol, but in general the problem is that the protocol does determine large parts of the implementation and we couldn’t do what we are doing if we didn’t have control over the protocol. Our belief is that it is possible to do better than existing messaging systems have in providing a truly distributed messaging system, and to do this we need to build something that works differently.

A final question is why we don’t use a system like Protocol Buffers or Thrift to define our request messages. These packages excel at helping you to managing lots and lots of serialized messages. However we have only a few messages. Support across languages is somewhat spotty (depending on the package). Finally the mapping between binary log format and wire protocol is something we manage somewhat carefully and this would not be possible with these systems. Finally we prefer the style of versioning APIs explicitly and checking this to inferring new values as nulls as it allows more nuanced control of compatibility.
