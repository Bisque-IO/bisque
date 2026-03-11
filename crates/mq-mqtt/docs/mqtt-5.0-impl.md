# MQTT 5.0 Implementation — Gap Analysis & Implementation Plan

**Date**: 2026-03-11
**Spec**: [MQTT Version 5.0 (OASIS Standard)](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
**Crate**: `bisque-mq-mqtt` (`crates/mq-mqtt/src/`)

---

## 1. Coverage Summary

| Area | Status | Notes |
|------|--------|-------|
| **Packet Types (15/15)** | COMPLETE | All packet types encode/decode correctly |
| **QoS 0/1/2** | COMPLETE | Full 4-way handshake for QoS 2, duplicate detection |
| **Properties (25/25)** | COMPLETE | All MQTT 5.0 properties parsed and emitted |
| **Will Messages** | COMPLETE | Will properties, will delay interval, retain |
| **Retained Messages** | COMPLETE | Store, clear-on-empty, retain handling 0/1/2 |
| **Topic Wildcards (+, #)** | COMPLETE | SIMD-accelerated validation, $ topic filtering |
| **Shared Subscriptions** | COMPLETE | `$share/group/filter` with round-robin delivery |
| **Topic Aliases** | COMPLETE | Bidirectional alias maps with zero-copy optimization |
| **Subscription Identifiers** | COMPLETE | Per-subscription tracking, included in outbound PUBLISH |
| **Request/Response** | COMPLETE | response_topic, correlation_data, response_information |
| **Enhanced Auth (SASL)** | COMPLETE | Challenge-response flow, re-authentication support |
| **Session Persistence** | COMPLETE | InMemorySessionStore with DashMap |
| **Keep Alive (1.5x)** | COMPLETE | Correct timeout detection and disconnect |
| **Message Expiry** | COMPLETE | TTL stored and reconstructed on delivery |
| **UTF-8 Validation** | COMPLETE | Disallowed Unicode code points rejected per 1.5.4/5.4.9 |
| **$ Topic Isolation** | COMPLETE | `$`-prefixed topics excluded from `#`/`+` wildcards |
| **Password w/o Username (V5)** | COMPLETE | V3.1.1 rejects, V5 allows per spec |
| **Server Shutdown (0x8B)** | COMPLETE | DISCONNECT with reason code supported |
| **Outbound Flow Control** | COMPLETE | `is_inflight_full()` enforces client receive_maximum |
| **Zero-Copy Codec** | COMPLETE | Bytes::slice, mmap FlatMessage integration |

---

## 2. Gaps Identified

### 2.1 Critical — Protocol Compliance

#### GAP-1: Session Takeover [Spec 3.1.4, 3.14.2.1]
**Severity**: Critical
**Spec Requirement**: When a new CONNECT arrives with the same ClientID as an existing active session, the server MUST:
1. Send DISCONNECT with reason code 0x8E (Session taken over) to the **old** connection
2. Close the old connection
3. Resume or create session for the new connection

**Current Behavior**: Server rejects duplicate ClientID with PROTOCOL_ERROR (0x82) — treats it as a protocol violation instead of performing takeover.

**Missing Reason Codes**: 0x8E (Session taken over)

**Impact**: Clients that reconnect without cleanly disconnecting first are permanently locked out until the old TCP connection times out.

---

#### GAP-2: Inbound Receive Maximum Enforcement [Spec 4.9, 3.3.4]
**Severity**: Critical
**Spec Requirement**: Server announces `receive_maximum` in CONNACK. If the client exceeds this by sending more concurrent QoS 1/2 PUBLISH packets than allowed, the server MUST disconnect with reason 0x93 (Receive Maximum exceeded).

**Current Behavior**: `receive_maximum` is announced in CONNACK but **never enforced** on inbound publishes. The server accepts unlimited concurrent QoS 1/2 packets.

**Missing Reason Codes**: 0x93 (Receive Maximum exceeded)

**Impact**: No protection against misbehaving clients flooding QoS 1/2 publishes.

---

#### GAP-3: Session Expiry Update on DISCONNECT [Spec 3.14.2.2]
**Severity**: Critical
**Spec Requirement**: Client MAY include `session_expiry_interval` in DISCONNECT to update the session expiry. Server MUST NOT allow changing from 0 to non-zero (protocol error 0x82).

**Current Behavior**: `handle_disconnect()` does not extract or apply `session_expiry_interval` from the DISCONNECT properties. The value from CONNECT is always used.

**Impact**: Clients cannot adjust session lifetime on disconnect. No protection against invalid 0-to-non-zero transitions.

---

### 2.2 High — Behavioral Compliance

#### GAP-4: No Local on Shared Subscriptions [Spec 3.8.3.1]
**Severity**: High
**Spec Requirement**: `no_local` option MUST NOT be set to 1 on a Shared Subscription. If set, server MUST respond with an error reason code.

**Current Behavior**: `no_local` flag is stored but never validated against shared subscriptions. A `$share/g/topic` subscription with `no_local=1` is silently accepted.

**Impact**: Silent spec violation; could cause unexpected message delivery behavior.

---

#### GAP-5: Request Problem Information Suppression [Spec 3.1.2.11.7]
**Severity**: High
**Spec Requirement**: When `request_problem_information=0` in CONNECT, server MUST NOT include Reason String or User Properties in CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, or DISCONNECT (except CONNACK on failure and AUTH).

**Current Behavior**: Property is parsed and stored but never checked when building response packets. Reason strings and user properties are always included if present.

**Impact**: Bandwidth waste and spec violation for constrained clients that explicitly opted out.

---

#### GAP-6: Will Delay Timer Management [Spec 3.1.3.9, 3.1.2.5]
**Severity**: High
**Spec Requirement**: When will_delay_interval > 0:
1. Server MUST delay will publication by the specified seconds after disconnect
2. If client reconnects within the delay, will MUST be cancelled
3. If session expires before will delay, will MUST be published immediately

**Current Behavior**: Will delay is encoded as `delay_ms` in FlatMessage and delegated to bisque-mq. No explicit timer task, no cancel-on-reconnect logic, no session-expiry-triggers-will logic visible in the MQTT layer.

**Impact**: Will messages may not fire correctly with delay intervals; reconnecting client may still see its will published.

---

#### GAP-7: Variable Byte Integer Minimum Encoding [Spec 1.5.5-1]
**Severity**: High
**Spec Requirement**: The encoded value MUST use the minimum number of bytes necessary. A decoder receiving a non-minimal encoding SHOULD treat it as a malformed packet.

**Current Behavior**: The decoder (`read_variable_int`) does not reject non-minimal encodings (e.g., encoding `10` as two bytes `0x8A 0x00` instead of one byte `0x0A`).

**Impact**: Interoperability issue; non-minimal encodings from malicious/buggy clients accepted silently.

---

### 2.3 Medium — Missing Reason Codes & Features

#### GAP-8: Missing Reason Codes
**Severity**: Medium
**Spec Requirement**: Several reason codes from the specification are not defined in `types.rs`:

| Code | Name | Used In |
|------|------|---------|
| 0x8A | Banned | CONNACK |
| 0x8B | Server shutting down | DISCONNECT |
| 0x8C | Bad authentication method | CONNACK |
| 0x8D | Keep Alive timeout | DISCONNECT |
| 0x8E | Session taken over | DISCONNECT |
| 0x93 | Receive Maximum exceeded | DISCONNECT |
| 0x96 | Message rate too high | DISCONNECT |
| 0x98 | Administrative action | DISCONNECT |
| 0x9C | QoS not supported | CONNACK |
| 0x9D | Use another server | DISCONNECT |
| 0x9E | Server moved | DISCONNECT |
| 0x9F | Connection rate exceeded | DISCONNECT |
| 0xA0 | Maximum connect time | DISCONNECT |

**Current**: Only ~18 of ~30+ spec-defined reason codes are constants.

---

#### GAP-9: Message Ordering Guarantee [Spec 4.6]
**Severity**: Medium
**Spec Requirement**: Server MUST deliver messages to each subscriber in the order in which they were published (per-topic, per-subscription).

**Current Behavior**: Ordering is implicitly maintained by bisque-mq queue semantics, but there is no explicit documentation or enforcement in the MQTT layer. QoS 2 handshake interleaving could potentially reorder deliveries.

**Impact**: Likely correct in practice (bisque-mq queues are ordered), but not explicitly guaranteed or tested at the MQTT layer.

---

#### GAP-10: Message Delivery Retry on Reconnect [Spec 4.4]
**Severity**: Medium
**Spec Requirement**: When a persistent session reconnects, the server MUST retransmit unacknowledged QoS 1 PUBLISH (with DUP=1) and pending PUBREL packets.

**Current Behavior**: Session store captures pending packet IDs, but the reconnect flow in `handle_connect` does not appear to retransmit in-flight messages. The `session_present=true` path restores subscriptions but doesn't replay unacked packets.

**Impact**: QoS 1/2 messages in flight at disconnect time may be lost on session resumption.

---

#### GAP-11: Background Session Expiry Task
**Severity**: Medium
**Spec Requirement**: Sessions with `session_expiry_interval > 0` MUST be maintained for that duration. After expiry, MUST be discarded [Spec 4.1.1-3].

**Current Behavior**: `SessionStore::expire()` exists but is an O(n) scan that must be called externally. No background timer task is wired up to call it periodically.

**Impact**: Expired sessions accumulate in memory until manually purged.

---

### 2.4 Low — Edge Cases & Hardening

#### GAP-12: Trailing Bytes Validation [Spec 3.x]
**Severity**: Low
**Spec Requirement**: Packets must not contain extra bytes beyond the declared remaining length.

**Current Behavior**: Codec decodes in correct order. Not verified if it rejects packets with unexpected trailing bytes after parsing.

---

#### GAP-13: Duplicate Property Detection Completeness [Spec 2.2.2]
**Severity**: Low
**Spec Requirement**: Except for User Property, properties MUST NOT appear more than once. Violation is a protocol error.

**Current Behavior**: Duplicate detection exists for most properties. Needs audit to ensure all non-repeatable properties are checked.

---

#### GAP-14: WebSocket Subprotocol Validation [Spec 6.0]
**Severity**: Low
**Spec Requirement**: WebSocket connections MUST use subprotocol name `"mqtt"`.

**Current Behavior**: Transport layer supports WebSocket but subprotocol negotiation needs verification.

---

#### GAP-15: Topic Alias Zero Rejection [Spec 3.3.2.3.4]
**Severity**: Low
**Spec Requirement**: Topic Alias value of 0 is not permitted. A PUBLISH with topic_alias=0 is a protocol error.

**Current Behavior**: Needs verification that alias value 0 is explicitly rejected.

---

---

## 3. Implementation Plan

### Phase 1: Critical Protocol Compliance (Priority: Immediate)

#### P1-1: Session Takeover
**Files**: `server.rs`, `session.rs`, `types.rs`
**Tasks**:
1. Add reason code constants: `SESSION_TAKEN_OVER = 0x8E`, `KEEP_ALIVE_TIMEOUT = 0x8D`
2. Add a global `ActiveSessions` registry (`DashMap<String, SessionHandle>`) in `MqttServer`
3. On new CONNECT with existing client_id:
   - Look up active session handle
   - Send DISCONNECT(0x8E) to old connection
   - Abort old connection task
   - Proceed with new connection (session_present=true if persistent)
4. On disconnect/drop, remove from registry
**Estimate**: ~200 LOC

#### P1-2: Inbound Receive Maximum Enforcement
**Files**: `session.rs`, `types.rs`
**Tasks**:
1. Add reason code: `RECEIVE_MAXIMUM_EXCEEDED = 0x93`
2. Add counter: `inbound_qos_inflight: usize` in session state
3. Increment on QoS 1/2 PUBLISH received, decrement on PUBACK/PUBCOMP sent
4. If counter exceeds `server_receive_maximum`, send DISCONNECT(0x93) and close
**Estimate**: ~50 LOC

#### P1-3: Session Expiry Update on DISCONNECT
**Files**: `session.rs`
**Tasks**:
1. In `handle_disconnect()`, extract `session_expiry_interval` from DISCONNECT properties
2. Validate: if current interval is 0 and new is non-zero, send DISCONNECT(0x82) (protocol error)
3. Otherwise update `self.session_expiry_interval` with new value
**Estimate**: ~30 LOC

### Phase 2: High Priority Behavioral Compliance

#### P2-1: No Local Validation on Shared Subscriptions
**Files**: `session.rs`
**Tasks**:
1. In subscription processing, after `parse_shared_subscription()`:
   - If `shared_group.is_some() && filter.no_local` then return reason code 0xA2
2. Add test case
**Estimate**: ~15 LOC

#### P2-2: Request Problem Information Suppression
**Files**: `session.rs`, `server.rs`
**Tasks**:
1. Store `request_problem_information: bool` in session (default true)
2. Create helper: `fn should_include_diagnostics(&self) -> bool`
3. Before setting `reason_string` or `user_properties` on PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT: check the flag
4. Exception: CONNACK on error and AUTH packets always include diagnostics
**Estimate**: ~40 LOC

#### P2-3: Will Delay Timer
**Files**: `session.rs`, `server.rs`
**Tasks**:
1. On unclean disconnect with `will_delay_interval > 0`:
   - Store will message + timer in a `PendingWills` registry (`DashMap<String, WillTimer>`)
   - Spawn `tokio::time::sleep(delay)` task that publishes will on expiry
2. On reconnect with same client_id: cancel pending will timer
3. On session expiry before will delay: publish will immediately
4. Integrate with session takeover (P1-1)
**Estimate**: ~80 LOC

#### P2-4: Variable Byte Integer Minimum Encoding Check
**Files**: `codec.rs`
**Tasks**:
1. In `read_variable_int()`, after decoding, verify the value could not be encoded in fewer bytes:
   - 1-byte max: 127
   - 2-byte max: 16,383
   - 3-byte max: 2,097,151
   - 4-byte max: 268,435,455
2. Return `CodecError::MalformedRemainingLength` if non-minimal
**Estimate**: ~15 LOC

### Phase 3: Medium Priority Features

#### P3-1: Complete Reason Code Constants
**Files**: `types.rs`
**Tasks**:
1. Add all missing reason code constants (see GAP-8 table)
2. Use semantic names matching the spec
**Estimate**: ~20 LOC

#### P3-2: Message Delivery Retry on Session Resumption
**Files**: `session.rs`, `session_store.rs`
**Tasks**:
1. On session restore (`session_present=true`):
   - Retransmit unacked QoS 1 PUBLISH packets with DUP=1
   - Retransmit pending PUBREL packets for QoS 2
2. Requires storing message content (not just packet IDs) in `PersistedSession`
3. Add `pending_publishes: Vec<StoredPublish>` to persisted state
**Estimate**: ~150 LOC

#### P3-3: Background Session Expiry Task
**Files**: `server.rs`, `session_store.rs`
**Tasks**:
1. Spawn periodic task (e.g., every 60s) calling `session_store.expire()`
2. Configurable interval via `MqttServerConfig`
3. Log expired sessions at debug level
**Estimate**: ~20 LOC

#### P3-4: Message Ordering Documentation & Testing
**Files**: tests
**Tasks**:
1. Add integration test: publish N messages to same topic, verify subscriber receives in order
2. Test with QoS 0, 1, and 2 separately
3. Document ordering guarantee dependency on bisque-mq
**Estimate**: ~60 LOC (tests)

### Phase 4: Low Priority Hardening

#### P4-1: Trailing Bytes Validation
Verify codec rejects packets with unexpected trailing bytes after parsing.

#### P4-2: Duplicate Property Audit
Audit all property decode paths to ensure non-repeatable properties trigger `DuplicateProperty` error.

#### P4-3: WebSocket Subprotocol Validation
Verify `"mqtt"` subprotocol is required during WebSocket upgrade handshake.

#### P4-4: Topic Alias Zero Rejection
Add explicit check: `if topic_alias == 0 { return Err(ProtocolError) }` in inbound PUBLISH handling.

---

## 4. Priority Matrix

```
              Impact
            High    Low
         +--------+--------+
Effort   | P1-1   | P3-1   |
Low      | P1-2   | P4-4   |
         | P1-3   | P2-1   |
         | P2-4   |        |
         +--------+--------+
Effort   | P2-3   | P3-4   |
High     | P3-2   | P4-1   |
         | P2-2   | P4-2   |
         |        | P4-3   |
         +--------+--------+
```

---

## 5. Test Coverage Recommendations

| Gap | Test Type | Description |
|-----|-----------|-------------|
| GAP-1 | Integration | Two clients connect with same ID; verify old gets 0x8E, new succeeds |
| GAP-2 | Unit | Send N+1 QoS 1 PUBLISHes where N=receive_maximum; verify disconnect |
| GAP-3 | Unit | DISCONNECT with session_expiry_interval; verify update and 0-to-non-zero rejection |
| GAP-4 | Unit | SUBSCRIBE `$share/g/t` with no_local=1; verify rejection |
| GAP-5 | Unit | CONNECT with request_problem_information=0; verify no reason_string in PUBACK |
| GAP-6 | Integration | Disconnect with will_delay=5s; reconnect at 3s; verify will NOT published |
| GAP-7 | Unit | Decode non-minimal variable byte integer; verify CodecError |
| GAP-10 | Integration | QoS 1 publish, disconnect before PUBACK, reconnect; verify retransmit with DUP=1 |

---

## 6. Spec Reference Quick-Index

| Spec Section | Implementation File | Status |
|-------------|-------------------|--------|
| 1.5.4 UTF-8 Strings | `codec.rs:244-282` | Complete |
| 1.5.5 Variable Byte Int | `codec.rs:319-331` | **GAP-7** |
| 2.2.2 Properties | `codec.rs` | Complete (audit GAP-13) |
| 3.1 CONNECT | `codec.rs`, `session.rs` | **GAP-1, GAP-3** |
| 3.2 CONNACK | `codec.rs`, `session.rs` | Complete |
| 3.3 PUBLISH | `codec.rs`, `session.rs` | **GAP-2** |
| 3.4-3.7 QoS Acks | `codec.rs`, `session.rs` | Complete |
| 3.8 SUBSCRIBE | `codec.rs`, `session.rs` | **GAP-4** |
| 3.9 SUBACK | `codec.rs` | Complete |
| 3.10-3.11 UNSUB | `codec.rs`, `session.rs` | Complete |
| 3.12-3.13 PING | `codec.rs`, `server.rs` | Complete |
| 3.14 DISCONNECT | `codec.rs`, `session.rs` | **GAP-3** |
| 3.15 AUTH | `codec.rs`, `auth.rs`, `session.rs` | Complete |
| 4.1 Session State | `session_store.rs` | **GAP-10, GAP-11** |
| 4.4 Message Retry | `session.rs` | **GAP-10** |
| 4.6 Ordering | -- | **GAP-9** |
| 4.7 Topic Filters | `codec.rs`, `server.rs` | Complete |
| 4.8 Shared Subs | `session.rs` | **GAP-4** |
| 4.9 Flow Control | `session.rs` | **GAP-2** |
| 4.12 Enhanced Auth | `auth.rs`, `session.rs` | Complete |
| 4.13 Error Handling | `codec.rs`, `session.rs` | **GAP-5, GAP-8** |
| 6.0 WebSocket | `transport.rs` | **GAP-14** |
