# MQTT 3.1.1 Gap Analysis & Implementation Plan

Cross-reference of the [OASIS MQTT 3.1.1 specification](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) against the bisque-mq-mqtt implementation.

**Legend:** PASS = fully implemented, PARTIAL = partially implemented, GAP = missing/incorrect

---

## 1. Data Representations (SS 1.5)

### 1.1 UTF-8 Encoded Strings (SS 1.5.3)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-1.5.3-1 | UTF-8 must be well-formed; must not contain U+D800..U+DFFF | PASS | `codec.rs:255-269` — `validate_mqtt_utf8()` checks all forbidden ranges. Rust `str` guarantees no surrogates. |
| MQTT-1.5.3-2 | Must not encode null U+0000; receiver must close connection | PASS | `validate_mqtt_utf8()` rejects `\0`. |
| MQTT-1.5.3-3 | U+FEFF BOM must not be skipped or stripped | PASS | No BOM stripping logic exists — bytes pass through as-is. |
| — | Control chars U+0001..U+001F, U+007F..U+009F rejected | PASS | `validate_mqtt_utf8()` rejects these ranges. |
| — | Non-characters U+FFFE, U+FFFF rejected | PASS | Explicit check in `validate_mqtt_utf8()`. |
| — | UTF-8 string length ≤ 65535 bytes | PASS | `read_mqtt_string()` reads a `u16` length prefix (max 65535). |

### 1.2 Remaining Length Encoding (SS 2.2.3)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| — | Variable-length encoding up to 4 bytes (max 268,435,455) | PASS | `decode_remaining_length()` / `encode_remaining_length()` handle 1-4 byte encoding. |
| — | Values > 256 MB rejected | PASS | `MAX_PACKET_SIZE = 256 * 1024 * 1024` enforced in decoder. |

---

## 2. MQTT Control Packet Format (SS 2)

### 2.1 Fixed Header (SS 2.2)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-2.2.2-1 | Reserved flag bits must match Table 2.2 | PASS | `validate_fixed_header_flags()` in `codec.rs:83-105` enforces all packet-type-specific flag values. |
| MQTT-2.2.2-2 | Invalid flags → close connection | PASS | Returns `CodecError::InvalidFixedHeaderFlags`, server disconnects. |
| — | PUBLISH flags: DUP(bit3), QoS(bits2-1), RETAIN(bit0) variable | PASS | `decode_publish()` extracts flags correctly. |
| — | PUBREL/SUBSCRIBE/UNSUBSCRIBE: flags must be 0x02 | PASS | `validate_fixed_header_flags()` checks packet types 6, 8, 10. |
| — | All other packet types: flags must be 0x00 | PASS | Covered in match arm for types 1,2,4,5,7,9,11,12,13,14. |

### 2.2 Packet Identifier (SS 2.3.1)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-2.3.1-1 | SUBSCRIBE, UNSUBSCRIBE, PUBLISH(QoS>0) must have non-zero packet ID | PARTIAL | Packet ID is decoded but **no explicit zero-check** is performed on inbound packet IDs. |
| MQTT-2.3.1-2 | Each new packet must use a currently unused packet ID | PASS | `alloc_packet_id()` in `session.rs:408-427` skips IDs in use. |
| MQTT-2.3.1-5 | QoS 0 PUBLISH must NOT have packet ID | PASS | `decode_publish()` only reads packet ID when `qos != AtMostOnce`. |
| MQTT-2.3.1-7 | SUBACK/UNSUBACK must echo the packet ID | PASS | `handle_subscribe()` and `handle_unsubscribe()` copy packet_id into response. |

**GAP-1:** No validation that inbound packet IDs from clients are non-zero for QoS 1/2 PUBLISH, SUBSCRIBE, and UNSUBSCRIBE.

---

## 3. CONNECT (SS 3.1)

### 3.1 Protocol Name & Version (SS 3.1.2.1-2)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.0-1 | First packet from client must be CONNECT | PASS | `handle_connection()` in `server.rs:848-866` waits for CONNECT with timeout, rejects non-CONNECT. |
| MQTT-3.1.0-2 | Second CONNECT must be treated as protocol violation | PASS | `process_inbound_packet()` in `server.rs:1244-1266` sends DISCONNECT on second CONNECT. |
| MQTT-3.1.2-1 | Incorrect protocol name → may disconnect | PASS | `decode_connect()` returns `InvalidProtocolName` if name ≠ "MQTT". |
| MQTT-3.1.2-2 | Unsupported protocol level → CONNACK 0x01 + disconnect | PARTIAL | `decode_connect()` returns `UnsupportedProtocolVersion` error, but the server closes the connection without first sending CONNACK with code 0x01. |

**GAP-2:** When protocol level is unsupported (not 4 or 5), the spec requires sending CONNACK with return code 0x01 before disconnecting. Currently the codec returns an error and the connection is dropped without a CONNACK.

### 3.2 Connect Flags (SS 3.1.2.3)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.2-3 | Reserved bit 0 must be 0; disconnect if not | PASS | `ConnectFlags::from_byte()` returns `None` if bit 0 set. |
| MQTT-3.1.2-22 | Password flag=1 requires username flag=1 (V3.1.1) | PASS | `decode_connect()` at line 972 rejects password without username for V311. |

### 3.3 Clean Session (SS 3.1.2.4)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.2-4 | CleanSession=0 → resume from stored session | PASS | `session_store.rs` implements persistence; `server.rs:878-894` loads persisted session on reconnect. |
| MQTT-3.1.2-5 | CleanSession=0 → store QoS 1/2 messages after disconnect | PASS | Queues persist across disconnects when clean_session=false; bindings remain. |
| MQTT-3.1.2-6 | CleanSession=1 → discard previous session; state must not be reused | PASS | `handle_connect()` clears all maps when `clean_session=true`. |
| MQTT-3.1.2-7 | Retained messages must not be deleted when session ends | PASS | Retained messages stored in separate `$mqtt/retained/` topics, not tied to session lifecycle. |

### 3.4 Will Message (SS 3.1.2.5-10)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.2-8 | Will flag=1 → publish will on unclean disconnect | PASS | `handle_unclean_disconnect()` builds will `PublishPlan`; `server.rs:1016-1044` executes it. |
| MQTT-3.1.2-9 | Will flag=1 → will topic/message must be in payload | PASS | `decode_connect()` reads will fields when flag is set. |
| MQTT-3.1.2-10 | Will message removed after publish or DISCONNECT receipt | PASS | `handle_disconnect()` sets `self.will = None`; `handle_unclean_disconnect()` sets `self.will = None` after building plan. |
| MQTT-3.1.2-11 | Will flag=0 → will QoS/retain must be 0, will fields absent | PASS | `ConnectFlags::from_byte()` validates at line 151. |
| MQTT-3.1.2-12 | Will flag=0 → will not published on disconnect | PASS | Will plan is `None` when `self.will` is `None`. |
| MQTT-3.1.2-14 | Will QoS must not be 3 | PASS | `QoS::from_u8(3)` returns `None`. |
| MQTT-3.1.2-16 | Will retain=0 → publish as non-retained | PASS | Will `retain` flag propagated through `PublishPlan`. |
| MQTT-3.1.2-17 | Will retain=1 → publish as retained | PASS | `handle_unclean_disconnect()` creates `RetainedPlan` when `will.retain=true`. |

### 3.5 Keep Alive (SS 3.1.2.10)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.2-24 | Server must disconnect if no packet within 1.5× keep-alive | PASS | `connection_loop()` in `server.rs:1089-1093` sets timeout to `keep_alive * 3 / 2`. |
| — | Keep alive=0 means no timeout (V3.1.1: server may override) | PASS | `handle_connect()` overrides keep_alive=0 with `default_keep_alive` for V311. |

### 3.6 Client Identifier (SS 3.1.3.1)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.3-3 | ClientId must be present and first field in payload | PASS | `decode_connect()` reads client_id first after variable header. |
| MQTT-3.1.3-4 | ClientId must be a UTF-8 encoded string | PASS | Uses `read_mqtt_string()` with UTF-8 validation. |
| MQTT-3.1.3-5 | Server must allow 1-23 byte ClientIds of [0-9a-zA-Z] | PARTIAL | Server accepts any valid UTF-8 ClientId. No restriction to alphanumeric or length enforcement — this is more permissive than required, which is allowed. |
| MQTT-3.1.3-7 | Zero-byte ClientId requires CleanSession=1 | PASS | `handle_connect()` at line 581-598 returns IdentifierRejected for empty ClientId with CleanSession=0. |
| MQTT-3.1.3-8 | Zero-byte ClientId + CleanSession=0 → CONNACK 0x02 + close | PASS | Returns CONNACK with `ConnectReturnCode::IdentifierRejected`. |

### 3.7 Payload Order (SS 3.1.3)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.3-1 | Payload fields in order: ClientId, Will Topic, Will Message, Username, Password | PASS | `decode_connect()` reads fields in spec order. |

### 3.8 Server Processing (SS 3.1.4)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.1.4-1 | Server must validate CONNECT and close without CONNACK if malformed | PARTIAL | Codec errors cause connection drop, but some codec errors should specifically not send CONNACK while others (like bad protocol level) should send CONNACK first. See GAP-2. |
| MQTT-3.1.4-2 | If ClientId represents already-connected client, disconnect old | PASS | `server.rs:964-969` — session takeover via `active_sessions` DashMap + oneshot signal. |
| MQTT-3.1.4-4 | Server must acknowledge CONNECT with CONNACK return code 0 | PASS | `handle_connect()` returns `ConnAck { return_code: Accepted as u8 }`. |

---

## 4. CONNACK (SS 3.2)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.2.0-1 | First packet from server must be CONNACK | PASS | `handle_connection()` sends CONNACK immediately after processing CONNECT. |
| MQTT-3.2.2-1 | CleanSession=1 accepted → session_present=0 | PASS | `handle_connect()` sets `session_present: !self.clean_session`, so CleanSession=1 → false. |
| MQTT-3.2.2-2 | CleanSession=0 with stored session → session_present=1 | PASS | `server.rs:918-921` patches `session_present=true` when persisted session found. |
| MQTT-3.2.2-3 | CleanSession=0 without stored session → session_present=0 | PASS | Default `session_present: !self.clean_session` = true, but no patch applied → returns true. |
| MQTT-3.2.2-4 | Non-zero return code → session_present=0 | PASS | Error CONNACK paths (e.g., IdentifierRejected) set `session_present: false`. |

**GAP-3:** MQTT-3.2.2-3 — When CleanSession=0 but no stored session exists, the server should set session_present=0. Currently `handle_connect()` returns `session_present: !self.clean_session` (which is `true`), and the server only patches it to `true` when a session IS found. It should also patch it to `false` when no stored session is found for CleanSession=0.

---

## 5. PUBLISH (SS 3.3)

### 5.1 Topic Name (SS 3.3.2.1)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.3.1-1 | Topic name must not contain wildcards | PASS | `validate_topic_name()` rejects `+`, `#`, and null bytes. |
| — | Topic name must be valid UTF-8 | PASS | `read_mqtt_string_as_bytes()` / `decode_publish_zero_copy()` validate UTF-8. |
| — | Topic name must not be empty | PASS | `validate_topic_name()` rejects empty topics. |

### 5.2 DUP Flag (SS 3.3.1.1)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.3.1-2 | DUP=0 for first delivery attempt | PASS | `build_outbound_publish` / `track_outbound_delivery` sets `dup: false`. |
| MQTT-3.3.1-3 | DUP=1 on retransmission of QoS>0 | PARTIAL | `pending_retransmits()` only retransmits PUBRELs. QoS 1 PUBLISH retransmits are noted as needing the original payload from the queue. |

**GAP-4:** QoS 1 PUBLISH retransmission on session resume does not reconstruct the original PUBLISH with DUP=1. Only PUBREL retransmissions are implemented. The session does not cache the original PUBLISH payloads needed for QoS 1 retransmit.

### 5.3 QoS (SS 3.3.1.2)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.3.1-5 | QoS 3 (value 0x03) is not permitted; close connection | PASS | `QoS::from_u8(3)` returns `None` → `InvalidQoS` error. `validate_fixed_header_flags` permits any PUBLISH flags, but the QoS decode catches the invalid value. |
| MQTT-3.3.1-12 | Delivery QoS = min(subscription QoS, publish QoS) | PASS | `handle_subscribe()` clamps QoS to `config.maximum_qos`; outbound delivery uses `mapping.max_qos`. |

### 5.4 Retain (SS 3.3.1.3)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.3.1-6 | RETAIN=1 → server stores message | PASS | `handle_publish()` creates `RetainedPlan` when retain=true. Stored in `$mqtt/retained/<topic>` with `max_messages: 1`. |
| MQTT-3.3.1-7 | RETAIN=1 + empty payload → delete retained message | PASS | `handle_publish()` creates `RetainedPlan { flat_message: None }` for empty payload → `purge_topic`. |
| MQTT-3.3.1-8 | RETAIN=0 + empty payload → do NOT delete retained | PASS | Retained plan only created when `publish.retain` is true. |
| MQTT-3.3.1-9 | New subscription → send matching retained messages | PASS | `deliver_retained_on_subscribe()` in `server.rs:1479+` delivers retained messages after SUBACK. |

### 5.5 Packet Identifier (SS 3.3.2.2)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| — | QoS 0 → no packet ID | PASS | `decode_publish()` only reads packet ID for QoS > 0. |
| — | QoS 1/2 → packet ID present | PASS | Packet ID decoded when QoS is AtLeastOnce or ExactlyOnce. |

---

## 6. PUBACK (SS 3.4)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| — | Remaining length = 2 | PASS | `encode_pub_ack_common()` writes exactly 2 bytes for V3.1.1. |
| MQTT-3.4.4-1 | Treat PUBACK as acknowledgment | PASS | `handle_puback()` removes from `qos1_inflight` and ACKs in bisque-mq. |

---

## 7. PUBREC (SS 3.5)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| — | Remaining length = 2 | PASS | Same common encoder. |
| MQTT-3.5.4-1 | Treat as QoS 2 step 1 ack | PASS | `handle_pubrec()` transitions from `PublishSent` → `PubRelSent`. |

---

## 8. PUBREL (SS 3.6)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.6.4-1 | Fixed header bit 1 = 1 (flags = 0x02) | PASS | `validate_fixed_header_flags()` enforces 0x02 for type 6. Encoder writes `0x62`. |
| MQTT-3.6.4-2 | Respond with PUBCOMP | PASS | `handle_pubrel()` returns PUBCOMP with matching packet ID. |

---

## 9. PUBCOMP (SS 3.7)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| — | Remaining length = 2 | PASS | Same common encoder. |
| MQTT-3.7.4-1 | Treat as final QoS 2 ack | PASS | `handle_pubcomp()` removes from `qos2_outbound`, ACKs in bisque-mq. |

---

## 10. SUBSCRIBE (SS 3.8)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.8.0-1 | Fixed header flags = 0x02 | PASS | `validate_fixed_header_flags()` enforces 0x02 for type 8. |
| MQTT-3.8.3-1 | Payload must contain at least one topic filter/QoS pair | GAP | No check for empty filter list after decoding. |
| MQTT-3.8.3-2 | Topic filter must be UTF-8 | PASS | `read_mqtt_string()` validates UTF-8. |
| MQTT-3.8.3-3 | Requested QoS must be 0, 1, or 2 | PASS | `QoS::from_u8()` rejects values > 2. |
| — | Options byte bits 6-7 reserved, must be 0 (V3.1.1) | GAP | No validation that reserved bits in the SUBSCRIBE options byte are 0 for V3.1.1. |

**GAP-5:** No validation that a SUBSCRIBE packet contains at least one topic filter. An empty SUBSCRIBE should be rejected as a protocol error.

**GAP-6:** For V3.1.1, the SUBSCRIBE options byte upper bits (6-7) are reserved and must be 0. Currently the decoder reads `no_local`, `retain_as_published`, and `retain_handling` from these bits unconditionally — these are V5-only features. For V3.1.1, these bits should be validated as 0.

---

## 11. SUBACK (SS 3.9)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.9.0-1 | Server must send SUBACK in response to SUBSCRIBE | PASS | `process_inbound_packet()` sends SUBACK after orchestration. |
| MQTT-3.9.3-1 | SUBACK must contain one return code per filter, in order | PASS | `handle_subscribe()` builds `return_codes` per filter in order. |
| MQTT-3.9.3-2 | Return code must be 0x00, 0x01, 0x02, or 0x80 | PASS | Returns `granted_qos.as_u8()` (0/1/2) or `0x80` on failure. |

---

## 12. UNSUBSCRIBE (SS 3.10)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.10.0-1 | Fixed header flags = 0x02 | PASS | `validate_fixed_header_flags()` enforces 0x02 for type 10. |
| MQTT-3.10.3-1 | Payload must contain at least one topic filter | GAP | No check for empty filter list after decoding. |
| MQTT-3.10.3-2 | Topic filter must be UTF-8 | PASS | `read_mqtt_string()` validates UTF-8. |

**GAP-7:** No validation that UNSUBSCRIBE contains at least one topic filter.

---

## 13. UNSUBACK (SS 3.11)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.11.0-1 | Server must send UNSUBACK | PASS | `process_inbound_packet()` dispatches to `session.process_packet()` which returns UNSUBACK. However, UNSUBSCRIBE is handled via the `_ =>` fallback, not a dedicated match arm. |

**Note:** UNSUBSCRIBE handling falls through to `session.process_packet()` in the `_ =>` catch-all of `process_inbound_packet()`. While functionally correct, it would be cleaner to have a dedicated match arm (like SUBSCRIBE has) to ensure proper logging and error handling.

---

## 14. PINGREQ / PINGRESP (SS 3.12-3.13)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-3.12.4-1 | Server must respond to PINGREQ with PINGRESP | PASS | `process_inbound_packet()` at line 1384-1395 handles PINGREQ, sends PINGRESP. |

---

## 15. DISCONNECT (SS 3.14)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| — | DISCONNECT has no payload | PASS | `decode_disconnect()` handles remaining_length=0. |
| MQTT-3.14.4-3 | After receiving DISCONNECT, server must discard will message | PASS | `handle_disconnect()` sets `self.will = None` (for V3.1.1 and V5 normal disconnect). |
| — | Server must close network connection after DISCONNECT | PASS | `process_inbound_packet()` returns `Ok(())` for DISCONNECT, which exits `connection_loop()`. |

---

## 16. Operational Behavior (SS 4)

### 16.1 Session State (SS 4.1)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-4.1.0-1 | Server must store session state for CleanSession=0 | PASS | `session_store.rs` persists subscriptions and in-flight packet IDs. |
| — | Session state includes: subscriptions, QoS 1/2 in-flight messages | PASS | `PersistedSession` captures `subscriptions`, `pending_qos1`, `pending_qos2`. |

### 16.2 Message Delivery (SS 4.3)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-4.3.1-1 | QoS 0: at-most-once delivery | PASS | QoS 0 messages published without tracking. No ACK. |
| MQTT-4.3.2-1 | QoS 1: at-least-once with PUBACK | PASS | Full QoS 1 flow: PUBLISH → PUBACK, with in-flight tracking. |
| MQTT-4.3.3-1 | QoS 2: exactly-once with PUBREC/PUBREL/PUBCOMP | PASS | Full QoS 2 4-step flow implemented in session.rs. |
| — | QoS 2 duplicate detection | PASS | `handle_publish()` checks `qos2_inbound` for existing `PubRecSent` state. |

### 16.3 Message Ordering (SS 4.6)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| — | Message order should be maintained per topic | PARTIAL | bisque-mq queues preserve order. However, multiple subscriptions may deliver from different queues, and the round-robin delivery poll doesn't guarantee cross-topic ordering. Within a single subscription queue, ordering is preserved. |

### 16.4 Topic Names and Filters (SS 4.7)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-4.7.1-1 | `+` matches exactly one level | PASS | Translated to `*` in bisque-mq exchange pattern via `mqtt_filter_to_mq_pattern()`. |
| MQTT-4.7.1-2 | `#` matches zero or more levels | PASS | `#` passed through directly (bisque-mq uses same wildcard). |
| — | Topic filter validation (wildcards in correct positions) | PASS | `validate_topic_filter()` validates `+` and `#` positioning. |
| MQTT-4.7.2-1 | Topics beginning with `$` must not match wildcard subscriptions | GAP | No `$`-prefix filtering is implemented. A subscription to `#` would match `$SYS/...` topics. |

**GAP-8:** The server does not prevent `$`-prefixed topics (like `$SYS/`) from being matched by subscriptions to `#` or `+/...`. The spec requires that topics beginning with `$` are not matched against topic filters starting with wildcards.

### 16.5 Error Handling (SS 4.8)

| Req ID | Requirement | Status | Notes |
|--------|-------------|--------|-------|
| MQTT-4.8.4-1 | Close connection on protocol violation | PASS | Various paths disconnect on protocol errors. V5 sends DISCONNECT first; V3.1.1 drops connection. |

---

## 17. Security (SS 5)

| Requirement | Status | Notes |
|-------------|--------|-------|
| Username/password authentication | PASS | `auth.rs` — `SimpleAuthProvider` validates credentials. Pluggable `AuthProvider` trait. |
| TLS support | PASS | `transport.rs` — `MqttStream::Tls` variant via `tokio-rustls` (behind `tls` feature). |
| Authorization (per-topic ACLs) | GAP | No topic-level authorization. Any authenticated client can publish/subscribe to any topic. |

**GAP-9:** No per-topic authorization. While the spec doesn't mandate a specific authorization model, it recommends restricting client access to specific topics. The `AuthProvider` trait only covers authentication, not authorization.

---

## Gap Summary

| Gap | Severity | Spec Reference | Description |
|-----|----------|----------------|-------------|
| GAP-1 | Low | SS 2.3.1 | No validation that inbound packet IDs are non-zero |
| GAP-2 | Medium | SS 3.1.2-2, 3.1.4-1 | Unsupported protocol version doesn't send CONNACK 0x01 before disconnect |
| GAP-3 | Medium | SS 3.2.2-3 | session_present incorrectly set to 1 for CleanSession=0 when no stored session exists |
| GAP-4 | Medium | SS 4.4 | QoS 1 PUBLISH retransmission on session resume not implemented (no cached payloads) |
| GAP-5 | Low | SS 3.8.3-1 | Empty SUBSCRIBE payload not rejected |
| GAP-6 | Low | SS 3.8.3 | V3.1.1 SUBSCRIBE options byte reserved bits not validated |
| GAP-7 | Low | SS 3.10.3-1 | Empty UNSUBSCRIBE payload not rejected |
| GAP-8 | Medium | SS 4.7.2-1 | `$`-prefixed topics match wildcard subscriptions (should not) |
| GAP-9 | Low | SS 5 | No per-topic authorization (recommendation, not normative) |

---

## Implementation Plan

### Phase 1: Protocol Correctness (Medium severity gaps)

#### 1.1 Fix CONNACK for unsupported protocol version (GAP-2)

**File:** `codec.rs` / `server.rs`

The `decode_connect()` function currently returns `UnsupportedProtocolVersion` which causes the connection to be dropped. Instead:
- In `decode_connect()`, when protocol level is not 4 or 5, still parse enough of the CONNECT to return a result (or return a partial result with the error).
- In `server.rs` `handle_connection()`, catch this specific error before the generic codec error path and send CONNACK with return code 0x01 (`UnacceptableProtocolVersion`) before closing.

**Approach:** Add a new `CodecError::UnsupportedProtocolVersionConnect` variant that carries the partial parse state, or handle it in the server by detecting the error type and sending a CONNACK 0x01 response.

#### 1.2 Fix session_present for CleanSession=0 without stored session (GAP-3)

**File:** `server.rs`

In `handle_connection()`, after checking the session store:
```
if !session.clean_session && !session_resumed {
    // No stored session found — patch session_present to false
}
```

Currently the code only patches `session_present=true` when a session is found. Add the inverse case.

#### 1.3 QoS 1 PUBLISH retransmission on session resume (GAP-4)

**File:** `session.rs` / `server.rs`

On session resume (CleanSession=0), outbound QoS 1 messages that were not ACKed should be retransmitted with DUP=1. Options:
1. **Simplest**: Re-deliver from the bisque-mq queue (messages are NACKed on disconnect, so they return to pending). The existing outbound delivery loop will naturally re-deliver them. This is the current behavior — the gap is primarily about DUP=1 on retransmit, which the current `build_outbound_publish_from_flat` sets to `false`.
2. **Fix**: Add a `retransmit` flag to delivery tracking so the first delivery after session resume sets DUP=1 for messages that were previously in-flight.

#### 1.4 `$`-prefixed topic filtering (GAP-8)

**File:** `session.rs` or exchange routing layer

When matching published messages to subscriptions:
- If the published topic starts with `$`, skip any subscription whose filter starts with `+`, `#`, or any other wildcard as the first character.
- The simplest implementation: in the exchange binding, topics starting with `$` should not match filters starting with `#` or `+`.

**Approach:** Add a check in the exchange routing or in the outbound delivery filtering. Since bisque-mq's topic exchange does the matching, this may need a flag on the binding or a post-match filter in `deliver_outbound()`.

### Phase 2: Validation Hardening (Low severity gaps)

#### 2.1 Validate non-zero packet IDs (GAP-1)

**File:** `codec.rs`

In `decode_subscribe()`, `decode_unsubscribe()`, and `decode_publish()` (for QoS > 0), add:
```rust
if packet_id == 0 {
    return Err(CodecError::ProtocolError);
}
```

#### 2.2 Validate non-empty SUBSCRIBE/UNSUBSCRIBE payloads (GAP-5, GAP-7)

**File:** `codec.rs`

After the decode loop in `decode_subscribe()` and `decode_unsubscribe()`, check:
```rust
if filters.is_empty() {
    return Err(CodecError::ProtocolError);
}
```

#### 2.3 Validate V3.1.1 SUBSCRIBE options byte (GAP-6)

**File:** `codec.rs` or `session.rs`

For V3.1.1, the upper 6 bits of the SUBSCRIBE options byte (bits 2-7) are reserved. Currently the decoder reads V5-only fields from these bits. Options:
1. **In codec**: Add version-aware decoding that rejects non-zero upper bits for V3.1.1.
2. **In session**: Ignore `no_local`, `retain_as_published`, `retain_handling` for V3.1.1 sessions (already partially done since these features only activate for V5 in session logic).

The cleanest fix is in the codec via `decode_subscribe_v()` / `decode_packet_versioned()` — for V3.1.1, validate `options_byte & 0xFC == 0`.

### Phase 3: Optional Enhancements

#### 3.1 Per-topic authorization (GAP-9)

Extend the `AuthProvider` trait with an authorization method:
```rust
fn authorize_publish(&self, client_id: &str, topic: &str) -> bool;
fn authorize_subscribe(&self, client_id: &str, filter: &str) -> bool;
```

Call these in `handle_publish()` and `handle_subscribe()` to enforce topic-level ACLs.

---

## Conformance Test Coverage

The existing test suite in `types.rs` and `codec.rs` covers:
- QoS roundtrip and boundary values
- Protocol version encoding/decoding
- Packet type parsing for all 15 types
- Connect flags encoding/decoding including reserved bit validation
- Will flag consistency validation
- Connect return codes
- MQTT 5.0 reason codes
- UTF-8 string validation

**Recommended additional tests:**
1. Zero packet ID rejection (after GAP-1 fix)
2. Empty SUBSCRIBE/UNSUBSCRIBE rejection (after GAP-5/7 fix)
3. CONNACK 0x01 for unsupported protocol version (after GAP-2 fix)
4. `$`-topic wildcard filtering (after GAP-8 fix)
5. Session_present=0 when CleanSession=0 with no stored session (after GAP-3 fix)
6. DUP=1 on QoS 1 retransmission (after GAP-4 fix)
7. V3.1.1 SUBSCRIBE options byte reserved bits validation (after GAP-6 fix)
