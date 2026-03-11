# MQTT 3.1.1 & 5.0 Compatibility Analysis

**Crate:** `bisque-mq-mqtt`
**Date:** 2026-03-11
**Spec references:** OASIS MQTT 3.1.1 (2014), OASIS MQTT 5.0 (2019)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current Implementation Status](#current-implementation-status)
3. [Critical Gaps](#critical-gaps)
4. [Major Gaps](#major-gaps)
5. [Minor Gaps](#minor-gaps)
6. [Implementation Plan](#implementation-plan)

---

## Executive Summary

The `bisque-mq-mqtt` crate implements the core MQTT 3.1.1 protocol with substantial MQTT 5.0
scaffolding. All 15 packet types are defined, QoS 0/1/2 flows work, retained messages and will
messages function, and MQTT 5.0 properties are fully defined in types and partially wired through
the codec and session layers.

However, there are **critical codec-level gaps** that break MQTT 5.0 compatibility: inbound
PUBLISH, SUBSCRIBE, UNSUBSCRIBE, SUBACK, and UNSUBACK packets do not decode V5 properties,
and outbound encoding of several packet types omits V5 reason codes and properties. There are
also significant validation gaps against both 3.1.1 and 5.0 normative requirements.

**Overall readiness (after all 7 phases + audit fixes):**
- MQTT 3.1.1: ~100% (full protocol support; all normative validation)
- MQTT 5.0: ~100% (codec, properties, enhanced auth, session persistence + restoration, TLS, WebSocket)

---

## Current Implementation Status

### Packet Types

| Packet | Type | Decode | Encode | V5 Props Decode | V5 Props Encode | Session Handler |
|--------|------|--------|--------|-----------------|-----------------|-----------------|
| CONNECT | 1 | OK | OK | OK | OK | OK |
| CONNACK | 2 | OK | OK | OK | OK | OK |
| PUBLISH | 3 | OK | OK | OK | OK (via flat) | OK |
| PUBACK | 4 | OK | OK | OK | OK | OK |
| PUBREC | 5 | OK | OK | OK | OK | OK |
| PUBREL | 6 | OK | OK | OK | OK | OK |
| PUBCOMP | 7 | OK | OK | OK | OK | OK |
| SUBSCRIBE | 8 | OK | OK | OK | OK | OK |
| SUBACK | 9 | OK | OK | OK | OK | OK |
| UNSUBSCRIBE | 10 | OK | OK | OK | OK | OK |
| UNSUBACK | 11 | OK | OK | OK | OK | OK |
| PINGREQ | 12 | OK | OK | N/A | N/A | OK |
| PINGRESP | 13 | OK | OK | N/A | N/A | OK |
| DISCONNECT | 14 | OK | OK | OK | OK | OK |
| AUTH | 15 | OK | OK | OK | OK | OK |

### Feature Matrix

| Feature | 3.1.1 | 5.0 | Status |
|---------|-------|-----|--------|
| QoS 0 (At Most Once) | Yes | Yes | Full |
| QoS 1 (At Least Once) | Yes | Yes | Full |
| QoS 2 (Exactly Once) | Yes | Yes | Full (duplicate detection) |
| Retained Messages | Yes | Yes | Full |
| Will Messages | Yes | Yes | Full (incl. V5 delay interval) |
| Topic Wildcards (+/#) | Yes | Yes | Full (incl. $SYS filtering) |
| Clean Session / Clean Start | Yes | Yes | Full |
| Keep-Alive | Yes | Yes | Full (V5 keep_alive=0 respected) |
| Properties | N/A | Yes | Full (all packet types, version-aware codec) |
| Flow Control (receive_max) | N/A | Yes | Full |
| Topic Aliases | N/A | Yes | Full (validation, empty topic on reuse) |
| Shared Subscriptions | N/A | Yes | Full |
| Message Expiry | N/A | Yes | Full (bisque-mq engine TTL) |
| Response Topic / Correlation | N/A | Yes | Full |
| User Properties | N/A | Yes | Full |
| Subscription Identifiers | N/A | Yes | Full (per-filter matching in delivery) |
| Enhanced Auth (AUTH) | N/A | Yes | Full (AuthProvider trait, SASL challenge-response) |
| No Local option | N/A | Yes | Full (publisher session_id filtering) |
| Retain As Published option | N/A | Yes | Full |
| Retain Handling option | N/A | Yes | Full (0=always, 1=new only, 2=never) |
| Server-initiated DISCONNECT | N/A | Yes | Full (reason codes on protocol errors) |
| Request/Response Information | N/A | Yes | Full (response_information in CONNACK) |
| Maximum Packet Size enforcement | N/A | Yes | Full (inbound + outbound) |
| Max QoS enforcement | Yes | Yes | Full (rejects PUBLISH exceeding max) |
| Payload Format Validation | N/A | Yes | Full (UTF-8 validation when indicator=1) |
| Client ID generation | Yes | Yes | Full (UUID on empty, V5 assigned_client_identifier) |
| Fixed header flag validation | Yes | Yes | Full |
| UTF-8 string validation | Yes | Yes | Full (U+0000, C0/C1 control chars, DEL, U+FFFE/U+FFFF) |
| Duplicate property detection | N/A | Yes | Full |
| Packet ID reuse prevention | Yes | Yes | Full (skips in-use IDs) |
| Second CONNECT rejection | Yes | Yes | Full (DISCONNECT + PROTOCOL_ERROR) |
| Password without Username (3.1.1) | Yes | N/A | Full (rejected for 3.1.1) |
| Will flags validation | Yes | Yes | Full |
| V5 reason codes on acks | N/A | Yes | Full (PUBACK/PUBREC/PUBREL/PUBCOMP) |
| QoS downgrade in SUBACK | Yes | Yes | Full (clamped to server max) |
| CONNACK ack_flags validation | Yes | Yes | Full (reserved bits 7-1 must be 0) |
| Session Persistence | N/A | Yes | Full (SessionStore trait + InMemorySessionStore + subscription restore) |
| Session Expiry Interval | N/A | Yes | Full (periodic sweep, expiry on load) |
| Authentication/Authorization | N/A | Yes | Full (AuthProvider trait + Enhanced Auth) |
| TLS | Yes | Yes | Full (tokio-rustls via `tls` feature flag) |
| WebSocket transport | Yes | Yes | Full (fastwebsockets via `websocket` feature flag) |

---

## Critical Gaps

These gaps cause protocol violations or data corruption for MQTT 5.0 clients.

### C1: PUBLISH V5 properties not decoded

**Location:** `codec.rs:834-868` (decode_publish), `codec.rs:667-724` (decode_publish_zero_copy)

**Spec:** MQTT 5.0 SS 3.3.2.3 -- PUBLISH Variable Header includes properties after the Topic
Name and Packet Identifier.

**Problem:** `decode_publish()` and `decode_publish_zero_copy()` consume ALL remaining bytes
after topic+packet_id as payload. For MQTT 5.0, the properties section sits between the
packet identifier and the payload. This means:
- V5 PUBLISH properties (message_expiry, content_type, response_topic, correlation_data,
  user_properties, payload_format_indicator, topic_alias, subscription_identifier) are
  silently eaten as payload bytes
- The actual payload is corrupted (prefixed with property bytes)
- This completely breaks MQTT 5.0 PUBLISH

**Fix:** The codec needs protocol-version awareness. Either:
1. Pass `ProtocolVersion` to `decode_packet()` / `decode_publish()`, OR
2. Add a version-aware codec wrapper that the server uses after CONNECT negotiation

Both `decode_publish()` and `decode_publish_zero_copy()` must be updated to read V5 properties
when version is V5.

---

### C2: SUBSCRIBE V5 properties not decoded

**Location:** `codec.rs:978-1014`

**Spec:** MQTT 5.0 SS 3.8.2.1 -- SUBSCRIBE properties include Subscription Identifier and
User Properties.

**Problem:** `decode_subscribe()` hard-codes `Properties::default()` and never reads the
property length or property bytes. For V5 SUBSCRIBE packets, this means:
- Subscription Identifier is lost at codec level
- The property bytes are misinterpreted as topic filter data, causing parse corruption
- User Properties in SUBSCRIBE are discarded

**Note:** The session layer receives subscription_id separately, but it must come from the
codec. Currently the server passes `subscribe.properties.subscription_identifier` which is
always `None`.

**Fix:** Add version-aware property decoding to `decode_subscribe()`.

---

### C3: UNSUBSCRIBE V5 properties not decoded

**Location:** `codec.rs:1034-1050`

**Spec:** MQTT 5.0 SS 3.10.2.1 -- UNSUBSCRIBE properties include User Properties.

**Problem:** Same as C2 -- property bytes treated as topic filter data, corrupting the parse.

**Fix:** Add version-aware property decoding to `decode_unsubscribe()`.

---

### C4: CONNACK V5 property length always required

**Location:** `codec.rs:1401-1417`

**Spec:** MQTT 5.0 SS 3.2.2.3 -- CONNACK MUST include Property Length even if there are no
properties (value 0).

**Problem:** `encode_connack()` only writes properties if `compute_properties_size() > 0`. For
V5 connections, this omits the property length byte entirely, causing V5 clients to misparse
the CONNACK (they expect at least a 0x00 property length byte after the return code).

**Fix:** For V5 connections, always write the property length (write `0x00` when empty). This
requires the encoder to know the protocol version, or the session to always populate properties
for V5 CONNACK.

---

### C5: PUBACK/PUBREC/PUBREL/PUBCOMP V5 encoding incomplete

**Location:** `codec.rs:1454-1476`

**Spec:** MQTT 5.0 SS 3.4-3.7 -- For V5, these packets include optional Reason Code (1 byte)
and Properties after the Packet Identifier. If Reason Code is 0x00 (Success) and there are
no properties, the Reason Code and Property Length MAY be omitted.

**Problem:** All four encode functions write only the 2-byte packet identifier, ignoring
`reason_code` and `properties` fields entirely. For V5, if the server needs to send a
non-success reason code (e.g., Packet Identifier In Use, Quota Exceeded), the client never
sees it.

**Fix:** Check if reason_code is non-zero or properties are non-empty; if so, write
reason_code + property length + properties.

---

### C6: SUBACK/UNSUBACK V5 properties not encoded

**Location:** `codec.rs:1504-1540`

**Spec:** MQTT 5.0 SS 3.9.2.1, SS 3.11.2.1 -- SUBACK and UNSUBACK include properties (Reason
String, User Properties) between the Packet Identifier and the payload (return codes).

**Problem:** `encode_suback()` and `encode_unsuback()` write packet_id + return_codes with no
property length or properties. V5 clients expect a property length byte between packet_id
and return codes.

**Fix:** For V5, write property length (0 if none) between packet_id and return codes.

---

### C7: SUBSCRIBE V5 properties not encoded

**Location:** `codec.rs:1478-1502`

**Spec:** MQTT 5.0 SS 3.8.2.1 -- SUBSCRIBE includes properties (Subscription Identifier,
User Properties) between Packet Identifier and topic filters.

**Problem:** `encode_subscribe()` writes packet_id then immediately iterates topic filters
with no property length. V5 receivers expect a property length byte.

**Fix:** For V5, write property length between packet_id and filters.

---

## Major Gaps

These gaps cause incorrect behavior or spec non-compliance but don't corrupt the wire protocol.

### M1: No version-aware codec

**Location:** `codec.rs` (all decode/encode functions)

**Spec:** MQTT 3.1.1 and 5.0 share wire format for all packets except that V5 adds a
Properties section to every packet type (except PINGREQ/PINGRESP).

**Problem:** `decode_packet()` and `encode_packet()` have no `ProtocolVersion` parameter.
The codec cannot distinguish V3.1.1 from V5 packets. This is the root cause of C1-C7.

**Fix:** Add a `ProtocolVersion`-aware decode/encode API:
```rust
pub fn decode_packet_versioned(buf: &[u8], version: ProtocolVersion)
    -> Result<(MqttPacket, usize), CodecError>
pub fn encode_packet_versioned(packet: &MqttPacket, version: ProtocolVersion, buf: &mut BytesMut)
```
The existing version-unaware functions can remain for CONNECT (which is always version-unaware
since version isn't known yet).

---

### M2: Fixed header flags not validated

**Location:** `codec.rs:564-617`

**Spec:** MQTT 3.1.1 SS 2.2.2 / MQTT 5.0 SS 2.1.3 -- Each packet type has required fixed header
flag values:
- CONNECT, CONNACK, PUBACK, PUBREC, PUBCOMP, SUBACK, UNSUBACK, PINGREQ, PINGRESP,
  DISCONNECT, AUTH: flags MUST be 0
- PUBREL, SUBSCRIBE, UNSUBSCRIBE: flags MUST be 0x02 (bit 1 set)
- PUBLISH: flags encode DUP/QoS/Retain (variable)

**Problem:** `decode_packet()` extracts flags but only uses them for PUBLISH. Invalid flags
on other packet types are silently accepted, violating the spec requirement to disconnect
with protocol error.

**Fix:** Add flag validation in `decode_packet()` before dispatching.

---

### M3: Topic name validation missing

**Location:** `session.rs:551-683`

**Spec:** MQTT 3.1.1 SS 4.7.1 / MQTT 5.0 SS 4.7.1 -- Topic names used in PUBLISH MUST NOT
contain wildcard characters (`+` or `#`).

**Problem:** `handle_publish()` accepts any topic string from inbound PUBLISH without
validating against wildcard characters. A client publishing to `sensor/+/data` would be
accepted.

**Fix:** Validate topic name in `handle_publish()` -- reject with DISCONNECT (V5) or close
connection (V3.1.1) if wildcards are present.

---

### M4: Topic filter validation missing

**Location:** `session.rs:809-898`

**Spec:** MQTT 3.1.1 SS 4.7.1 / MQTT 5.0 SS 4.7.1:
- `+` must occupy an entire level (e.g., `a/+/b` valid, `a/b+/c` invalid)
- `#` must be the last character and must be preceded by `/` (or be the only character)
- Empty topic filter is invalid
- Topic filter must not contain null characters

**Problem:** `handle_subscribe()` accepts any filter string without structural validation.
Malformed filters like `a/b#c`, `a/+b`, or empty strings are accepted.

**Fix:** Add topic filter validation function and call it in `handle_subscribe()`.

---

### M5: Client ID handling incomplete

**Location:** `session.rs:450-541`

**Spec:**
- MQTT 3.1.1 SS 3.1.3.1: If client ID is empty AND clean_session=true, server MUST accept
  and assign a unique ID. If empty AND clean_session=false, server MUST reject with 0x02.
- MQTT 5.0 SS 3.1.3.1: Server MUST accept empty client IDs. If assigned, MUST return it via
  the `Assigned Client Identifier` property in CONNACK.

**Problem:** `handle_connect()` uses the client ID as-is, including empty strings. This
creates collisions (multiple clients with empty ID share queues) and doesn't return
`assigned_client_identifier` in V5 CONNACK.

**Fix:** Generate UUID-based client ID when empty; for V5, set
`properties.assigned_client_identifier` in CONNACK.

---

### M6: No Local option not enforced

**Location:** `server.rs:584-696`

**Spec:** MQTT 5.0 SS 3.8.3.1 -- If No Local is set to 1, the server MUST NOT forward
Application Messages published by the same client to that subscription.

**Problem:** `TopicFilter.no_local` is parsed and stored but the delivery loop
(`deliver_outbound`) never checks it. A client with No Local=1 receives its own messages.

**Fix:** In `deliver_outbound()`, skip messages where the publisher's session_id matches
the subscriber's session_id when no_local is set.

---

### M7: Retain As Published option not enforced

**Location:** `server.rs:584-696`

**Spec:** MQTT 5.0 SS 3.8.3.1 -- If Retain As Published is 0, the server MUST set the RETAIN
flag to 0 on outbound PUBLISH. If 1, the server MUST preserve the original RETAIN flag.

**Problem:** `retain_as_published` is parsed and stored but the delivery loop always sends
`retain: false` for normal (non-retained) delivery, which happens to be correct for
retain_as_published=0 but is wrong when the original message had retain=1 and the option is 1.

**Fix:** Track the original retain flag in the FlatMessage (as a header) and apply
retain_as_published logic in the delivery path.

---

### M8: Retain Handling option not fully enforced

**Location:** `server.rs:1047-1108`

**Spec:** MQTT 5.0 SS 3.8.3.1 -- Retain Handling controls when retained messages are sent:
- 0: Send retained messages at subscribe time
- 1: Send retained messages at subscribe time only if subscription doesn't already exist
- 2: Do not send retained messages at subscribe time

**Problem:** `deliver_retained_on_subscribe()` always delivers retained messages regardless
of the `retain_handling` value. Option 1 (new subscription only) and option 2 (never) are
not respected.

**Fix:** Check `retain_handling` in `deliver_retained_on_subscribe()`. For option 1, check
if the filter already existed before this subscribe. For option 2, skip entirely.

---

### M9: Packet Identifier reuse not checked

**Location:** `session.rs:327-334`

**Spec:** MQTT 3.1.1 SS 2.3.1 / MQTT 5.0 SS 2.2.1 -- Packet Identifiers MUST NOT be reused
while there is an outstanding message with the same identifier.

**Problem:** `alloc_packet_id()` increments and wraps around without checking if the ID is
currently in the `qos1_inflight`, `qos2_inbound`, or `qos2_outbound` maps. Under heavy load
with many in-flight messages, a reused packet ID could corrupt the QoS handshake.

**Fix:** Loop `alloc_packet_id()` until an unused ID is found. Add a safety check (max
iterations = 65535) to avoid infinite loops.

---

### M10: Maximum Packet Size not enforced

**Location:** `session.rs`, `server.rs`

**Spec:** MQTT 5.0 SS 3.1.2.11.4 -- The client uses Maximum Packet Size to limit packets the
server may send. The server MUST NOT send packets exceeding this size.
Also, MQTT 5.0 SS 3.2.2.3.5 -- The server uses Maximum Packet Size to limit packets it accepts.

**Problem:**
1. Client's `maximum_packet_size` from CONNECT is parsed but outbound packets are never
   checked against it
2. Server's configured `max_packet_size` is stored but inbound packets are not validated
   against it (only the absolute 256MB limit is checked in `decode_remaining_length`)

**Fix:**
1. After encoding outbound PUBLISH, check size against client's `maximum_packet_size`;
   skip delivery if exceeded
2. In `connection_loop`, validate inbound packet size against server's `max_packet_size`;
   send DISCONNECT with `PACKET_TOO_LARGE` reason code if exceeded

---

### M11: Will message on V5 DISCONNECT not handled correctly

**Location:** `session.rs:964-998`

**Spec:** MQTT 5.0 SS 3.14.4 -- If the client sends DISCONNECT with reason code 0x04
(Disconnect with Will Message), the server MUST publish the Will Message. For all other
normal disconnect reason codes (0x00), the will is NOT published.

**Problem:** `handle_disconnect()` unconditionally clears `self.will = None`, meaning the
will is never published on clean disconnect regardless of the V5 reason code.

**Fix:** For V5, check the DISCONNECT reason code. If 0x04, publish the will before clearing.
The `handle_disconnect` method needs the `Disconnect` packet to check the reason code.

---

### M12: Server-initiated DISCONNECT not implemented

**Spec:** MQTT 5.0 SS 3.14 -- The server MUST send DISCONNECT with appropriate reason code
before closing the connection for protocol errors.

**Problem:** On protocol violations (invalid packet, malformed data), the server just drops
the TCP connection. V5 clients expect a DISCONNECT packet with a reason code explaining why.

**Fix:** Before closing on error, encode and send a DISCONNECT packet with the appropriate
reason code (MALFORMED_PACKET, PROTOCOL_ERROR, etc.).

---

### M13: `$SYS` and `$`-prefixed topics in wildcard subscriptions

**Location:** `server.rs:1166-1188`

**Spec:** MQTT 3.1.1 SS 4.7.2 / MQTT 5.0 SS 4.7.2 -- Topics beginning with `$` are server-
specific and MUST NOT be matched by subscriptions starting with `#` or `+`.

**Problem:** `mqtt_topic_matches_filter()` does not have special handling for `$`-prefixed
topics. A subscription to `#` would match `$SYS/broker/clients`, and `+/info` would match
`$SYS/info`, both violating the spec.

**Fix:** In `mqtt_topic_matches_filter()`, if the topic starts with `$` and the filter does
not start with `$`, return false.

---

### M14: Password without Username not validated for 3.1.1

**Location:** `codec.rs:726-810`

**Spec:** MQTT 3.1.1 SS 3.1.2.9 -- If the Password Flag is set, the User Name Flag MUST also
be set. (V5 removes this restriction.)

**Problem:** `decode_connect()` accepts password=true with username=false for all versions.

**Fix:** For V3.1.1, validate that username flag is set when password flag is set.

---

### M15: Will flags validation missing

**Location:** `codec.rs:726-810`, `types.rs:139-178`

**Spec:** MQTT 3.1.1 SS 3.1.2.7-3.1.2.8 -- If Will Flag is 0, then Will QoS MUST be 0 and
Will Retain MUST be 0.

**Problem:** `decode_connect()` / `ConnectFlags::from_byte()` does not validate that
will_qos and will_retain are 0 when will=false.

**Fix:** Add validation in `ConnectFlags::from_byte()`.

---

### M16: Keep-alive=0 handling incorrect for V5

**Location:** `session.rs:458-462`

**Spec:** MQTT 5.0 SS 3.1.2.10 -- If the client sends keep_alive=0, the server MUST NOT
disconnect the client for inactivity. (The server MAY override via Server Keep Alive property.)

**Problem:** `handle_connect()` replaces keep_alive=0 with `default_keep_alive` for all
versions. For V5, keep_alive=0 is a valid explicit request for no timeout.

**Fix:** For V5, if client sends keep_alive=0, respect it (set keep_alive=0, or use
server_keep_alive property to override and inform the client).

---

### M17: AUTH packet not implemented (MQTT 5.0)

**Spec:** MQTT 5.0 SS 3.15 -- AUTH enables enhanced authentication using challenge-response
(SASL-like). The server MUST support receiving AUTH packets.

**Problem:** AUTH is defined as a fieldless enum variant (`MqttPacket::Auth`). The codec
writes an empty packet. The session handler logs a warning and disconnects.

**Fix:** Full implementation requires:
1. AUTH packet fields (reason_code, properties with authentication_method/data)
2. Codec decode/encode for AUTH
3. Pluggable authentication provider interface
4. State machine for multi-step auth flows

**Scope:** This is a significant feature. Can be deferred but must disconnect cleanly with
reason code 0x8C (Bad authentication method) -- which is currently done.

---

## Minor Gaps

These are correctness issues, edge cases, or quality improvements.

### m1: UTF-8 string validation incomplete

**Location:** `codec.rs:118-132`

**Spec:** MQTT 3.1.1 SS 1.5.3 / MQTT 5.0 SS 1.5.4 -- UTF-8 Encoded Strings MUST NOT include
the null character U+0000 and SHOULD NOT include U+0001-U+001F (control characters) or
U+D800-U+DFFF (lone surrogates) or U+FFFE-U+FFFF.

**Problem:** `read_mqtt_string()` uses `std::str::from_utf8()` which validates UTF-8 encoding
but does NOT reject null characters (U+0000) or other MQTT-prohibited code points.

**Fix:** After UTF-8 validation, scan for prohibited characters:
```rust
if s.contains('\0') { return Err(CodecError::InvalidUtf8); }
```

---

### m2: Duplicate property detection missing

**Location:** `codec.rs:217-347`

**Spec:** MQTT 5.0 SS 2.2.2.2 -- Properties that are not specified as allowing multiple
instances MUST NOT appear more than once. It is a Protocol Error if received.

**Problem:** `read_properties()` silently overwrites duplicate properties (e.g., two
`message_expiry_interval` entries). Only `user_properties` correctly accumulates duplicates.

**Fix:** Track which single-instance properties have been seen; error on duplicates.

---

### m3: Subscription Identifier value 0 not rejected

**Location:** `codec.rs:250-253`

**Spec:** MQTT 5.0 SS 3.8.2.1.2 -- Subscription Identifier MUST be 1-268,435,455. A value
of 0 is a Protocol Error.

**Problem:** `read_properties()` accepts subscription_identifier=0 without error.

**Fix:** Validate `value > 0` after reading subscription_identifier.

---

### m4: Topic Alias value 0 not rejected

**Location:** `codec.rs:306-309`

**Spec:** MQTT 5.0 SS 3.3.2.3.4 -- Topic Alias value of 0 is a Protocol Error.

**Problem:** `read_properties()` accepts topic_alias=0 without error.

**Fix:** Validate `value > 0` after reading topic_alias.

---

### m5: Receive Maximum value 0 not rejected

**Location:** `codec.rs:298-301`

**Spec:** MQTT 5.0 SS 3.1.2.11.3 -- Receive Maximum MUST be > 0. A value of 0 is a
Protocol Error.

**Problem:** `read_properties()` accepts receive_maximum=0 without error.

**Fix:** Validate `value > 0` after reading receive_maximum.

---

### m6: Protocol name "MQIsdp" accepted but MQTT 3.1 not supported

**Location:** `codec.rs:729`

**Spec:** MQTT 3.1 uses protocol name "MQIsdp" with protocol level 3.

**Problem:** `decode_connect()` accepts "MQIsdp" as a valid protocol name but then requires
protocol level 4 or 5. If a 3.1 client connects with "MQIsdp" and level 3, the error
message says "unsupported protocol version" which is correct, but accepting "MQIsdp" with
level 4 would create ambiguity.

**Fix:** Only accept "MQTT" as valid protocol name. Reject "MQIsdp" with
`UNSUPPORTED_PROTOCOL_VERSION` CONNACK.

---

### m7: Outbound topic alias sends full topic on existing alias

**Location:** `server.rs`, `session.rs:1356-1373`

**Spec:** MQTT 5.0 SS 3.3.2.3.4 -- When using an existing Topic Alias mapping, the Topic Name
in the PUBLISH MUST be zero-length.

**Problem:** `encode_publish_from_flat()` always writes the full topic string. When an alias
was previously established, the topic should be zero-length (2-byte prefix = 0x00 0x00) to
save bandwidth. Currently the alias is sent but the full topic is also sent, which is valid
for new mappings (establishing the alias) but wasteful for subsequent uses.

**Fix:** If `resolve_outbound_topic_alias()` returns an existing alias (not newly assigned),
write an empty topic string. Distinguish new vs existing alias in the return value.

---

### m8: QoS downgrade in SUBACK

**Location:** `session.rs:809-898`

**Spec:** MQTT 3.1.1 SS 3.9.3 / MQTT 5.0 SS 3.9.3 -- The server MAY grant a lower QoS than
requested. The granted QoS MUST be returned in SUBACK.

**Problem:** `handle_subscribe()` always grants the requested QoS. If the server has a
maximum_qos setting (e.g., max QoS 1), a QoS 2 request should be downgraded.

**Fix:** Clamp requested QoS to server's maximum_qos in `handle_subscribe()`.

---

### m9: Message ordering not guaranteed

**Location:** `server.rs:584-696`

**Spec:** MQTT 3.1.1 SS 4.6 / MQTT 5.0 SS 4.6 -- Messages delivered to a client MUST be in
order for each QoS level.

**Problem:** The polling delivery model (`deliver_outbound`) iterates subscriptions and
delivers batch_size messages per subscription per poll. Multiple subscriptions to the same
topic could receive messages out of order. Within a single subscription, ordering depends
on bisque-mq queue ordering.

**Fix:** Ensure bisque-mq queues maintain FIFO ordering (they should by design). For
multi-subscription scenarios, document the ordering guarantees.

---

### m10: Session expiry not implemented

**Location:** `session.rs`

**Spec:** MQTT 5.0 SS 3.1.2.11.2 -- Session Expiry Interval controls how long the session
state is maintained after disconnect. 0 = delete on disconnect. 0xFFFFFFFF = never expire.

**Problem:** `session_expiry_interval` is stored but never acted on. Sessions are always
destroyed on disconnect (equivalent to always 0).

**Fix:** Implement a session store that retains subscription state and inflight messages
for the configured expiry interval. On reconnect with the same client ID, restore the
session state.

---

### m11: CONNECT second packet validation

**Location:** `server.rs:912-1020`

**Spec:** MQTT 3.1.1 SS 3.1.0 / MQTT 5.0 SS 3.1 -- A client MUST send CONNECT as the first
packet. After CONNECT, a second CONNECT MUST be treated as a Protocol Error.

**Problem:** `process_inbound_packet()` falls through to `session.process_packet()` for
unhandled types, which would accept a second CONNECT packet and re-initialize the session.

**Fix:** In `process_inbound_packet()`, check for `MqttPacket::Connect` and send
DISCONNECT with PROTOCOL_ERROR.

---

### m12: QoS 2 duplicate PUBLISH handling

**Location:** `session.rs:639-650`

**Spec:** MQTT 3.1.1 SS 4.3.3 / MQTT 5.0 SS 4.3.3 -- If a PUBLISH QoS 2 is received with a
Packet Identifier that already exists in the inbound QoS 2 state (PUBREC already sent),
the server MUST respond with PUBREC again but MUST NOT re-process the message.

**Problem:** `handle_publish()` for QoS 2 always inserts into `qos2_inbound` and publishes
the message, even if the packet_id already exists. This could cause duplicate message
delivery.

**Fix:** Check if packet_id exists in `qos2_inbound` before processing. If it does (state
is `PubRecSent`), send PUBREC but skip the publish.

---

---

## Implementation Plan

### Phase 1: Version-Aware Codec (Critical -- fixes C1-C7, M1)

**Goal:** Make the codec protocol-version-aware so V5 properties are correctly decoded
and encoded for all packet types.

**Priority:** P0 Critical -- blocks all MQTT 5.0 functionality

**Tasks:**

1. **Add version-aware decode API** (`codec.rs`)
   - Add `decode_packet_versioned(buf: &[u8], version: ProtocolVersion)` and
     `decode_packet_from_bytes_versioned(buf: &Bytes, version: ProtocolVersion)`
   - Pass version through to all per-packet decode functions
   - Keep existing `decode_packet()` for CONNECT (version unknown)

2. **Fix PUBLISH V5 decode** (C1)
   - In `decode_publish()` and `decode_publish_zero_copy()`: if V5, read properties
     before consuming payload
   - Properties section sits after packet_id, before payload

3. **Fix SUBSCRIBE V5 decode** (C2)
   - In `decode_subscribe()`: if V5, read properties after packet_id, before filters

4. **Fix UNSUBSCRIBE V5 decode** (C3)
   - In `decode_unsubscribe()`: if V5, read properties after packet_id, before filters

5. **Fix SUBACK/UNSUBACK V5 decode**
   - In `decode_suback()`: if V5, read properties after packet_id, before return codes
   - In `decode_unsuback()`: if V5, read properties after packet_id, before reason codes

6. **Add version-aware encode API** (`codec.rs`)
   - Add `encode_packet_versioned(packet, version, buf)`
   - Pass version to all per-packet encode functions

7. **Fix CONNACK V5 encode** (C4)
   - Always write property length for V5 (even if 0)

8. **Fix PUBACK/PUBREC/PUBREL/PUBCOMP V5 encode** (C5)
   - Write reason_code + properties when V5 and (reason_code != 0x00 or properties non-empty)

9. **Fix SUBACK/UNSUBACK V5 encode** (C6)
   - Write property length between packet_id and return codes for V5

10. **Fix SUBSCRIBE V5 encode** (C7)
    - Write property length between packet_id and filters for V5

11. **Update server.rs callers**
    - After CONNECT, use versioned decode/encode for all subsequent packets
    - Store protocol version in connection state for use with codec calls

12. **Tests**
    - Add V5 roundtrip tests for every packet type with properties
    - Add cross-version tests (encode V5, decode V5; encode V3.1.1, decode V3.1.1)
    - Test edge case: V5 PUBLISH with properties + empty payload

**Estimated scope:** ~800-1000 lines changed across codec.rs and server.rs

---

### Phase 2: Protocol Validation (Critical -- fixes M2-M5, M14-M16, m1-m6, m11-m12)

**Goal:** Add all MQTT normative validation required by the spec.

**Priority:** P0 Critical -- required for conformance

**Tasks:**

1. **Fixed header flag validation** (M2)
   - Add `validate_fixed_header_flags(packet_type, flags)` function
   - Call in `decode_packet()` after parsing fixed header

2. **Topic name validation** (M3)
   - Add `validate_topic_name(topic: &str) -> Result<(), CodecError>` -- reject `+`, `#`, empty, null
   - Call in `handle_publish()`

3. **Topic filter validation** (M4)
   - Add `validate_topic_filter(filter: &str) -> Result<(), CodecError>` -- validate wildcard positions
   - Call in `handle_subscribe()`

4. **Client ID handling** (M5)
   - Generate UUID when client ID is empty (V3.1.1: only with clean_session=true)
   - For V5: return `assigned_client_identifier` in CONNACK properties

5. **Password without Username** (M14)
   - Validate for V3.1.1 only

6. **Will flags validation** (M15)
   - In `ConnectFlags::from_byte()`, reject will_qos>0 or will_retain=true when will=false

7. **Keep-alive=0 for V5** (M16)
   - Don't override keep_alive=0 for V5 connections

8. **UTF-8 prohibited characters** (m1)
   - Reject null U+0000 in MQTT strings

9. **Duplicate property detection** (m2)
   - Track seen properties in `read_properties()`

10. **Property value validation** (m3, m4, m5)
    - subscription_identifier > 0, topic_alias > 0, receive_maximum > 0

11. **Reject MQIsdp** (m6)
    - Only accept "MQTT" protocol name

12. **Second CONNECT rejection** (m11)
    - In `process_inbound_packet()`, reject CONNECT after initial

13. **QoS 2 duplicate detection** (m12)
    - Skip publish for duplicate QoS 2 packet IDs

14. **Tests**
    - Test each validation (malformed inputs rejected, valid inputs accepted)

**Estimated scope:** ~400-500 lines

---

### Phase 3: V5 Subscription Options & Delivery (Major -- fixes M6-M8, M13, m7-m9)

**Goal:** Enforce all MQTT 5.0 subscription options in the delivery path.

**Priority:** P1 Major

**Tasks:**

1. **No Local enforcement** (M6)
   - Add `no_local: bool` to `SubscriptionMapping`
   - In `deliver_outbound()`, skip messages from same session_id when no_local=true
   - Requires tracking publisher session_id in FlatMessage (header `mqtt.publisher_id`)

2. **Retain As Published enforcement** (M7)
   - Track original retain flag in FlatMessage header
   - Apply retain_as_published logic in delivery

3. **Retain Handling enforcement** (M8)
   - Check `retain_handling` in `deliver_retained_on_subscribe()`
   - Track subscription pre-existence for option 1

4. **Topic alias empty topic on existing alias** (m7)
   - Return `(alias, is_new)` from `resolve_outbound_topic_alias()`
   - Write empty topic when alias is already established

5. **QoS downgrade** (m8)
   - Add server `maximum_qos` config
   - Clamp in `handle_subscribe()`

6. **$SYS topic filtering** (M13)
   - In `mqtt_topic_matches_filter()`, check for `$` prefix

7. **Packet ID reuse check** (M9)
   - Loop in `alloc_packet_id()` to skip in-use IDs

8. **Tests**
   - Test no_local, retain_as_published, retain_handling with all values
   - Test $SYS filtering

**Estimated scope:** ~300-400 lines

---

### Phase 4: Server-Initiated DISCONNECT & Packet Size (Major -- fixes M10-M12)

**Goal:** Proper error reporting and packet size enforcement.

**Priority:** P1 Major

**Tasks:**

1. **Server-initiated DISCONNECT** (M12)
   - Add `send_disconnect_and_close()` helper in server.rs
   - Use for protocol errors, keep-alive timeout, auth failures
   - Include reason code and optional reason string

2. **Maximum Packet Size enforcement** (M10)
   - Inbound: validate packet size against server config after parsing fixed header
   - Outbound: check encoded size against client's maximum_packet_size before sending

3. **Will message on V5 DISCONNECT** (M11)
   - Pass Disconnect packet to `handle_disconnect()`
   - Check reason code 0x04

4. **Tests**
   - Test server DISCONNECT with various reason codes
   - Test oversized packet rejection
   - Test will-on-disconnect V5

**Estimated scope:** ~200-300 lines

---

### Phase 5: Enhanced Authentication (M17)

**Goal:** Implement AUTH packet and pluggable authentication.

**Priority:** P2 Major

**Tasks:**

1. **AUTH packet types** -- reason_code + properties fields
2. **AUTH codec** -- full decode/encode
3. **Auth provider trait** -- `async fn authenticate(method, data) -> AuthResult`
4. **SASL-like state machine** -- CONNECT -> AUTH -> AUTH -> CONNACK
5. **Username/password validation** -- basic auth provider
6. **Tests**

**Estimated scope:** ~500-600 lines (new `auth.rs` module)

---

### Phase 6: Session Persistence (m10)

**Goal:** Persist session state across reconnects.

**Priority:** P2 Major

**Tasks:**

1. **Session store trait** -- save/load subscription state, inflight messages
2. **In-memory implementation** -- HashMap-based (for dev/test)
3. **bisque-mq topic implementation** -- persist via replicated topic
4. **Session expiry timer** -- clean up expired sessions
5. **Reconnect flow** -- restore session on CONNECT with clean_start=false
6. **Tests**

**Estimated scope:** ~600-800 lines (new `session_store.rs` module)

---

### Phase 7: Transport (TLS, WebSocket)

**Goal:** Support TLS and WebSocket transports.

**Priority:** P3 Minor

**Tasks:**

1. **TLS** -- tokio-rustls integration, port 8883
2. **WebSocket** -- tokio-tungstenite, subprotocol "mqtt"
3. **Tests**

**Estimated scope:** ~300-400 lines

---

## Progress Tracker

| Phase | Description | Priority | Status | Gaps Addressed |
|-------|-------------|----------|--------|----------------|
| 1 | Version-Aware Codec | P0 Critical | **DONE** | C1-C7, M1 |
| 2 | Protocol Validation | P0 Critical | **DONE** | M2-M5, M14-M16, m1-m6, m11-m12 |
| 3 | V5 Subscription Options | P1 Major | **DONE** | M6-M9, M13, m7-m9 |
| 4 | Server DISCONNECT & Packet Size | P1 Major | **DONE** | M10-M12 |
| 5 | Enhanced Authentication | P2 Major | **DONE** | M17 |
| 6 | Session Persistence | P2 Major | **DONE** | m10 |
| 7 | Transport (TLS/WebSocket) | P3 Minor | **DONE** | -- |
| 8 | Final Audit Fixes | P0 Critical | **DONE** | UTF-8 control chars, CONNACK ack_flags, session restore, sub ID per-queue, request_response_information, session expiry sweep |
