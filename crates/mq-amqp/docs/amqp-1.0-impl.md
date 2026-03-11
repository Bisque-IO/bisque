# AMQP 1.0 Implementation — Gap Analysis & Implementation Plan

**Spec**: OASIS AMQP v1.0 (29 October 2012)
**Crate**: `bisque-mq-amqp` (~15K LOC)
**Date**: 2026-03-11
**Last Updated**: 2026-03-12

---

## 1. Current Implementation Summary

| Module | Lines | Responsibility |
|--------|-------|----------------|
| `types.rs` | ~2,050 | Protocol types, performatives, errors, SASL, lifetime policies, message state |
| `codec.rs` | ~4,600 | Binary encoding/decoding, frame handling |
| `connection.rs` | ~5,100 | Connection lifecycle, SASL, session mgmt, transactions, exactly-once |
| `link.rs` | ~1,650 | Link state, flow control, delivery handling |
| `broker.rs` | ~570 | Broker trait, action queue |
| `server.rs` | ~800 | TCP/TLS server, per-connection loop |
| `transport.rs` | ~280 | AmqpStream enum (TCP/TLS), TlsConfig, cert extraction |

---

## 2. Gap Analysis by Spec Section

### 2.1 Type System (`amqp-core-types`)

| Type | Format Code(s) | Status | Notes |
|------|---------------|--------|-------|
| null | 0x40 | ✅ | |
| boolean | 0x56, 0x41, 0x42 | ✅ | |
| ubyte | 0x50 | ✅ | |
| ushort | 0x60 | ✅ | |
| uint | 0x70, 0x52, 0x43 | ✅ | All three encodings |
| ulong | 0x80, 0x53, 0x44 | ✅ | All three encodings |
| byte | 0x51 | ✅ | |
| short | 0x61 | ✅ | |
| int | 0x71, 0x54 | ✅ | Both encodings |
| long | 0x81, 0x55 | ✅ | Both encodings |
| float | 0x72 | ✅ | |
| double | 0x82 | ✅ | |
| decimal32 | 0x74 | ✅ Fixed | Opaque [u8; 4] representation |
| decimal64 | 0x84 | ✅ Fixed | Opaque [u8; 8] representation |
| decimal128 | 0x94 | ✅ Fixed | Opaque [u8; 16] representation |
| char | 0x73 | ✅ Fixed | UTF-32BE Unicode character |
| timestamp | 0x83 | ✅ | |
| uuid | 0x98 | ✅ | |
| binary | 0xA0, 0xB0 | ✅ | vbin8 + vbin32 |
| string | 0xA1, 0xB1 | ✅ | str8 + str32 |
| symbol | 0xA3, 0xB3 | ✅ | sym8 + sym32 |
| list | 0x45, 0xC0, 0xD0 | ✅ | list0 + list8 + list32 |
| map | 0xC1, 0xD1 | ✅ | map8 + map32 |
| array | 0xE0, 0xF0 | ✅ | array8 + array32 |
| described | 0x00 | ✅ | |

~~**Gap T1**~~ — ✅ **FIXED**: `decimal32`, `decimal64`, `decimal128` added as opaque byte arrays.
~~**Gap T2**~~ — ✅ **FIXED**: `char` (0x73) added as `AmqpValue::Char(char)`.

### 2.2 Transport — Connection (`amqp-core-transport`)

#### Connection State Machine

| Spec State | Impl Phase | Status |
|------------|-----------|--------|
| START | (implicit) | ✅ |
| HDR_RCVD | AwaitingHeader | ✅ Conflated |
| HDR_SENT | AwaitingHeader | ✅ Conflated |
| HDR_EXCH | AwaitingHeader | ✅ Conflated |
| OPEN_PIPE | — | ✅ Supported via loop-based processing |
| OC_PIPE | — | ✅ Supported via loop-based processing |
| OPEN_RCVD | AwaitingOpen | ✅ |
| OPEN_SENT | AwaitingOpen | ✅ |
| CLOSE_PIPE | — | ✅ Supported via loop-based processing |
| OPENED | Open | ✅ |
| CLOSE_RCVD | Closing | ✅ |
| CLOSE_SENT | Closing | ✅ |
| DISCARDING | Closing | ✅ Fixed — silently drops non-CLOSE frames |
| END | Closed | ✅ |

~~**Gap C1**~~ — ✅ **FIXED**: Pipelining supported inherently by the `process()` loop design — all buffered frames are processed sequentially in a single call, so OPEN/BEGIN/ATTACH can be pipelined after the header.

~~**Gap C2**~~ — ✅ **FIXED**: When `phase == Closing`, `dispatch_performative()` silently drops all non-CLOSE frames.

#### OPEN Performative — ✅ All fields supported.

~~**Gap C3**~~ — ✅ **FIXED**: `max-frame-size` default changed from 65536 to `u32::MAX` per spec. The negotiation logic takes the minimum of both sides, so we defer to whatever the client proposes.

~~**Gap C4**~~ — ✅ **FIXED**: Capability negotiation enforcement — `handle_open()` loops over `desired_capabilities` and warns for any not in our offered set (`ANONYMOUS-RELAY`, `DELAYED-DELIVERY`, `SHARED-SUBS`, `SOLE-CONNECTION-FOR-CONTAINER`). Connection still opens per spec (desired is a hint, not mandatory).

~~**Gap C5**~~ — ✅ **FIXED**: Version negotiation sends supported header on mismatch.

### 2.3 Transport — Sessions

#### Session State Machine

| Spec State | Status | Notes |
|------------|--------|-------|
| UNMAPPED | ✅ Implicit | Sessions only exist in HashMap when mapped |
| BEGIN_SENT | ✅ Implicit | Server creates session on receipt of BEGIN |
| BEGIN_RCVD | ✅ | |
| MAPPED | ✅ | |
| END_SENT | ✅ | |
| END_RCVD | ✅ | |
| DISCARDING | ✅ Fixed | `discarding` flag silently drops frames |

~~**Gap S1**~~ — ✅ **FIXED**: Session `discarding` flag set after END with error; frames silently dropped.

#### Session Flow Control — ✅ All parameters tracked and enforced.

~~**Gap S2**~~ — ✅ **FIXED**: `remote_incoming_window` tracked; sends blocked when zero.
~~**Gap S3**~~ — ✅ **FIXED**: Window updated per spec formula on FLOW receipt.

### 2.4 Transport — Links

#### ATTACH — ✅ All fields supported.

~~**Gap L1**~~ — ✅ **FIXED**: Sender-role ATTACH without `initial-delivery-count` rejected with `amqp:invalid-field`.

#### FLOW — ✅ All fields supported, including echo.

~~**Gap L2**~~ — ✅ Already implemented: `echo=true` triggers FLOW response.

#### TRANSFER — ✅ All fields supported.

~~**Gap L3**~~ — ✅ **FIXED**: Delivery resume on reattach — unsettled deliveries preserved when terminus is durable, reconciled against peer's unsettled map. Resume transfers respond with Received state.

~~**Gap L4**~~ — ✅ Already implemented: Aborted transfers clear partial delivery buffer.

#### Settlement Modes — ✅ All modes supported.

~~**Gap L5**~~ — ✅ **FIXED**: Exactly-once settlement (`rcv-settle-mode=second`):
- Phase 1: Receiver sends Disposition with `settled=false` and `state=Accepted`
- Unsettled delivery tracks pending state in `UnsettledDelivery.state` field
- Phase 2: When sender sends Disposition `settled=true`, receiver settles locally and sends final Disposition `settled=true`
- Batchable optimization skipped in second-mode (always sends immediately)
- `rcv-settle-mode=first` continues to settle immediately as before

### 2.5 Transport — Error Conditions — ✅ All conditions defined.

| Condition | Status |
|-----------|--------|
| amqp:internal-error | ✅ |
| amqp:not-found | ✅ |
| amqp:unauthorized-access | ✅ |
| amqp:decode-error | ✅ |
| amqp:resource-limit-exceeded | ✅ |
| amqp:not-allowed | ✅ |
| amqp:invalid-field | ✅ |
| amqp:not-implemented | ✅ |
| amqp:resource-locked | ✅ |
| amqp:precondition-failed | ✅ |
| amqp:resource-deleted | ✅ |
| amqp:frame-size-too-small | ✅ Fixed |
| amqp:unknown-error | ✅ Fixed |
| amqp:policy-violation | ✅ Fixed |
| amqp:connection:forced | ✅ |
| amqp:connection:framing-error | ✅ |
| amqp:connection:redirect | ✅ Fixed |
| amqp:connection:reset | ✅ Fixed |
| amqp:session:window-violation | ✅ |
| amqp:session:errant-link | ✅ |
| amqp:session:handle-in-use | ✅ |
| amqp:session:unattached-handle | ✅ |
| amqp:session:undeclared-link-handle | ✅ Fixed |
| amqp:link:detach-forced | ✅ |
| amqp:link:transfer-limit-exceeded | ✅ |
| amqp:link:message-size-exceeded | ✅ |
| amqp:link:redirect | ✅ |
| amqp:link:stolen | ✅ |
| amqp:link:source-rank-too-high | ✅ Fixed |
| amqp:link:ambiguous-commit | ✅ Fixed |
| amqp:transaction:unknown-id | ✅ Fixed |
| amqp:transaction:rollback | ✅ Fixed |
| amqp:transaction:timeout | ✅ Fixed |

~~**Gap E1**~~ — ✅ **FIXED**: All 11 missing error condition constants added.

### 2.6 Messaging — Message Sections — ✅ All sections supported.

~~**Gap M1**~~ — ✅ Already handled: message-id polymorphism works via `AmqpValue` enum.
~~**Gap M2**~~ — ✅ Already handled: amqp-sequence body sections decoded.
~~**Gap M3**~~ — ✅ Already handled: amqp-value body sections decoded.

#### Source/Target Terminus — ✅ All fields supported.

~~**Gap M4**~~ — ✅ **FIXED**: `dynamic_node_properties` added to both Source and Target structs, with codec encode/decode.

~~**Gap M5**~~ — ✅ **FIXED**: Dynamic node creation implemented:
- `create_dynamic_node()` and `destroy_dynamic_node()` added to `MessageBroker` trait
- Lifetime policy extraction from `dynamic_node_properties`
- `CreateDynamicNode` / `DestroyDynamicNode` broker actions
- `DeleteOnClose` policy triggers node destruction on link detach
- `LifetimePolicy` enum with all 4 spec policies (0x2B-0x2E)

#### Delivery States — ✅ All states supported including Declared.

~~**Gap M6**~~ — ✅ **FIXED**: `MessageState` enum with `Available` → `Acquired` → `Archived` transitions. Terminal outcomes mapped: Accepted/Rejected → Archived, Released/Modified → Available.

### 2.7 Transactions (`amqp-core-transactions`) — ✅ Full lifecycle implemented.

| Feature | Status |
|---------|--------|
| Coordinator target (0x30) | ✅ |
| Declare (0x31) | ✅ |
| Discharge (0x32) | ✅ |
| Declared (0x33) | ✅ |
| TransactionalState (0x34) | ✅ |
| Transaction lifecycle | ✅ Fixed |
| Transactional posting | ✅ Fixed |
| Transactional retirement | ✅ Fixed |
| Transaction error conditions | ✅ Fixed |
| Recovery (rollback on close) | ✅ Fixed |

~~**Gap X1**~~ — ✅ **FIXED**: Full transaction lifecycle:
- Coordinator link ATTACH/DETACH handling
- Declare → allocate txn-id → Declared response (Disposition)
- Discharge → commit (release buffered actions) or rollback (discard)
- Transactional transfers buffered in `Transaction` struct
- Transactional dispositions buffered until discharge
- Coordinator detach rolls back all active transactions
- Unknown txn-id → Rejected with `amqp:transaction:unknown-id`

~~**Gap X2**~~ — ✅ **FIXED**: Transaction error conditions defined.

### 2.8 Security (`amqp-core-security`)

| Feature | Status |
|---------|--------|
| SASL header (proto-id 3) | ✅ |
| sasl-mechanisms (0x40) | ✅ |
| sasl-init (0x41) | ✅ |
| sasl-challenge (0x42) | ✅ |
| sasl-response (0x43) | ✅ |
| sasl-outcome (0x44) | ✅ |
| SASL outcome codes (0-4) | ✅ |
| Multi-round SASL | ✅ |
| Pluggable authenticator | ✅ |
| PLAIN mechanism | ✅ |
| ANONYMOUS mechanism | ✅ |
| EXTERNAL mechanism | ✅ Fixed |
| SASL frame max size | ✅ Fixed |
| TLS transport | ✅ Fixed |

~~**Gap SEC1**~~ — ✅ **FIXED**: SASL frames > 512 bytes rejected.

~~**Gap SEC2**~~ — ✅ **FIXED**: TLS integration via `tokio-rustls`:
- `transport.rs` module with `AmqpStream` enum (TCP/TLS variants)
- `TlsConfig` with `from_pem()` (server-only) and `from_pem_with_client_auth()` (mutual TLS)
- Feature-gated behind `tls` Cargo feature (enabled by default)
- Dual-listener: plain TCP on port 5672, TLS on configurable port (e.g. 5671)
- `AmqpServer::with_tls()` builder method
- `AmqpServerConfig::with_tls_bind_addr()` for TLS listener address
- Peer certificate CN extraction for SASL EXTERNAL

~~**Gap SEC3**~~ — ✅ **FIXED**: SASL EXTERNAL mechanism:
- `EXTERNAL` included in mechanisms list when TLS client certificate is present
- `set_peer_cert_subject()` stores client cert CN from TLS handshake
- SASL EXTERNAL authenticates using the certificate subject as identity
- Works with both built-in auth and pluggable `SaslAuthenticator`
- Without client cert, EXTERNAL auth attempt is rejected

---

## 3. Remaining Gaps

### Low-Priority / Future Enhancements

| ID | Gap | Priority | Notes |
|----|-----|----------|-------|
| — | Multi-resource transactions | Low | multi-txns-per-ssn, multi-ssns-per-txn |
| — | Transactional acquisition (txn-id in FLOW) | Low | Rare client usage |
| — | SCRAM-SHA-1/256 SASL mechanisms | Medium | Stronger auth mechanisms |

All spec-required features are now implemented. Remaining items are optional enhancements beyond core AMQP 1.0 compliance.

---

## 4. Implementation Status

### Phase 1 — Spec Compliance Fixes (P0) — ✅ COMPLETE

All correctness issues fixed:
- ✅ E1: Missing error condition constants (11 added)
- ✅ L1: initial-delivery-count validation
- ✅ S2/S3: Session window enforcement
- ✅ SEC1: SASL frame size limit (512 bytes)
- ✅ C2: DISCARDING state (connection)
- ✅ S1: DISCARDING state (session)
- ✅ L4: Aborted transfer cleanup (already implemented)
- ✅ C5: Version negotiation response

### Phase 2 — Interoperability (P1) — ✅ COMPLETE

All commonly-used features supported:
- ✅ T1/T2: decimal32/64/128, char types
- ✅ M1: message-id polymorphism
- ✅ M2/M3: amqp-sequence and amqp-value body sections
- ✅ L2: FLOW echo handling
- ✅ M4: dynamic-node-properties field

### Phase 3 — Advanced Features (P2) — ✅ COMPLETE

- ✅ X1/X2: Transaction support (coordinator link, declare/discharge, buffering, rollback)
- ✅ M5: Dynamic node creation (lifetime policies, broker integration)
- ✅ L3: Delivery resume on reattach (unsettled reconciliation)
- ✅ M6: Message state machine (AVAILABLE→ACQUIRED→ARCHIVED)
- ✅ C1: Pipelining support (inherent in loop-based processing)

### Phase 4 — Security & Settlement (P3) — ✅ COMPLETE

- ✅ C3: max-frame-size default changed to u32::MAX per spec
- ✅ C4: Capability negotiation enforcement (warn on unsupported desired capabilities)
- ✅ L5: Exactly-once settlement (rcv-settle-mode=second two-phase protocol)
- ✅ SEC2: TLS integration (tokio-rustls, dual TCP/TLS listener, feature-gated)
- ✅ SEC3: SASL EXTERNAL (TLS client-cert authentication)

---

## 5. Test Coverage

**330 tests passing** across all modules (326 without TLS feature):

| Module | Test Count | Coverage Areas |
|--------|-----------|----------------|
| `types.rs` | ~20 | Type constructors, Display, error conditions |
| `codec.rs` | ~130 | Encode/decode roundtrip for all types, performatives, delivery states |
| `connection.rs` | ~130 | Connection lifecycle, SASL (PLAIN/ANONYMOUS/EXTERNAL), session management, link handling, transactions, pipelining, exactly-once settlement, capability negotiation |
| `link.rs` | ~25 | Link state, flow control, partial delivery, batchable disposition |
| `broker.rs` | ~15 | NoopBroker, SettleAction mapping, dynamic nodes |
| `server.rs` | ~5 | Server config, basic lifecycle |
| `transport.rs` | ~5 | Stream enum, TLS config, CN extraction |

Key test areas:
- Transaction lifecycle (declare, discharge, rollback on detach, buffering)
- Dynamic node creation with lifetime policies
- Delivery resume with unsettled reconciliation
- Message state machine transitions
- Session window enforcement (remote_incoming_window)
- DISCARDING state (connection and session)
- SASL frame size limit
- SASL EXTERNAL with/without client cert
- initial-delivery-count validation
- Pipelining (header+open+begin+attach in single buffer)
- Coordinator link ATTACH/DETACH
- Declared delivery state encode/decode roundtrip
- max-frame-size negotiation (u32::MAX default)
- Capability negotiation (accepted + unknown capabilities)
- Exactly-once two-phase settlement (rcv-settle-mode=second)

---

## 6. Appendix — Descriptor Reference

| Type | Descriptor | Hex |
|------|-----------|-----|
| error | 0x1d | |
| received | 0x23 | |
| accepted | 0x24 | |
| rejected | 0x25 | |
| released | 0x26 | |
| modified | 0x27 | |
| source | 0x28 | |
| target | 0x29 | |
| delete-on-close | 0x2b | |
| delete-on-no-links | 0x2c | |
| delete-on-no-messages | 0x2d | |
| delete-on-no-links-or-messages | 0x2e | |
| coordinator | 0x30 | |
| declare | 0x31 | |
| discharge | 0x32 | |
| declared | 0x33 | |
| transactional-state | 0x34 | |
| sasl-mechanisms | 0x40 | |
| sasl-init | 0x41 | |
| sasl-challenge | 0x42 | |
| sasl-response | 0x43 | |
| sasl-outcome | 0x44 | |
| header | 0x70 | |
| delivery-annotations | 0x71 | |
| message-annotations | 0x72 | |
| properties | 0x73 | |
| application-properties | 0x74 | |
| data | 0x75 | |
| amqp-sequence | 0x76 | |
| amqp-value | 0x77 | |
| footer | 0x78 | |
| open | 0x10 | |
| begin | 0x11 | |
| attach | 0x12 | |
| flow | 0x13 | |
| transfer | 0x14 | |
| disposition | 0x15 | |
| detach | 0x16 | |
| end | 0x17 | |
| close | 0x18 | |
