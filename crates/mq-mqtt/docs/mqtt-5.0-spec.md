# MQTT Version 5.0

**OASIS Standard — 07 March 2019**

**Editors:** Andrew Banks (IBM), Ed Briggs (Microsoft), Ken Borgendale (IBM), Rahul Gupta (IBM)

**Specification URIs:**
- https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html (Authoritative)
- https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html (Latest)

---

## Abstract

MQTT is a Client Server publish/subscribe messaging transport protocol. It is light weight, open, simple, and designed to be easy to implement. These characteristics make it ideal for use in many situations, including constrained environments such as for communication in Machine to Machine (M2M) and Internet of Things (IoT) contexts where a small code footprint is required and/or network bandwidth is at a premium.

The protocol runs over TCP/IP, or over other network protocols that provide ordered, lossless, bi-directional connections. Its features include:

- Use of the publish/subscribe message pattern which provides one-to-many message distribution and decoupling of applications.
- A messaging transport that is agnostic to the content of the payload.
- Three qualities of service for message delivery:
  - **"At most once"** — messages are delivered according to the best efforts of the operating environment. Message loss can occur.
  - **"At least once"** — messages are assured to arrive but duplicates can occur.
  - **"Exactly once"** — messages are assured to arrive exactly once.
- A small transport overhead and protocol exchanges minimized to reduce network traffic.
- A mechanism to notify interested parties when an abnormal disconnection occurs.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [MQTT Control Packet Format](#2-mqtt-control-packet-format)
3. [MQTT Control Packets](#3-mqtt-control-packets)
4. [Operational Behavior](#4-operational-behavior)
5. [Security (non-normative)](#5-security-non-normative)
6. [Using WebSocket as a Network Transport](#6-using-websocket-as-a-network-transport)
7. [Conformance](#7-conformance)
- [Appendix A: Acknowledgments](#appendix-a-acknowledgments)
- [Appendix B: Mandatory Normative Statement (non-normative)](#appendix-b-mandatory-normative-statement-non-normative)
- [Appendix C: Summary of New Features in MQTT v5.0 (non-normative)](#appendix-c-summary-of-new-features-in-mqtt-v50-non-normative)

---

# 1 Introduction

## 1.0 Intellectual Property Rights Policy

This specification is provided under the [Non-Assertion Mode](https://www.oasis-open.org/policies-guidelines/ipr#Non-Assertion-Mode) of the OASIS IPR Policy. For information on patent disclosures, refer to https://www.oasis-open.org/committees/mqtt/ipr.php.

## 1.1 Organization of the MQTT Specification

The specification is split into seven chapters:

- Chapter 1 — Introduction
- Chapter 2 — MQTT Control Packet format
- Chapter 3 — MQTT Control Packets
- Chapter 4 — Operational behavior
- Chapter 5 — Security
- Chapter 6 — Using WebSocket as a network transport
- Chapter 7 — Conformance Targets

## 1.2 Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this specification are to be interpreted as described in IETF RFC 2119.

**Network Connection:** A construct provided by the underlying transport protocol that connects the Client to the Server and provides ordered, lossless byte streaming in both directions.

**Application Message:** The data carried by the MQTT protocol across the network for the application. Contains payload data, a Quality of Service (QoS), a collection of Properties, and a Topic Name.

**Client:** A program or device that uses MQTT. A Client opens the Network Connection to the Server, publishes Application Messages, subscribes to/unsubscribes from topics, and closes the Network Connection.

**Server:** A program or device that acts as an intermediary between Clients. Accepts Network Connections, processes Subscribe/Unsubscribe requests, forwards matching Application Messages, and closes Network Connections.

**Session:** A stateful interaction between a Client and a Server. May span multiple consecutive Network Connections.

**Subscription:** Comprises a Topic Filter and a maximum QoS. Associated with a single Session.

**Shared Subscription:** Can be associated with more than one Session to allow wider message exchange patterns. A matching Application Message is only sent to one of the associated Sessions.

**Wildcard Subscription:** A Subscription with a Topic Filter containing one or more wildcard characters.

**Topic Name:** The label attached to an Application Message which is matched against Subscriptions.

**Topic Filter:** An expression in a Subscription to indicate interest in one or more topics. Can include wildcard characters.

**MQTT Control Packet:** A packet of information sent across the Network Connection. MQTT defines fifteen different types.

**Malformed Packet:** A control packet that cannot be parsed according to this specification.

**Protocol Error:** An error detected after parsing where the packet contains data not allowed by the protocol or inconsistent with state.

**Will Message:** An Application Message published by the Server after the Network Connection is closed abnormally.

**Disallowed Unicode code point:** The set of Unicode Control Codes and Unicode Noncharacters that should not be included in a UTF-8 Encoded String.

## 1.3 Normative References

- **[RFC2119]** Key words for use in RFCs to Indicate Requirement Levels — http://www.rfc-editor.org/info/rfc2119
- **[RFC3629]** UTF-8, a transformation format of ISO 10646 — http://www.rfc-editor.org/info/rfc3629
- **[RFC6455]** The WebSocket Protocol — http://www.rfc-editor.org/info/rfc6455
- **[Unicode]** The Unicode Standard — http://www.unicode.org/versions/latest/

## 1.4 Non-normative References

- **[RFC0793]** Transmission Control Protocol
- **[RFC5246]** TLS Protocol Version 1.2
- **[AES]** Advanced Encryption Standard (FIPS PUB 197)
- **[CHACHA20]** ChaCha20 and Poly1305 for IETF Protocols
- **[FIPS1402]** Security Requirements for Cryptographic Modules
- **[MQTTV311]** MQTT V3.1.1 Protocol Specification — http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html

## 1.5 Data Representation

### 1.5.1 Bits

Bits in a byte are labeled 7 to 0. Bit 7 is the most significant bit; bit 0 is the least significant.

### 1.5.2 Two Byte Integer

16-bit unsigned integers in big-endian order (MSB first, then LSB).

### 1.5.3 Four Byte Integer

32-bit unsigned integers in big-endian order.

### 1.5.4 UTF-8 Encoded String

Text fields are encoded as UTF-8 strings, each prefixed with a Two Byte Integer length field. Maximum size: 65,535 bytes.

**Figure 1-1 Structure of UTF-8 Encoded Strings**

| Byte   | Content                                  |
|--------|------------------------------------------|
| byte 1 | String length MSB                        |
| byte 2 | String length LSB                        |
| byte 3+| UTF-8 encoded character data (if length > 0) |

Requirements:
- `[MQTT-1.5.4-1]` Character data MUST be well-formed UTF-8; MUST NOT include code points between U+D800 and U+DFFF.
- `[MQTT-1.5.4-2]` MUST NOT include an encoding of the null character U+0000.
- `[MQTT-1.5.4-3]` The UTF-8 BOM sequence 0xEF 0xBB 0xBF MUST NOT be skipped or stripped by a packet receiver.

Disallowed Unicode code points (SHOULD NOT be included):
- U+0001..U+001F control characters
- U+007F..U+009F control characters
- Unicode non-characters (e.g., U+0FFFF)

### 1.5.5 Variable Byte Integer

Encodes values up to 268,435,455 using 1–4 bytes. The least significant 7 bits of each byte carry data; the MSB indicates whether more bytes follow.

**Table 1-1 Size of Variable Byte Integer**

| Digits | From                          | To                             |
|--------|-------------------------------|--------------------------------|
| 1      | 0 (0x00)                      | 127 (0x7F)                     |
| 2      | 128 (0x80, 0x01)              | 16,383 (0xFF, 0x7F)            |
| 3      | 16,384 (0x80, 0x80, 0x01)     | 2,097,151 (0xFF, 0xFF, 0x7F)   |
| 4      | 2,097,152 (0x80, 0x80, 0x80, 0x01) | 268,435,455 (0xFF, 0xFF, 0xFF, 0x7F) |

`[MQTT-1.5.5-1]` The encoded value MUST use the minimum number of bytes necessary.

### 1.5.6 Binary Data

Represented by a Two Byte Integer length followed by that number of bytes. Maximum: 65,535 bytes.

### 1.5.7 UTF-8 String Pair

Two UTF-8 Encoded Strings (name + value). Both MUST comply with UTF-8 Encoded String requirements `[MQTT-1.5.7-1]`.

## 1.6 Security

Client and Server implementations SHOULD offer Authentication, Authorization, and secure communication options (see Chapter 5).

## 1.7 Editing Convention

Conformance statements are highlighted and referenced as `[MQTT-x.x.x-y]`.

## 1.8 Change History

### 1.8.1 MQTT v3.1.1

First OASIS standard version of MQTT. Also standardized as ISO/IEC 20922:2016.

### 1.8.2 MQTT v5.0

Major functional objectives:
- Enhancements for scalability and large scale systems
- Improved error reporting
- Formalized common patterns including capability discovery and request/response
- Extensibility mechanisms including user properties
- Performance improvements and support for small clients

---

# 2 MQTT Control Packet Format

## 2.1 Structure of an MQTT Control Packet

An MQTT Control Packet consists of up to three parts in this order:

| Part           | Presence                                 |
|----------------|------------------------------------------|
| Fixed Header   | Present in all MQTT Control Packets      |
| Variable Header| Present in some MQTT Control Packets     |
| Payload        | Present in some MQTT Control Packets     |

### 2.1.1 Fixed Header

Each Control Packet contains a Fixed Header:

| Bit    | 7–4                         | 3–0                                   |
|--------|-----------------------------|---------------------------------------|
| byte 1 | MQTT Control Packet type    | Flags specific to each packet type    |
| byte 2+| Remaining Length            |                                       |

### 2.1.2 MQTT Control Packet Type

**Position:** byte 1, bits 7–4. Represented as a 4-bit unsigned value.

**Table 2-1 MQTT Control Packet Types**

| Name        | Value | Direction              | Description                                 |
|-------------|-------|------------------------|---------------------------------------------|
| Reserved    | 0     | Forbidden              | Reserved                                    |
| CONNECT     | 1     | Client → Server        | Connection request                          |
| CONNACK     | 2     | Server → Client        | Connect acknowledgment                      |
| PUBLISH     | 3     | Client ↔ Server        | Publish message                             |
| PUBACK      | 4     | Client ↔ Server        | Publish acknowledgment (QoS 1)              |
| PUBREC      | 5     | Client ↔ Server        | Publish received (QoS 2 part 1)             |
| PUBREL      | 6     | Client ↔ Server        | Publish release (QoS 2 part 2)              |
| PUBCOMP     | 7     | Client ↔ Server        | Publish complete (QoS 2 part 3)             |
| SUBSCRIBE   | 8     | Client → Server        | Subscribe request                           |
| SUBACK      | 9     | Server → Client        | Subscribe acknowledgment                    |
| UNSUBSCRIBE | 10    | Client → Server        | Unsubscribe request                         |
| UNSUBACK    | 11    | Server → Client        | Unsubscribe acknowledgment                  |
| PINGREQ     | 12    | Client → Server        | PING request                                |
| PINGRESP    | 13    | Server → Client        | PING response                               |
| DISCONNECT  | 14    | Client ↔ Server        | Disconnect notification                     |
| AUTH        | 15    | Client ↔ Server        | Authentication exchange                     |

### 2.1.3 Flags

Remaining bits [3–0] of byte 1 contain packet-type-specific flags. Reserved bits MUST be set to the listed values `[MQTT-2.1.3-1]`.

**Table 2-2 Flag Bits**

| Packet      | Bit 3  | Bit 2  | Bit 1  | Bit 0  |
|-------------|--------|--------|--------|--------|
| CONNECT     | 0      | 0      | 0      | 0      |
| CONNACK     | 0      | 0      | 0      | 0      |
| PUBLISH     | DUP    | QoS    | QoS    | RETAIN |
| PUBACK      | 0      | 0      | 0      | 0      |
| PUBREC      | 0      | 0      | 0      | 0      |
| PUBREL      | 0      | 0      | 1      | 0      |
| PUBCOMP     | 0      | 0      | 0      | 0      |
| SUBSCRIBE   | 0      | 0      | 1      | 0      |
| SUBACK      | 0      | 0      | 0      | 0      |
| UNSUBSCRIBE | 0      | 0      | 1      | 0      |
| UNSUBACK    | 0      | 0      | 0      | 0      |
| PINGREQ     | 0      | 0      | 0      | 0      |
| PINGRESP    | 0      | 0      | 0      | 0      |
| DISCONNECT  | 0      | 0      | 0      | 0      |
| AUTH        | 0      | 0      | 0      | 0      |

### 2.1.4 Remaining Length

**Position:** starts at byte 2. A Variable Byte Integer representing the number of bytes remaining within the current Control Packet, including Variable Header and Payload data. Does not include the bytes used to encode the Remaining Length itself.

## 2.2 Variable Header

Resides between the Fixed Header and the Payload. Content varies by packet type.

### 2.2.1 Packet Identifier

Many Control Packets include a Two Byte Integer Packet Identifier in the Variable Header: PUBLISH (QoS > 0), PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK.

**Table 2-3 Packets Containing a Packet Identifier**

| Packet      | Packet Identifier field       |
|-------------|-------------------------------|
| CONNECT     | NO                            |
| CONNACK     | NO                            |
| PUBLISH     | YES (if QoS > 0)              |
| PUBACK      | YES                           |
| PUBREC      | YES                           |
| PUBREL      | YES                           |
| PUBCOMP     | YES                           |
| SUBSCRIBE   | YES                           |
| SUBACK      | YES                           |
| UNSUBSCRIBE | YES                           |
| UNSUBACK    | YES                           |
| PINGREQ     | NO                            |
| PINGRESP    | NO                            |
| DISCONNECT  | NO                            |
| AUTH        | NO                            |

Key rules:
- `[MQTT-2.2.1-2]` A PUBLISH MUST NOT contain a Packet Identifier if QoS is 0.
- `[MQTT-2.2.1-3]` Each new SUBSCRIBE, UNSUBSCRIBE, or PUBLISH (QoS > 0) MUST assign a non-zero, currently unused Packet Identifier.
- `[MQTT-2.2.1-4]` Each new Server PUBLISH (QoS > 0) MUST assign a non-zero, currently unused Packet Identifier.
- `[MQTT-2.2.1-5]` PUBACK, PUBREC, PUBREL, PUBCOMP MUST contain the same Packet Identifier as the original PUBLISH.
- `[MQTT-2.2.1-6]` SUBACK/UNSUBACK MUST contain the Packet Identifier from the corresponding SUBSCRIBE/UNSUBSCRIBE.

### 2.2.2 Properties

The last field in the Variable Header of most packets is a set of Properties.

#### 2.2.2.1 Property Length

Encoded as a Variable Byte Integer. Does not include the bytes used to encode itself. If no properties, MUST include a Property Length of zero `[MQTT-2.2.2-1]`.

#### 2.2.2.2 Property

A Property consists of an Identifier (Variable Byte Integer) and a value.

**Table 2-4 Properties**

| Dec | Hex  | Name                            | Type                 | Applicable Packets                                                            |
|-----|------|---------------------------------|----------------------|-------------------------------------------------------------------------------|
| 1   | 0x01 | Payload Format Indicator        | Byte                 | PUBLISH, Will Properties                                                      |
| 2   | 0x02 | Message Expiry Interval         | Four Byte Integer    | PUBLISH, Will Properties                                                      |
| 3   | 0x03 | Content Type                    | UTF-8 Encoded String | PUBLISH, Will Properties                                                      |
| 8   | 0x08 | Response Topic                  | UTF-8 Encoded String | PUBLISH, Will Properties                                                      |
| 9   | 0x09 | Correlation Data                | Binary Data          | PUBLISH, Will Properties                                                      |
| 11  | 0x0B | Subscription Identifier         | Variable Byte Integer| PUBLISH, SUBSCRIBE                                                            |
| 17  | 0x11 | Session Expiry Interval         | Four Byte Integer    | CONNECT, CONNACK, DISCONNECT                                                  |
| 18  | 0x12 | Assigned Client Identifier      | UTF-8 Encoded String | CONNACK                                                                       |
| 19  | 0x13 | Server Keep Alive               | Two Byte Integer     | CONNACK                                                                       |
| 21  | 0x15 | Authentication Method           | UTF-8 Encoded String | CONNECT, CONNACK, AUTH                                                        |
| 22  | 0x16 | Authentication Data             | Binary Data          | CONNECT, CONNACK, AUTH                                                        |
| 23  | 0x17 | Request Problem Information     | Byte                 | CONNECT                                                                       |
| 24  | 0x18 | Will Delay Interval             | Four Byte Integer    | Will Properties                                                               |
| 25  | 0x19 | Request Response Information    | Byte                 | CONNECT                                                                       |
| 26  | 0x1A | Response Information            | UTF-8 Encoded String | CONNACK                                                                       |
| 28  | 0x1C | Server Reference                | UTF-8 Encoded String | CONNACK, DISCONNECT                                                           |
| 31  | 0x1F | Reason String                   | UTF-8 Encoded String | CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH |
| 33  | 0x21 | Receive Maximum                 | Two Byte Integer     | CONNECT, CONNACK                                                              |
| 34  | 0x22 | Topic Alias Maximum             | Two Byte Integer     | CONNECT, CONNACK                                                              |
| 35  | 0x23 | Topic Alias                     | Two Byte Integer     | PUBLISH                                                                       |
| 36  | 0x24 | Maximum QoS                     | Byte                 | CONNACK                                                                       |
| 37  | 0x25 | Retain Available                | Byte                 | CONNACK                                                                       |
| 38  | 0x26 | User Property                   | UTF-8 String Pair    | CONNECT, CONNACK, PUBLISH, Will Properties, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH |
| 39  | 0x27 | Maximum Packet Size             | Four Byte Integer    | CONNECT, CONNACK                                                              |
| 40  | 0x28 | Wildcard Subscription Available | Byte                 | CONNACK                                                                       |
| 41  | 0x29 | Subscription Identifier Available | Byte               | CONNACK                                                                       |
| 42  | 0x2A | Shared Subscription Available   | Byte                 | CONNACK                                                                       |

## 2.3 Payload

Some MQTT Control Packets contain a Payload as the final part.

**Table 2-5 Packets Containing a Payload**

| Packet      | Payload  |
|-------------|----------|
| CONNECT     | Required |
| CONNACK     | None     |
| PUBLISH     | Optional |
| PUBACK      | None     |
| PUBREC      | None     |
| PUBREL      | None     |
| PUBCOMP     | None     |
| SUBSCRIBE   | Required |
| SUBACK      | Required |
| UNSUBSCRIBE | Required |
| UNSUBACK    | Required |
| PINGREQ     | None     |
| PINGRESP    | None     |
| DISCONNECT  | None     |
| AUTH        | None     |

## 2.4 Reason Code

A one byte unsigned value indicating the result of an operation. Values < 0x80 indicate success; values ≥ 0x80 indicate failure.

**Table 2-6 Reason Codes**

| Dec | Hex  | Name                                   | Packets                                                                         |
|-----|------|----------------------------------------|---------------------------------------------------------------------------------|
| 0   | 0x00 | Success                                | CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK, AUTH                        |
| 0   | 0x00 | Normal disconnection                   | DISCONNECT                                                                      |
| 0   | 0x00 | Granted QoS 0                          | SUBACK                                                                          |
| 1   | 0x01 | Granted QoS 1                          | SUBACK                                                                          |
| 2   | 0x02 | Granted QoS 2                          | SUBACK                                                                          |
| 4   | 0x04 | Disconnect with Will Message           | DISCONNECT                                                                      |
| 16  | 0x10 | No matching subscribers                | PUBACK, PUBREC                                                                  |
| 17  | 0x11 | No subscription existed                | UNSUBACK                                                                        |
| 24  | 0x18 | Continue authentication                | AUTH                                                                            |
| 25  | 0x19 | Re-authenticate                        | AUTH                                                                            |
| 128 | 0x80 | Unspecified error                      | CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT                           |
| 129 | 0x81 | Malformed Packet                       | CONNACK, DISCONNECT                                                             |
| 130 | 0x82 | Protocol Error                         | CONNACK, DISCONNECT                                                             |
| 131 | 0x83 | Implementation specific error          | CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT                           |
| 132 | 0x84 | Unsupported Protocol Version           | CONNACK                                                                         |
| 133 | 0x85 | Client Identifier not valid            | CONNACK                                                                         |
| 134 | 0x86 | Bad User Name or Password              | CONNACK                                                                         |
| 135 | 0x87 | Not authorized                         | CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT                           |
| 136 | 0x88 | Server unavailable                     | CONNACK                                                                         |
| 137 | 0x89 | Server busy                            | CONNACK, DISCONNECT                                                             |
| 138 | 0x8A | Banned                                 | CONNACK                                                                         |
| 139 | 0x8B | Server shutting down                   | DISCONNECT                                                                      |
| 140 | 0x8C | Bad authentication method              | CONNACK, DISCONNECT                                                             |
| 141 | 0x8D | Keep Alive timeout                     | DISCONNECT                                                                      |
| 142 | 0x8E | Session taken over                     | DISCONNECT                                                                      |
| 143 | 0x8F | Topic Filter invalid                   | SUBACK, UNSUBACK, DISCONNECT                                                    |
| 144 | 0x90 | Topic Name invalid                     | CONNACK, PUBACK, PUBREC, DISCONNECT                                             |
| 145 | 0x91 | Packet Identifier in use               | PUBACK, PUBREC, SUBACK, UNSUBACK                                                |
| 146 | 0x92 | Packet Identifier not found            | PUBREL, PUBCOMP                                                                 |
| 147 | 0x93 | Receive Maximum exceeded               | DISCONNECT                                                                      |
| 148 | 0x94 | Topic Alias invalid                    | DISCONNECT                                                                      |
| 149 | 0x95 | Packet too large                       | CONNACK, DISCONNECT                                                             |
| 150 | 0x96 | Message rate too high                  | DISCONNECT                                                                      |
| 151 | 0x97 | Quota exceeded                         | CONNACK, PUBACK, PUBREC, SUBACK, DISCONNECT                                     |
| 152 | 0x98 | Administrative action                  | DISCONNECT                                                                      |
| 153 | 0x99 | Payload format invalid                 | CONNACK, PUBACK, PUBREC, DISCONNECT                                             |
| 154 | 0x9A | Retain not supported                   | CONNACK, DISCONNECT                                                             |
| 155 | 0x9B | QoS not supported                      | CONNACK, DISCONNECT                                                             |
| 156 | 0x9C | Use another server                     | CONNACK, DISCONNECT                                                             |
| 157 | 0x9D | Server moved                           | CONNACK, DISCONNECT                                                             |
| 158 | 0x9E | Shared Subscriptions not supported     | SUBACK, DISCONNECT                                                              |
| 159 | 0x9F | Connection rate exceeded               | CONNACK, DISCONNECT                                                             |
| 160 | 0xA0 | Maximum connect time                   | DISCONNECT                                                                      |
| 161 | 0xA1 | Subscription Identifiers not supported | SUBACK, DISCONNECT                                                              |
| 162 | 0xA2 | Wildcard Subscriptions not supported   | SUBACK, DISCONNECT                                                              |

---

# 3 MQTT Control Packets

## 3.1 CONNECT — Connection Request

After a Network Connection is established, the first packet from Client to Server MUST be a CONNECT `[MQTT-3.1.0-1]`. A Client can only send CONNECT once; a second CONNECT is a Protocol Error `[MQTT-3.1.0-2]`.

### 3.1.1 CONNECT Fixed Header

| Bit    | 7–4                    | 3–0      |
|--------|------------------------|----------|
| byte 1 | Packet type (1) = 0001 | Reserved = 0000 |
| byte 2+| Remaining Length       |          |

### 3.1.2 CONNECT Variable Header

Contains in order: Protocol Name, Protocol Version, Connect Flags, Keep Alive, Properties.

#### 3.1.2.1 Protocol Name

UTF-8 string "MQTT" (bytes: 0x00, 0x04, 'M', 'Q', 'T', 'T'). `[MQTT-3.1.2-1]`

#### 3.1.2.2 Protocol Version

One byte. Value 5 (0x05) for MQTT v5.0. `[MQTT-3.1.2-2]`

#### 3.1.2.3 Connect Flags

| Bit | 7              | 6             | 5           | 4–3      | 2         | 1           | 0        |
|-----|----------------|---------------|-------------|----------|-----------|-------------|----------|
|     | User Name Flag | Password Flag | Will Retain | Will QoS | Will Flag | Clean Start | Reserved |

Reserved bit MUST be 0 `[MQTT-3.1.2-3]`.

#### 3.1.2.4 Clean Start

If set to 1, Client and Server MUST discard any existing Session `[MQTT-3.1.2-4]`. If 0 and a Session exists, the Server MUST resume it `[MQTT-3.1.2-5]`; if no Session exists, the Server MUST create one `[MQTT-3.1.2-6]`.

#### 3.1.2.5 Will Flag

If set to 1, a Will Message MUST be stored on the Server `[MQTT-3.1.2-7]` and published when the Network Connection closes abnormally `[MQTT-3.1.2-8]`. Will Properties, Will Topic, and Will Payload MUST be present in Payload `[MQTT-3.1.2-9]`.

#### 3.1.2.6 Will QoS

If Will Flag = 0, Will QoS MUST be 0 `[MQTT-3.1.2-11]`. If Will Flag = 1, Will QoS can be 0, 1, or 2 `[MQTT-3.1.2-12]`.

#### 3.1.2.7 Will Retain

If Will Flag = 0, Will Retain MUST be 0 `[MQTT-3.1.2-13]`. If Will Flag = 1 and Will Retain = 0, publish as non-retained `[MQTT-3.1.2-14]`. If Will Flag = 1 and Will Retain = 1, publish as retained `[MQTT-3.1.2-15]`.

#### 3.1.2.8 User Name Flag

If 0, User Name MUST NOT be present `[MQTT-3.1.2-16]`. If 1, User Name MUST be present `[MQTT-3.1.2-17]`.

#### 3.1.2.9 Password Flag

If 0, Password MUST NOT be present `[MQTT-3.1.2-18]`. If 1, Password MUST be present `[MQTT-3.1.2-19]`.

#### 3.1.2.10 Keep Alive

Two Byte Integer (seconds). Maximum time interval between MQTT Control Packets. If non-zero and no other packets are sent, Client MUST send PINGREQ `[MQTT-3.1.2-20]`. If Server returns Server Keep Alive in CONNACK, Client MUST use that value `[MQTT-3.1.2-21]`. Server MUST close connection if no packet received within 1.5× Keep Alive `[MQTT-3.1.2-22]`.

#### 3.1.2.11 CONNECT Properties

| Property                    | ID   | Type               |
|-----------------------------|------|--------------------|
| Session Expiry Interval     | 0x11 | Four Byte Integer  |
| Receive Maximum             | 0x21 | Two Byte Integer   |
| Maximum Packet Size         | 0x27 | Four Byte Integer  |
| Topic Alias Maximum         | 0x22 | Two Byte Integer   |
| Request Response Information| 0x19 | Byte               |
| Request Problem Information | 0x17 | Byte               |
| User Property               | 0x26 | UTF-8 String Pair  |
| Authentication Method       | 0x15 | UTF-8 Encoded String|
| Authentication Data         | 0x16 | Binary Data        |

**Session Expiry Interval** `[MQTT-3.1.2-23]`: Client and Server MUST store Session State if > 0. Value 0xFFFFFFFF = session does not expire.

**Maximum Packet Size** `[MQTT-3.1.2-24]`: Server MUST NOT send packets exceeding this limit.

**Topic Alias Maximum** `[MQTT-3.1.2-26]`/`[MQTT-3.1.2-27]`: Server MUST NOT send Topic Aliases exceeding this value. If absent or 0, Server MUST NOT send any Topic Aliases.

**Authentication Method** `[MQTT-3.1.2-30]`: If set, Client MUST NOT send any packets other than AUTH or DISCONNECT until it receives a CONNACK.

### 3.1.3 CONNECT Payload

Fields MUST appear in this order (if present): Client Identifier, Will Properties, Will Topic, Will Payload, User Name, Password `[MQTT-3.1.3-1]`.

#### 3.1.3.1 Client Identifier (ClientID)

- MUST be present `[MQTT-3.1.3-3]`
- MUST be a UTF-8 Encoded String `[MQTT-3.1.3-4]`
- Server MUST allow 1–23 alphanumeric characters `[MQTT-3.1.3-5]`
- Zero-length ClientID: Server MUST assign a unique ClientID `[MQTT-3.1.3-6]` and return it in CONNACK `[MQTT-3.1.3-7]`

#### 3.1.3.2 Will Properties

| Property                | ID   | Type               |
|-------------------------|------|--------------------|
| Will Delay Interval     | 0x18 | Four Byte Integer  |
| Payload Format Indicator| 0x01 | Byte               |
| Message Expiry Interval | 0x02 | Four Byte Integer  |
| Content Type            | 0x03 | UTF-8 Encoded String|
| Response Topic          | 0x08 | UTF-8 Encoded String|
| Correlation Data        | 0x09 | Binary Data        |
| User Property           | 0x26 | UTF-8 String Pair  |

**Will Delay Interval**: Server delays publishing Will Message until this interval passes or Session ends, whichever comes first. If new connection established before interval passes, MUST NOT send Will Message `[MQTT-3.1.3-9]`.

### 3.1.4 CONNECT Actions

On successful validation:
1. If ClientID already connected, Server sends DISCONNECT 0x8E (Session taken over) to existing client `[MQTT-3.1.4-3]`.
2. Server MUST process Clean Start `[MQTT-3.1.4-4]`.
3. Server MUST acknowledge with CONNACK containing 0x00 (Success) `[MQTT-3.1.4-5]`.
4. If Server rejects CONNECT, MUST NOT process subsequent data except AUTH packets `[MQTT-3.1.4-6]`.

---

## 3.2 CONNACK — Connect Acknowledgement

The Server MUST send a CONNACK with 0x00 (Success) before sending any other packet (except AUTH) `[MQTT-3.2.0-1]`. MUST NOT send more than one CONNACK `[MQTT-3.2.0-2]`.

### 3.2.2 CONNACK Variable Header

Contains: Connect Acknowledge Flags, Connect Reason Code, Properties.

#### 3.2.2.1 Session Present Flag (bit 0 of byte 1)

Indicates whether the Server has Session State for this ClientID.

- If Clean Start = 1: Session Present MUST be 0 `[MQTT-3.2.2-2]`.
- If Clean Start = 0 with existing session: Session Present MUST be 1; without existing session: MUST be 0 `[MQTT-3.2.2-3]`.
- If Client has no Session State but receives Session Present = 1: Client MUST close connection `[MQTT-3.2.2-4]`.
- If Client has Session State but receives Session Present = 0: Client MUST discard its Session State `[MQTT-3.2.2-5]`.

#### 3.2.2.2 Connect Reason Code

**Table 3-1 Connect Reason Code Values**

| Value | Hex  | Name                         | Description                                                           |
|-------|------|------------------------------|-----------------------------------------------------------------------|
| 0     | 0x00 | Success                      | Connection accepted.                                                  |
| 128   | 0x80 | Unspecified error            | Server does not wish to reveal reason.                                |
| 129   | 0x81 | Malformed Packet             | CONNECT packet could not be parsed.                                   |
| 130   | 0x82 | Protocol Error               | Data does not conform to specification.                               |
| 131   | 0x83 | Implementation specific error| CONNECT valid but not accepted.                                       |
| 132   | 0x84 | Unsupported Protocol Version | Server does not support this MQTT version.                            |
| 133   | 0x85 | Client Identifier not valid  | ClientID not allowed by the Server.                                   |
| 134   | 0x86 | Bad User Name or Password    | Credentials rejected.                                                 |
| 135   | 0x87 | Not authorized               | Client not authorized.                                                |
| 136   | 0x88 | Server unavailable           | MQTT Server not available.                                            |
| 137   | 0x89 | Server busy                  | Try again later.                                                      |
| 138   | 0x8A | Banned                       | Client banned by administrative action.                               |
| 140   | 0x8C | Bad authentication method    | Authentication method not supported.                                  |
| 144   | 0x90 | Topic Name invalid           | Will Topic Name not accepted.                                         |
| 149   | 0x95 | Packet too large             | CONNECT packet exceeded maximum size.                                 |
| 151   | 0x97 | Quota exceeded               | Implementation or administrative limit exceeded.                      |
| 153   | 0x99 | Payload format invalid       | Will Payload doesn't match Payload Format Indicator.                  |
| 154   | 0x9A | Retain not supported         | Server doesn't support retained messages; Will Retain was 1.          |
| 155   | 0x9B | QoS not supported            | Server doesn't support the Will QoS level.                            |
| 156   | 0x9C | Use another server           | Client should temporarily use another server.                         |
| 157   | 0x9D | Server moved                 | Client should permanently use another server.                         |
| 159   | 0x9F | Connection rate exceeded     | Connection rate limit exceeded.                                       |

#### 3.2.2.3 CONNACK Properties

Key properties in CONNACK:
- **Session Expiry Interval (0x11)**: Server informs Client of a different value than sent in CONNECT.
- **Receive Maximum (0x21)**: Limits QoS 1/2 publications processed concurrently.
- **Maximum QoS (0x24)**: Highest QoS level supported by Server `[MQTT-3.2.2-9]`.
- **Retain Available (0x25)**: Whether Server supports retained messages.
- **Maximum Packet Size (0x27)**: Maximum packet size Server will accept `[MQTT-3.2.2-15]`.
- **Assigned Client Identifier (0x12)**: Assigned when zero-length ClientID was used `[MQTT-3.2.2-16]`.
- **Topic Alias Maximum (0x22)**: Maximum Topic Alias the Server will accept `[MQTT-3.2.2-17]`.
- **Reason String (0x1F)**: Human-readable diagnostics string.
- **Server Keep Alive (0x13)**: Client MUST use this value if present `[MQTT-3.2.2-21]`.
- **Response Information (0x1A)**: Basis for creating a Response Topic.
- **Server Reference (0x1C)**: Points Client to another Server.
- **Authentication Method/Data**: For extended authentication.

---

## 3.3 PUBLISH — Publish Message

A PUBLISH packet transports an Application Message between Client and Server.

### 3.3.1 PUBLISH Fixed Header

| Bit    | 7–4              | 3       | 2–1   | 0      |
|--------|------------------|---------|-------|--------|
| byte 1 | Packet type (3)  | DUP     | QoS   | RETAIN |

#### 3.3.1.1 DUP Flag

- `[MQTT-3.3.1-1]` DUP MUST be set to 1 when re-delivering a PUBLISH packet.
- `[MQTT-3.3.1-2]` DUP MUST be 0 for all QoS 0 messages.
- `[MQTT-3.3.1-3]` DUP in outgoing PUBLISH MUST be determined solely by whether it is a retransmission.

#### 3.3.1.2 QoS

**Table 3-2 QoS Definitions**

| QoS value | Bit 2 | Bit 1 | Description            |
|-----------|-------|-------|------------------------|
| 0         | 0     | 0     | At most once delivery  |
| 1         | 0     | 1     | At least once delivery |
| 2         | 1     | 0     | Exactly once delivery  |
| -         | 1     | 1     | Reserved — must not be used |

`[MQTT-3.3.1-4]` A PUBLISH MUST NOT have both QoS bits set to 1.

#### 3.3.1.3 RETAIN

- `[MQTT-3.3.1-5]` If RETAIN=1, Server MUST replace any existing retained message and store it.
- `[MQTT-3.3.1-6]` Zero-byte payload with RETAIN=1: remove any retained message for the topic.
- `[MQTT-3.3.1-8]` If RETAIN=0, Server MUST NOT store as retained and MUST NOT remove existing retained messages.

**Retain Handling subscription options:**
- `[MQTT-3.3.1-9]` Retain Handling=0: Server MUST send retained messages at subscribe time.
- `[MQTT-3.3.1-10]` Retain Handling=1: Send retained messages only if subscription doesn't already exist.
- `[MQTT-3.3.1-11]` Retain Handling=2: MUST NOT send retained messages at subscribe time.

**Retain As Published:**
- `[MQTT-3.3.1-12]` If Retain As Published=0, Server MUST set RETAIN flag to 0 when forwarding.
- `[MQTT-3.3.1-13]` If Retain As Published=1, Server MUST preserve the RETAIN flag from received packet.

### 3.3.2 PUBLISH Variable Header

Contains: Topic Name, Packet Identifier (if QoS > 0), Properties.

#### PUBLISH Properties

| Property                | ID   | Notes                                                       |
|-------------------------|------|-------------------------------------------------------------|
| Payload Format Indicator| 0x01 | 0=unspecified bytes; 1=UTF-8 encoded character data         |
| Message Expiry Interval | 0x02 | Lifetime in seconds; Server adjusts for time waiting        |
| Topic Alias             | 0x23 | Integer replacing the Topic Name to reduce packet size      |
| Response Topic          | 0x08 | Topic for response message; identifies message as a request |
| Correlation Data        | 0x09 | Associates request/response; forwarded unaltered            |
| User Property           | 0x26 | Name-value pairs; forwarded unaltered and in order          |
| Subscription Identifier | 0x0B | 1–268,435,455; multiple if matching multiple subscriptions  |
| Content Type            | 0x03 | MIME-like content description; forwarded unaltered          |

**Topic Alias rules:**
- `[MQTT-3.3.2-7]` Topic Alias mappings MUST NOT be carried forward across Network Connections.
- `[MQTT-3.3.2-8]` Sender MUST NOT send Topic Alias value of 0.
- `[MQTT-3.3.2-9]` Client MUST NOT use Topic Alias > Server's Topic Alias Maximum.
- `[MQTT-3.3.2-11]` Server MUST NOT use Topic Alias > Client's Topic Alias Maximum.

### 3.3.4 PUBLISH Actions

**Table 3-3 Expected PUBLISH Packet Response**

| QoS Level | Expected Response |
|-----------|-------------------|
| QoS 0     | None              |
| QoS 1     | PUBACK            |
| QoS 2     | PUBREC            |

- `[MQTT-3.3.4-7]` Client MUST NOT send more than Receive Maximum QoS 1/2 PUBLISH packets without receiving PUBACK/PUBCOMP/PUBREC (≥128).
- `[MQTT-3.3.4-9]` Server MUST NOT send more than Receive Maximum QoS 1/2 PUBLISH packets without receiving PUBACK/PUBCOMP/PUBREC (≥128).

---

## 3.4 PUBACK — Publish Acknowledgement

Response to PUBLISH with QoS 1.

**Table 3-4 PUBACK Reason Codes**

| Value | Hex  | Name                          |
|-------|------|-------------------------------|
| 0     | 0x00 | Success                       |
| 16    | 0x10 | No matching subscribers       |
| 128   | 0x80 | Unspecified error             |
| 131   | 0x83 | Implementation specific error |
| 135   | 0x87 | Not authorized                |
| 144   | 0x90 | Topic Name invalid            |
| 145   | 0x91 | Packet Identifier in use      |
| 151   | 0x97 | Quota exceeded                |
| 153   | 0x99 | Payload format invalid        |

Reason Code and Property Length can be omitted if Reason Code = 0x00 and no Properties (Remaining Length = 2).

---

## 3.5 PUBREC — Publish Received (QoS 2 Delivery Part 1)

Response to PUBLISH with QoS 2. Second packet of QoS 2 exchange. Same Reason Codes as PUBACK.

---

## 3.6 PUBREL — Publish Release (QoS 2 Delivery Part 2)

Response to PUBREC. Third packet of QoS 2 exchange. Fixed Header bits 3,2,1,0 MUST be 0,0,1,0 `[MQTT-3.6.1-1]`.

**Table 3-6 PUBREL Reason Codes**

| Value | Hex  | Name                        |
|-------|------|-----------------------------|
| 0     | 0x00 | Success                     |
| 146   | 0x92 | Packet Identifier not found |

---

## 3.7 PUBCOMP — Publish Complete (QoS 2 Delivery Part 3)

Response to PUBREL. Fourth and final packet of QoS 2 exchange. Same Reason Codes as PUBREL.

---

## 3.8 SUBSCRIBE — Subscribe Request

Sent from Client to Server to create one or more Subscriptions.

Fixed Header bits 3,2,1,0 MUST be 0,0,1,0 `[MQTT-3.8.1-1]`.

### 3.8.3 SUBSCRIBE Payload

Contains a list of Topic Filter + Subscription Options pairs. MUST contain at least one pair `[MQTT-3.8.3-2]`.

**Subscription Options byte:**

| Bits | Field            | Description                                                |
|------|------------------|------------------------------------------------------------|
| 0–1  | Maximum QoS      | Maximum QoS for this subscription (0, 1, or 2)            |
| 2    | No Local         | If 1, don't forward to same ClientID as publisher `[MQTT-3.8.3-3]` |
| 3    | Retain As Published | If 1, keep RETAIN flag from original message            |
| 4–5  | Retain Handling  | 0=send retained at subscribe; 1=only if new; 2=never      |
| 6–7  | Reserved         | MUST be 0 `[MQTT-3.8.3-5]`                               |

### 3.8.4 SUBSCRIBE Actions

- Server MUST respond with SUBACK `[MQTT-3.8.4-1]` with same Packet Identifier `[MQTT-3.8.4-2]`.
- If identical Topic Filter exists, MUST replace it `[MQTT-3.8.4-3]`.
- SUBACK MUST contain a Reason Code for each Topic Filter `[MQTT-3.8.4-6]`.
- QoS of messages MUST be the minimum of published QoS and granted QoS `[MQTT-3.8.4-8]`.

---

## 3.9 SUBACK — Subscribe Acknowledgement

**Table 3-8 Subscribe Reason Codes**

| Value | Hex  | Name                                    |
|-------|------|-----------------------------------------|
| 0     | 0x00 | Granted QoS 0                           |
| 1     | 0x01 | Granted QoS 1                           |
| 2     | 0x02 | Granted QoS 2                           |
| 128   | 0x80 | Unspecified error                       |
| 131   | 0x83 | Implementation specific error           |
| 135   | 0x87 | Not authorized                          |
| 143   | 0x8F | Topic Filter invalid                    |
| 145   | 0x91 | Packet Identifier in use               |
| 151   | 0x97 | Quota exceeded                          |
| 158   | 0x9E | Shared Subscriptions not supported      |
| 161   | 0xA1 | Subscription Identifiers not supported  |
| 162   | 0xA2 | Wildcard Subscriptions not supported    |

Order of Reason Codes MUST match order of Topic Filters in SUBSCRIBE `[MQTT-3.9.3-1]`.

---

## 3.10 UNSUBSCRIBE — Unsubscribe Request

Fixed Header bits 3,2,1,0 MUST be 0,0,1,0 `[MQTT-3.10.1-1]`. Payload MUST contain at least one Topic Filter `[MQTT-3.10.3-2]`.

### 3.10.4 UNSUBSCRIBE Actions

- Server MUST stop delivering new messages matching Topic Filters `[MQTT-3.10.4-2]`.
- Server MUST complete delivery of in-flight QoS 1/2 messages `[MQTT-3.10.4-3]`.
- Server MUST respond with UNSUBACK `[MQTT-3.10.4-4]`.

---

## 3.11 UNSUBACK — Unsubscribe Acknowledgement

**Table 3-9 Unsubscribe Reason Codes**

| Value | Hex  | Name                          |
|-------|------|-------------------------------|
| 0     | 0x00 | Success                       |
| 17    | 0x11 | No subscription existed       |
| 128   | 0x80 | Unspecified error             |
| 131   | 0x83 | Implementation specific error |
| 135   | 0x87 | Not authorized                |
| 143   | 0x8F | Topic Filter invalid          |
| 145   | 0x91 | Packet Identifier in use      |

---

## 3.12 PINGREQ — PING Request

Sent from Client to Server to indicate the Client is alive, request Server confirmation, or test the Network Connection. Fixed Header: type=12, Remaining Length=0.

`[MQTT-3.12.4-1]` Server MUST send a PINGRESP in response.

---

## 3.13 PINGRESP — PING Response

Sent by Server in response to PINGREQ, indicating the Server is alive. Fixed Header: type=13, Remaining Length=0.

---

## 3.14 DISCONNECT — Disconnect Notification

Final MQTT Control Packet. Indicates reason for closing the Network Connection.

`[MQTT-3.14.0-1]` Server MUST NOT send DISCONNECT until after it has sent a CONNACK with Reason Code < 0x80.

**Table 3-10 Disconnect Reason Codes**

| Value | Hex  | Name                                    | Sent By        |
|-------|------|-----------------------------------------|----------------|
| 0     | 0x00 | Normal disconnection                    | Client/Server  |
| 4     | 0x04 | Disconnect with Will Message            | Client         |
| 128   | 0x80 | Unspecified error                       | Client/Server  |
| 129   | 0x81 | Malformed Packet                        | Client/Server  |
| 130   | 0x82 | Protocol Error                          | Client/Server  |
| 131   | 0x83 | Implementation specific error           | Client/Server  |
| 135   | 0x87 | Not authorized                          | Server         |
| 137   | 0x89 | Server busy                             | Server         |
| 139   | 0x8B | Server shutting down                    | Server         |
| 141   | 0x8D | Keep Alive timeout                      | Server         |
| 142   | 0x8E | Session taken over                      | Server         |
| 143   | 0x8F | Topic Filter invalid                    | Server         |
| 144   | 0x90 | Topic Name invalid                      | Client/Server  |
| 147   | 0x93 | Receive Maximum exceeded                | Client/Server  |
| 148   | 0x94 | Topic Alias invalid                     | Client/Server  |
| 149   | 0x95 | Packet too large                        | Client/Server  |
| 150   | 0x96 | Message rate too high                   | Client/Server  |
| 151   | 0x97 | Quota exceeded                          | Client/Server  |
| 152   | 0x98 | Administrative action                   | Client/Server  |
| 153   | 0x99 | Payload format invalid                  | Client/Server  |
| 154   | 0x9A | Retain not supported                    | Server         |
| 155   | 0x9B | QoS not supported                       | Server         |
| 156   | 0x9C | Use another server                      | Server         |
| 157   | 0x9D | Server moved                            | Server         |
| 158   | 0x9E | Shared Subscriptions not supported      | Server         |
| 159   | 0x9F | Connection rate exceeded                | Server         |
| 160   | 0xA0 | Maximum connect time                    | Server         |
| 161   | 0xA1 | Subscription Identifiers not supported  | Server         |
| 162   | 0xA2 | Wildcard Subscriptions not supported    | Server         |

### 3.14.4 DISCONNECT Actions

After sending DISCONNECT:
- `[MQTT-3.14.4-1]` Sender MUST NOT send any more MQTT Control Packets.
- `[MQTT-3.14.4-2]` Sender MUST close the Network Connection.
- `[MQTT-3.14.4-3]` On receipt of DISCONNECT 0x00 (Success), Server MUST discard any Will Message without publishing it.

DISCONNECT Properties: Session Expiry Interval (Client only `[MQTT-3.14.2-2]`), Reason String, User Property, Server Reference.

---

## 3.15 AUTH — Authentication Exchange

Used for extended authentication (e.g., challenge/response). It is a Protocol Error to send AUTH if the CONNECT packet did not contain an Authentication Method.

**Table 3-11 Authenticate Reason Codes**

| Value | Hex  | Name                     | Sent By        |
|-------|------|--------------------------|----------------|
| 0     | 0x00 | Success                  | Server         |
| 24    | 0x18 | Continue authentication  | Client/Server  |
| 25    | 0x19 | Re-authenticate          | Client         |

Fixed Header bits 3,2,1,0 MUST all be 0 `[MQTT-3.15.1-1]`. AUTH Properties include Authentication Method (required), Authentication Data, Reason String, User Property.

---

# 4 Operational Behavior

## 4.1 Session State

Session State in the **Client** consists of:
- QoS 1/2 messages sent to Server but not completely acknowledged.
- QoS 2 messages received from Server but not completely acknowledged.

Session State in the **Server** consists of:
- Existence of a Session (even if empty).
- Client subscriptions (including Subscription Identifiers).
- QoS 1/2 messages sent to Client but not completely acknowledged.
- QoS 1/2 (and optionally QoS 0) messages pending transmission.
- QoS 2 messages received from Client but not completely acknowledged.
- Will Message and Will Delay Interval.
- Session expiry time.

Retained messages do NOT form part of Session State.

### 4.1.1 Storing Session State

- `[MQTT-4.1.0-1]` Client and Server MUST NOT discard Session State while Network Connection is open.
- `[MQTT-4.1.0-2]` Server MUST discard Session State when Network Connection is closed and Session Expiry Interval has passed.

## 4.2 Network Connections

MQTT requires an ordered, lossless, byte-stream transport. `[MQTT-4.2-1]` Client or Server MUST support one or more such underlying transport protocols. TCP/IP, TLS, and WebSocket are all suitable.

## 4.3 Quality of Service Levels and Protocol Flows

### 4.3.1 QoS 0: At Most Once Delivery

Sender sends PUBLISH (QoS=0, DUP=0). No response. Message arrives once or not at all.

```
Sender: PUBLISH QoS=0, DUP=0  -->  Receiver: Deliver message
```

### 4.3.2 QoS 1: At Least Once Delivery

Sender assigns Packet Identifier and sends PUBLISH (QoS=1, DUP=0). Receiver responds with PUBACK. Packet Identifier available for reuse after PUBACK received.

```
Sender: Store message
Sender: PUBLISH QoS=1, DUP=0, <PktId>  -->  Receiver: Initiate delivery
                                         <--  PUBACK <PktId>
Sender: Discard message
```

### 4.3.3 QoS 2: Exactly Once Delivery

Four-packet exchange: PUBLISH → PUBREC → PUBREL → PUBCOMP.

```
Sender: Store message
Sender: PUBLISH QoS=2, DUP=0, <PktId>  -->  Receiver: Store PktId, initiate delivery
                                         <--  PUBREC <PktId>
Sender: Discard message, store PUBREC received
Sender: PUBREL <PktId>                  -->  Receiver: Discard PktId
                                         <--  PUBCOMP <PktId>
Sender: Discard stored state
```

Key rules:
- `[MQTT-4.3.3-6]` Sender MUST NOT re-send PUBLISH once PUBREL has been sent.
- `[MQTT-4.3.3-7]` Sender MUST NOT apply Message Expiry if PUBLISH has been sent.

## 4.4 Message Delivery Retry

`[MQTT-4.4.0-1]` When a Client reconnects with Clean Start = 0 and a session is present, both Client and Server MUST resend unacknowledged PUBLISH (QoS > 0) and PUBREL packets using their original Packet Identifiers. This is the ONLY circumstance requiring resend; MUST NOT resend at other times.

## 4.5 Message Receipt

`[MQTT-4.5.0-1]` When a Server takes ownership of an incoming Application Message, it MUST add it to Session State for Clients with matching Subscriptions.

`[MQTT-4.5.0-2]` Client MUST acknowledge any PUBLISH packet according to QoS rules regardless of whether it elects to process the Application Message.

## 4.6 Message Ordering

- `[MQTT-4.6.0-1]` Client MUST re-send PUBLISH packets in original order (QoS 1 and 2).
- `[MQTT-4.6.0-2]` Client MUST send PUBACK in order of received PUBLISH (QoS 1).
- `[MQTT-4.6.0-3]` Client MUST send PUBREC in order of received PUBLISH (QoS 2).
- `[MQTT-4.6.0-4]` Client MUST send PUBREL in order of received PUBREC (QoS 2).
- `[MQTT-4.6.0-5]` Server MUST send PUBLISH to consumers in the order received for Ordered Topics.
- `[MQTT-4.6.0-6]` Server MUST treat every Topic as Ordered when forwarding on Non-shared Subscriptions.

## 4.7 Topic Names and Topic Filters

### 4.7.1 Topic Wildcards

`[MQTT-4.7.0-1]` Wildcard characters MUST NOT be used within a Topic Name (only Topic Filters).

#### 4.7.1.1 Topic Level Separator

Forward slash `/` (U+002F) separates topic levels and provides hierarchical structure.

#### 4.7.1.2 Multi-Level Wildcard

`#` (U+0023) matches any number of levels. `[MQTT-4.7.1-1]` MUST be either on its own or following a `/`, and MUST be the last character.

Examples: `sport/tennis/player1/#` matches `sport/tennis/player1`, `sport/tennis/player1/ranking`, etc.

#### 4.7.1.3 Single-Level Wildcard

`+` (U+002B) matches exactly one topic level. `[MQTT-4.7.1-2]` MUST occupy an entire level of the filter.

Examples: `sport/tennis/+` matches `sport/tennis/player1` and `sport/tennis/player2`.

### 4.7.2 Topics Beginning with $

`[MQTT-4.7.2-1]` Server MUST NOT match Topic Filters starting with `#` or `+` against Topic Names beginning with `$`. `$SYS/` is widely used for Server-specific information.

### 4.7.3 Topic Semantic and Usage

- `[MQTT-4.7.3-1]` All Topic Names and Topic Filters MUST be at least one character long.
- `[MQTT-4.7.3-2]` MUST NOT include the null character U+0000.
- `[MQTT-4.7.3-3]` MUST NOT encode to more than 65,535 bytes.
- `[MQTT-4.7.3-4]` Server MUST NOT perform any normalization of Topic Names or Topic Filters.
- Topic Names and Topic Filters are case sensitive.

## 4.8 Subscriptions

### 4.8.1 Non-Shared Subscriptions

Associated only with the MQTT Session that created it. Each subscription has a unique Topic Filter per Session. Multiple Clients with Non-shared Subscriptions each receive their own copy.

### 4.8.2 Shared Subscriptions

Can be associated with multiple Sessions. Only one of the associated Sessions receives each matching message. Useful for load-balancing.

**Format:** `$share/{ShareName}/{filter}`

- `[MQTT-4.8.2-1]` Topic Filter MUST start with `$share/` and ShareName MUST be at least one character.
- `[MQTT-4.8.2-2]` ShareName MUST NOT contain `/`, `+`, or `#`, and MUST be followed by `/` and a Topic Filter.

## 4.9 Flow Control

Receive Maximum establishes a send quota limiting PUBLISH QoS > 0 packets in flight.

- `[MQTT-4.9.0-1]` Client/Server MUST set initial send quota to a non-zero value not exceeding Receive Maximum.
- `[MQTT-4.9.0-2]` If send quota reaches zero, MUST NOT send more PUBLISH packets with QoS > 0.
- `[MQTT-4.9.0-3]` MUST continue to process and respond to all other MQTT Control Packets even if quota is zero.
- Send quota increments by 1 on each PUBACK, PUBCOMP, or PUBREC with Reason Code ≥ 0x80.

## 4.10 Request / Response

Three properties enable request/response interaction:

- **Response Topic** (0x08): Topic for response message; its presence identifies the Application Message as a request.
- **Correlation Data** (0x09): Used by Requester to identify which request a Response Message is for.
- **Request Response Information** (0x19) in CONNECT: Requests Server to send Response Information.
- **Response Information** (0x1A) in CONNACK: Basis for creating a Response Topic.

### 4.10.1 Basic Request/Response Flow (non-normative)

1. Requester publishes a Request Message with a Response Topic.
2. Responder subscribed to matching filter receives the request.
3. Responder publishes to the Response Topic, copying Correlation Data.
4. Requester (subscribed to Response Topic) receives the response.

## 4.11 Server Redirection

A Server can request the Client use another Server via CONNACK or DISCONNECT with Reason Codes:
- **0x9C (Use another server)**: Temporarily switch to another server.
- **0x9D (Server moved)**: Permanently switch to another server.

A Server Reference property specifies a space-separated list of host[:port] references.

## 4.12 Enhanced Authentication

Extends basic authentication to include challenge/response style.

- `[MQTT-4.12.0-1]` If Server doesn't support the Authentication Method, it MAY send CONNACK 0x8C or 0x87 and MUST close the Network Connection.
- `[MQTT-4.12.0-2]` Server sends AUTH with Reason Code 0x18 (Continue authentication) if more data needed.
- `[MQTT-4.12.0-3]` Client responds to AUTH with another AUTH containing 0x18.
- `[MQTT-4.12.0-5]` All AUTH packets and successful CONNACK MUST include the same Authentication Method as the CONNECT.
- `[MQTT-4.12.0-6]` If Client doesn't include Authentication Method in CONNECT, Server MUST NOT send AUTH.

### 4.12.1 Re-authentication

Client can initiate re-authentication at any time after CONNACK by sending AUTH with Reason Code 0x19 (Re-authenticate), using the same Authentication Method `[MQTT-4.12.1-1]`.

## 4.13 Handling Errors

### 4.13.1 Malformed Packet and Protocol Errors

Reason Codes for Malformed Packet and Protocol Errors:

| Hex  | Meaning                               |
|------|---------------------------------------|
| 0x81 | Malformed Packet                      |
| 0x82 | Protocol Error                        |
| 0x93 | Receive Maximum exceeded              |
| 0x95 | Packet too large                      |
| 0x9A | Retain not supported                  |
| 0x9B | QoS not supported                     |
| 0x9E | Shared Subscriptions not supported    |
| 0xA1 | Subscription Identifiers not supported|
| 0xA2 | Wildcard Subscriptions not supported  |

`[MQTT-4.13.1-1]` When a Server detects a Malformed Packet or Protocol Error, it MUST close the Network Connection.

### 4.13.2 Other Errors

`[MQTT-4.13.2-1]` If CONNACK or DISCONNECT specifies a Reason Code ≥ 0x80, the Network Connection MUST be closed whether or not the packet is sent.

---

# 5 Security (non-normative)

## 5.1 Introduction

MQTT implementations should address threats including:
- Device compromise
- Data at rest accessibility
- Denial of Service attacks
- Communications interception
- Injection of spoofed MQTT Control Packets

Recommended: Server implementations offering TLS should use TCP port 8883.

## 5.2 MQTT Solutions: Security and Certification

Consider conformance with NIST Cyber Security Framework, PCI-DSS, FIPS-140-2, and NSA Suite B.

## 5.3 Lightweight Cryptography and Constrained Devices

- AES is widely adopted with hardware support.
- ChaCha20 encrypts/decrypts faster in software.
- ISO 29192 provides cryptographic primitives tuned for constrained devices.

## 5.4 Implementation Notes

### 5.4.1 Authentication of Clients by the Server

Use CONNECT User Name/Password fields, external authentication (LDAP, OAuth), or MQTT v5.0 enhanced authentication. TLS certificates can authenticate the Client.

### 5.4.2 Authorization of Clients by the Server

Check Client is authorized after authentication. Control publish/subscribe access per Topic. Consider limiting access to broad filters like `#`.

### 5.4.3 Authentication of the Server by the Client

Use TLS certificates. Consider TLS Server Name Indication for multiple hostnames on a single IP. MQTT v5.0 enhanced authentication can provide mutual authentication.

### 5.4.4 Integrity

Applications can include hash values in Application Messages. TLS provides hash algorithms. VPNs can provide integrity across network segments.

### 5.4.5 Privacy

Use TLS with non-NULL cipher suites. Applications can independently encrypt payload content. TLS does not protect Topic Names unless the whole connection is encrypted.

### 5.4.9 Handling of Disallowed Unicode Code Points

If Server doesn't validate code points but a subscribing Client does, a malicious publisher can force subscriber disconnections. Remedies:

1. Change Server to reject strings with Disallowed Unicode code points.
2. Change Client library to tolerate Disallowed code points without disconnecting.

### 5.4.12 Security Profiles

- **Clear communication profile**: No security mechanisms.
- **Secured network communication profile**: VPN or physically secure network.
- **Secured transport profile**: TLS providing authentication, integrity, and privacy.
- **Industry specific security profiles**: Defined threat models and specific security mechanisms.

---

# 6 Using WebSocket as a Network Transport

If MQTT is transported over a WebSocket connection:

- `[MQTT-6.0.0-1]` MQTT Control Packets MUST be sent in WebSocket binary data frames.
- `[MQTT-6.0.0-2]` Receiver MUST NOT assume MQTT Control Packets are aligned on WebSocket frame boundaries.
- `[MQTT-6.0.0-3]` Client MUST include `mqtt` in the list of WebSocket Sub Protocols offered.
- `[MQTT-6.0.0-4]` Server MUST return `mqtt` as the selected WebSocket Subprotocol name.

## 6.1 IANA Considerations

| Field                     | Value                                                                          |
|---------------------------|--------------------------------------------------------------------------------|
| Subprotocol Identifier    | mqtt                                                                           |
| Subprotocol Common Name   | mqtt                                                                           |
| Subprotocol Definition    | http://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html               |

---

# 7 Conformance

## 7.1 Conformance Clauses

### 7.1.1 MQTT Server Conformance Clause

An MQTT Server conforms only if it:
1. Sends MQTT Control Packets matching the format in Chapters 2 and 3.
2. Follows Topic matching rules in section 4.7 and Subscription rules in section 4.8.
3. Satisfies all MUST-level requirements in Chapters 1–4 and 6 (excluding Client-only requirements).
4. Does not require extensions outside the specification for interoperability.

### 7.1.2 MQTT Client Conformance Clause

An MQTT Client conforms only if it:
1. Sends MQTT Control Packets matching the format in Chapters 2 and 3.
2. Satisfies all MUST-level requirements in Chapters 1–4 and 6 (excluding Server-only requirements).
3. Does not require extensions outside the specification for interoperability.

---

## Appendix A: Acknowledgments

Special thanks to Dr. Andy Stanford-Clark and Arlen Nipper as original inventors of MQTT.

TC Chair: Richard Coppen (IBM).

Editors: Andrew Banks, Ed Briggs, Ken Borgendale, Rahul Gupta.

---

## Appendix C: Summary of New Features in MQTT v5.0 (non-normative)

- **Session expiry**: Split Clean Session into Clean Start flag + Session Expiry Interval.
- **Message expiry**: Expiry interval settable when publishing.
- **Reason code on all ACKs**: CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH all carry Reason Codes.
- **Reason string on all ACKs**: Optional human-readable diagnostic string.
- **Server disconnect**: Server can send DISCONNECT with reason code.
- **Payload format indicator**: Indicates whether payload is UTF-8 or unspecified bytes.
- **Request/Response**: Response Topic, Correlation Data, Response Information properties.
- **Shared Subscriptions**: Load-balance messages across multiple consuming Clients.
- **Subscription Identifiers**: Associate subscriptions with callbacks or state.
- **Topic Alias**: Integer replacing long Topic Names to reduce bandwidth.
- **Flow control**: Receive Maximum property for QoS 1/2 in-flight message limiting.
- **User properties**: Application-defined name-value pairs on most packets.
- **Maximum Packet Size**: Clients and Servers can declare maximum packet size.
- **Optional server feature availability**: Clients can discover Server capabilities (Wildcard Subscriptions, Subscription Identifiers, Shared Subscriptions, Retain, Max QoS).
- **Enhanced authentication**: Challenge/response authentication via AUTH packet.
- **Server reference**: Redirect Clients to another Server (temporary or permanent).
- **Will delay interval**: Delay publishing Will Message after disconnect.
- **Assigned Client Identifier**: Server assigns and returns identifier for zero-length ClientID.
- **Server Keep Alive**: Server can specify Keep Alive value in CONNACK.
