/**
 * Unified WebSocket protocol types for bisque UI.
 *
 * Mirrors the Rust types in `crates/protocol/src/ws.rs`.
 * All messages are MessagePack-encoded binary frames.
 */

import type { Operation } from "./api"

export const WS_PROTOCOL_VERSION = 1

// ---------------------------------------------------------------------------
// Server → Client messages
// ---------------------------------------------------------------------------

export type ServerMessage =
  | { type: "Handshake"; protocol_version: number; session_id: number; catalog_seq: number; server_seq: number }
  | { type: "Response"; request_id: number } & ResponseResult
  | { type: "CatalogEvent"; seq: number; catalog: string; event: CatalogEventKind }
  | { type: "OperationUpdate"; seq: number; operation: Operation }
  | { type: "OperationsSnapshot"; seq: number; operations: Operation[] }
  | { type: "Heartbeat"; seq: number; server_time_ms: number }
  | { type: "SnapshotRequired"; seq: number; catalog: string }
  | { type: "Close"; reason: string }

export type ResponseResult =
  | { status: "ok"; method: string; [key: string]: unknown }
  | { status: "error"; code: number; message: string }

export type CatalogEventKind =
  | { type: "table_created"; table: string; schema_ipc: number[] }
  | { type: "table_dropped"; table: string }
  | { type: "active_version_bumped"; table: string; version: number }
  | { type: "segment_sealed"; table: string; active_version: number; sealed_version: number }
  | { type: "segment_promoted"; table: string; s3_manifest_version: number }

// ---------------------------------------------------------------------------
// Client → Server messages
// ---------------------------------------------------------------------------

export type ClientMessage =
  | { type: "Handshake"; protocol_version: number; token: string; last_seen_seq: number; subscribe_catalogs: string[] }
  | { type: "Request"; request_id: number; method: string; [key: string]: unknown }
  | { type: "Pin"; catalog: string; table: string; tier: string; version: number }
  | { type: "Unpin"; catalog: string; table: string; tier: string; version: number }
  | { type: "Heartbeat"; last_seen_seq: number }
  | { type: "Close" }
