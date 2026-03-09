/**
 * Unified WebSocket protocol types for bisque UI.
 *
 * Mirrors the Rust types in `crates/protocol/src/ws.rs`.
 * Control messages are MessagePack-encoded binary frames.
 * SQL result data is sent as raw Arrow IPC binary frames with a 4-byte
 * request_id prefix (big-endian).
 */

import type { Account, Operation } from "./api"

export const WS_PROTOCOL_VERSION = 2

// ---------------------------------------------------------------------------
// Server → Client messages
// ---------------------------------------------------------------------------

export type ServerMessage =
  | { type: "Handshake"; protocol_version: number }
  | { type: "HandshakeComplete"; session_id: number; catalog_seq: number;
      token: string; user_id: number; username: string;
      accounts: Account[]; default_tenant_id?: number; default_tenant_name?: string }
  | { type: "Response"; request_id: number } & ResponseResult
  | { type: "CatalogEvent"; seq: number; catalog: string; event: CatalogEventKind }
  | { type: "OperationUpdate"; seq: number; operation: Operation }
  | { type: "OperationsSnapshot"; seq: number; operations: Operation[] }
  | { type: "Heartbeat"; seq: number; server_time_ms: number }
  | { type: "SnapshotRequired"; seq: number; catalog: string }
  | { type: "Close"; reason: string }
  | { type: "SqlResultHeader"; request_id: number; columns: { name: string; type: string }[] }
  | { type: "SqlResultChunk"; request_id: number; rows: Record<string, unknown>[]; rows_so_far: number }
  | { type: "SqlResultComplete"; request_id: number; row_count: number }

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
  | { type: "Handshake"; protocol_version: number; token: string; username: string; password: string; last_seen_seq: number; subscribe_catalogs: string[] }
  | { type: "Request"; request_id: number; method: string; [key: string]: unknown }
  | { type: "Pin"; catalog: string; table: string; tier: string; version: number }
  | { type: "Unpin"; catalog: string; table: string; tier: string; version: number }
  | { type: "Heartbeat"; last_seen_seq: number }
  | { type: "Close" }
