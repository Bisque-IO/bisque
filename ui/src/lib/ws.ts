/**
 * Unified WebSocket client for the bisque UI.
 *
 * Provides:
 * - Real-time push events (catalog mutations, operation updates)
 * - Request/response RPC over WebSocket (replaces most HTTP API calls)
 * - Reliability: protocol versioning, sequence tracking, gap detection,
 *   connection TTL refresh, jittered exponential backoff, heartbeats,
 *   handshake timeout, heartbeat timeout, request retry
 */

import { encode, decode } from "@msgpack/msgpack"
import { tableFromIPC } from "apache-arrow"
import { useAuthStore } from "@/stores/auth"
import { useClusterStore } from "@/stores/cluster"
import { useConnectionStore } from "@/stores/connection"
import type { ServerMessage, ClientMessage, CatalogEventKind } from "./ws-protocol"
import { WS_PROTOCOL_VERSION } from "./ws-protocol"
import type { Operation, ClusterStatus, Tenant, ApiKeyEntry, TenantLimits, Account, SqlResult, CatalogEntry } from "./api"

// Re-export for backward compat
export type { ServerMessage, CatalogEventKind as CatalogEvent }

type PushHandler = (msg: ServerMessage) => void

interface PendingRequest {
  resolve: (data: Record<string, unknown>) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
  // Store enough info to retry on reconnect
  msg: ClientMessage
  retries: number
}

interface PendingSqlStream {
  resolve: (result: SqlResult) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
  columns: { name: string; type: string }[]
  rows: Record<string, unknown>[]
  headerReceived: boolean
  onHeader?: (columns: { name: string; type: string }[]) => void
  onChunk?: (rows: Record<string, unknown>[], rowsSoFar: number) => void
}

export interface SqlStreamOptions {
  onHeader?: (columns: { name: string; type: string }[]) => void
  onChunk?: (rows: Record<string, unknown>[], rowsSoFar: number) => void
}

// Request ID wraps at 2^31 to stay safely within JS integer precision
const MAX_REQUEST_ID = 0x7fffffff

export class BisqueWsClient {
  private ws: WebSocket | null = null
  private handlers: Set<PushHandler> = new Set()
  private pendingRequests: Map<number, PendingRequest> = new Map()
  private pendingSqlStreams: Map<number, PendingSqlStream> = new Map()
  private nextRequestId = 1
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectDelay = 1000
  private readonly maxReconnectDelay = 30000
  private _connected = false
  private _handshakeComplete = false
  private lastSeenSeq = 0 // preserved across reconnects for WAL replay
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private heartbeatTimeoutTimer: ReturnType<typeof setTimeout> | null = null
  private handshakeTimeoutTimer: ReturnType<typeof setTimeout> | null = null
  private subscribeCatalogs: string[] = []
  private authToken = ""
  private readonly requestTimeoutMs = 15000
  private readonly handshakeTimeoutMs = 10000
  private readonly heartbeatIntervalMs = 15000
  private readonly heartbeatTimeoutMs = 45000 // 3 missed heartbeats
  // M4: Allow up to 3 retries for in-flight requests on reconnect.
  private readonly maxRequestRetries = 3
  private intentionalClose = false
  // R3: Track retry handler subscriptions so they can be cleaned up on disconnect.
  private retryUnsubs: Array<() => void> = []
  // Pending credentials for login-via-WS flow (cleared after use).
  private pendingCredentials: { username: string; password: string } | null = null
  // Close reason from server Close message (captured for login error reporting).
  private serverCloseReason = ""

  get connected(): boolean {
    return this._connected && this._handshakeComplete
  }

  /** Subscribe to push events. Returns unsubscribe function. */
  onPush(handler: PushHandler): () => void {
    this.handlers.add(handler)
    return () => this.handlers.delete(handler)
  }

  // Keep old name for backward compat with use-ws.ts
  subscribe(handler: PushHandler): () => void {
    return this.onPush(handler)
  }

  /** Set which catalogs to subscribe to (sent during handshake). */
  setSubscribeCatalogs(catalogs: string[]): void {
    this.subscribeCatalogs = catalogs
  }

  connect(): void {
    if (this.connected) return // already handshaked, skip

    // For reconnects, require a stored token. For login flow, pendingCredentials is set.
    const token = useAuthStore.getState().token
    if (!token && !this.pendingCredentials) return

    // C5: Guard against re-entry — close any existing socket before connecting.
    if (this.ws) {
      this.ws.onclose = null
      this.ws.onerror = null
      this.ws.onmessage = null
      this.ws.close()
      this.ws = null
    }

    this.intentionalClose = false
    this.authToken = token || ""
    useConnectionStore.getState().setState("connecting")
    const cluster = useClusterStore.getState().activeCluster
    let url: string

    // S2: Token is sent in the handshake frame, NOT the URL, to prevent
    // exposure in HTTP access logs and browser history.
    if (cluster.url) {
      const parsed = new URL(cluster.url)
      const protocol = parsed.protocol === "https:" ? "wss:" : "ws:"
      url = `${protocol}//${parsed.host}/_bisque/ws`
    } else {
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:"
      url = `${protocol}//${window.location.host}/_bisque/ws`
    }

    this.ws = new WebSocket(url)
    this.ws.binaryType = "arraybuffer"

    this.ws.onopen = () => {
      this._connected = true
      // M17: Don't reset reconnectDelay here — wait for handshake success.

      this.startHandshakeTimeout()
    }

    this.ws.onmessage = (event) => {

      try {
        // L1: Only accept binary frames — no JSON fallback.
        if (!(event.data instanceof ArrayBuffer)) {
          console.warn("[bisque-ws] Ignoring non-binary message (expected binary)")
          return
        }
        const bytes = new Uint8Array(event.data)

        // Check for Arrow IPC binary frame: [4-byte request_id BE] ++ [Arrow IPC bytes]
        // If the first 4 bytes match a pending SQL stream that has received its header,
        // treat as Arrow IPC data frame.
        if (bytes.byteLength > 4) {
          const view = new DataView(event.data)
          const possibleReqId = view.getUint32(0, false) // big-endian
          const stream = this.pendingSqlStreams.get(possibleReqId)
          if (stream && stream.headerReceived) {
            // Arrow IPC binary frame — decode and convert to rows
            const ipcBytes = bytes.subarray(4) // zero-copy view, not a copy
            try {
              const table = tableFromIPC(ipcBytes)
              const rows = arrowTableToRows(table)
              stream.rows.push(...rows)
              stream.onChunk?.(stream.rows, stream.rows.length)
              // Reset timeout
              clearTimeout(stream.timer)
              stream.timer = setTimeout(() => {
                this.pendingSqlStreams.delete(possibleReqId)
                stream.reject(new Error("SQL stream timed out"))
              }, this.requestTimeoutMs)
            } catch (ipcErr) {
              console.error("[bisque-ws] Failed to decode Arrow IPC:", ipcErr)
            }
            return
          }
        }

        // MessagePack control frame
        const msg = decode(bytes) as ServerMessage
        this.handleMessage(msg)
      } catch (err) {
        console.error("[bisque-ws] Failed to decode message:", err)
      }
    }

    this.ws.onclose = (event) => {
      this._connected = false
      this._handshakeComplete = false
      this.clearHandshakeTimeout()
      this.stopHeartbeat()
      this.stopHeartbeatTimeout()
      useConnectionStore.getState().setState("disconnected")
      if (!this.intentionalClose) {
        // C6: Only retry pending requests on unintentional close — prevents retry leak.
        this.handlePendingOnDisconnect()
        useConnectionStore.getState().incrementReconnectAttempts()
        console.warn(`[bisque-ws] Connection closed (code=${event.code}, reason=${event.reason || "none"}). Reconnecting...`)
        this.scheduleReconnect()
      } else {
        // Intentional close: reject all pending, don't retry.
        this.rejectAllPending("disconnected")
      }
    }

    this.ws.onerror = (event) => {
      // L12: Don't call ws.close() — browser fires onclose automatically after onerror.
      console.error("[bisque-ws] WebSocket error:", event)
    }
  }

  /** Login via WS handshake: connect, send credentials, wait for HandshakeComplete. */
  async loginAndConnect(
    username: string,
    password: string,
  ): Promise<ServerMessage & { type: "HandshakeComplete" }> {
    this.pendingCredentials = { username, password }

    return new Promise((resolve, reject) => {
      const unsub = this.onPush((msg) => {
        if (msg.type === "HandshakeComplete") {
          unsub()
          resolve(msg)
        }
      })

      this.connect()

      // If socket closes before HandshakeComplete (auth failure, timeout), reject
      if (this.ws) {
        const savedOnClose = this.ws.onclose
        this.ws.onclose = (event) => {
          unsub()
          const reason = this.serverCloseReason || event.reason || "Login failed"
          this.serverCloseReason = ""
          reject(new Error(reason))
          if (savedOnClose) (savedOnClose as (ev: CloseEvent) => void).call(this.ws, event)
        }
      }
    })
  }

  disconnect(): void {
    this.intentionalClose = true
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.clearHandshakeTimeout()
    this.stopHeartbeat()
    this.stopHeartbeatTimeout()
    // R3: Clean up any pending retry handler subscriptions.
    for (const unsub of this.retryUnsubs) unsub()
    this.retryUnsubs = []
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendClientMsg({ type: "Close" })
    }
    this.ws?.close()
    this.ws = null
    this._connected = false
    this._handshakeComplete = false
    this.rejectAllPending("disconnected")
  }

  // ---------------------------------------------------------------------------
  // Request/Response API (replaces HTTP calls)
  // ---------------------------------------------------------------------------

  async listOperations(params?: {
    type?: string
    tier?: string
    status?: string
  }): Promise<Operation[]> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "list_operations",
      op_type: params?.type,
      tier: params?.tier,
      status: params?.status,
    })
    return resp.operations as Operation[]
  }

  async getOperation(opId: string): Promise<Operation> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "get_operation",
      op_id: opId,
    })
    return resp.operation as Operation
  }

  async cancelOperation(opId: string): Promise<{ op_id: string; message: string }> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "cancel_operation",
      op_id: opId,
    })
    return resp as unknown as { op_id: string; message: string }
  }

  async submitReindex(
    bucket: string,
    table: string,
  ): Promise<{ op_id: string; message: string }> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "submit_reindex",
      bucket,
      table,
    })
    return resp as unknown as { op_id: string; message: string }
  }

  async submitCompact(
    bucket: string,
    table: string,
  ): Promise<{ op_id: string; message: string }> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "submit_compact",
      bucket,
      table,
    })
    return resp as unknown as { op_id: string; message: string }
  }

  async listCatalogs(tenantId: number): Promise<CatalogEntry[]> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "list_catalogs",
      tenant_id: tenantId,
    })
    return resp.catalogs as CatalogEntry[]
  }

  async createCatalog(
    tenantId: number,
    name: string,
    engine: string,
    config = "",
  ): Promise<{ catalog_id: number; raft_group_id: number }> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "create_catalog",
      tenant_id: tenantId,
      name,
      engine,
      config,
    })
    return resp as unknown as { catalog_id: number; raft_group_id: number }
  }

  async getCatalog(bucket: string): Promise<Record<string, unknown>> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "get_catalog",
      bucket,
    })
    return resp.catalog as Record<string, unknown>
  }

  async getClusterStatus(): Promise<ClusterStatus> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "cluster_status",
    })
    return resp.cluster as ClusterStatus
  }

  async listTenants(accountId: number): Promise<Tenant[]> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "list_tenants",
      account_id: accountId,
    })
    return resp.tenants as Tenant[]
  }

  async updateTenantLimits(tenantId: number, limits: TenantLimits): Promise<void> {
    await this.request({
      type: "Request",
      request_id: 0,
      method: "update_tenant_limits",
      tenant_id: tenantId,
      limits,
    })
  }

  async deleteTenant(tenantId: number): Promise<void> {
    await this.request({
      type: "Request",
      request_id: 0,
      method: "delete_tenant",
      tenant_id: tenantId,
    })
  }

  async deleteCatalog(tenantId: number, catalogId: number): Promise<void> {
    await this.request({
      type: "Request",
      request_id: 0,
      method: "delete_catalog",
      tenant_id: tenantId,
      catalog_id: catalogId,
    })
  }

  async listApiKeys(tenantId: number): Promise<ApiKeyEntry[]> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "list_api_keys",
      tenant_id: tenantId,
    })
    return resp.api_keys as ApiKeyEntry[]
  }

  async revokeApiKey(keyId: number): Promise<void> {
    await this.request({
      type: "Request",
      request_id: 0,
      method: "revoke_api_key",
      key_id: keyId,
    })
  }

  async createTable(
    catalog: string,
    table: string,
    schemaJson: string,
  ): Promise<{ table: string }> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "create_table",
      catalog,
      table,
      schema_json: schemaJson,
    })
    return resp as unknown as { table: string }
  }

  async dropTable(catalog: string, table: string): Promise<{ table: string }> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "drop_table",
      catalog,
      table,
    })
    return resp as unknown as { table: string }
  }

  async executeSql(
    catalog: string,
    sql: string,
    options?: SqlStreamOptions,
  ): Promise<SqlResult> {
    if (!this.connected) {
      throw new Error("WebSocket not connected")
    }
    const requestId = this.allocRequestId()
    const msg: ClientMessage & { request_id: number } = {
      type: "Request",
      request_id: requestId,
      method: "execute_sql",
      catalog,
      sql,
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(requestId)
        this.pendingSqlStreams.delete(requestId)
        reject(new Error(`SQL request ${requestId} timed out after ${this.requestTimeoutMs}ms`))
      }, this.requestTimeoutMs)

      // Register in pendingRequests for fast-path (single-frame Response)
      this.pendingRequests.set(requestId, {
        resolve: (data) => {
          // Fast path hit — clean up stream entry and resolve
          this.pendingSqlStreams.delete(requestId)
          resolve(data as unknown as SqlResult)
        },
        reject: (err) => {
          this.pendingSqlStreams.delete(requestId)
          reject(err)
        },
        timer,
        msg,
        retries: 0,
      })

      // Also register in pendingSqlStreams for chunked/Arrow IPC path
      this.pendingSqlStreams.set(requestId, {
        resolve,
        reject,
        timer,
        columns: [],
        rows: [],
        headerReceived: false,
        onHeader: options?.onHeader,
        onChunk: options?.onChunk,
      })

      this.sendClientMsg(msg)
    })
  }

  async enableOtel(catalog: string): Promise<{ tables_created: string[] }> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "enable_otel",
      catalog,
    })
    return resp as unknown as { tables_created: string[] }
  }

  async listAccounts(): Promise<Account[]> {
    const resp = await this.request({
      type: "Request",
      request_id: 0,
      method: "list_accounts",
    })
    return resp.accounts as Account[]
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  private allocRequestId(): number {
    const id = this.nextRequestId
    this.nextRequestId = this.nextRequestId >= MAX_REQUEST_ID ? 1 : this.nextRequestId + 1
    return id
  }

  private async request(
    msg: ClientMessage & { request_id: number },
  ): Promise<Record<string, unknown>> {
    if (!this.connected) {
      throw new Error("WebSocket not connected")
    }
    const requestId = this.allocRequestId()
    msg.request_id = requestId

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(requestId)
        reject(new Error(`Request ${requestId} timed out after ${this.requestTimeoutMs}ms`))
      }, this.requestTimeoutMs)

      this.pendingRequests.set(requestId, { resolve, reject, timer, msg, retries: 0 })
      this.sendClientMsg(msg)
    })
  }

  private handleMessage(msg: ServerMessage): void {
    // Track seq for all push messages — gap detection
    // L13: Skip Heartbeat seq from gap detection since it's a keepalive, not a data event.
    if ("seq" in msg && typeof msg.seq === "number" && msg.type !== "Heartbeat") {
      const expectedSeq = this.lastSeenSeq + 1
      if (this.lastSeenSeq > 0 && msg.seq > expectedSeq) {
        const gap = msg.seq - expectedSeq
        console.warn(
          `[bisque-ws] Sequence gap detected: expected=${expectedSeq}, got=${msg.seq} (${gap} missing). ` +
            "Dispatching SnapshotRequired for auto-recovery.",
        )
        // M2: Auto-trigger snapshot re-fetch on sequence gap instead of just logging.
        // H10: Do NOT advance lastSeenSeq past the gap — let server replay fill the gap.
        for (const handler of this.handlers) {
          handler({ type: "SnapshotRequired", seq: msg.seq, catalog: "*" } as ServerMessage)
        }
        return
      }
      this.lastSeenSeq = msg.seq
    }

    switch (msg.type) {
      case "Handshake": {
        this.clearHandshakeTimeout()
        if (msg.protocol_version !== WS_PROTOCOL_VERSION) {
          console.error(
            `[bisque-ws] Protocol version mismatch: server=${msg.protocol_version}, client=${WS_PROTOCOL_VERSION}. ` +
              "Will reconnect with backoff.",
          )
          // M18: Force maximum backoff to avoid rapid reconnect loops on version mismatch.
          this.reconnectDelay = this.maxReconnectDelay
          this.ws?.close()
          return
        }
        // Send client handshake with credentials (login) or token (reconnect).
        const creds = this.pendingCredentials
        this.pendingCredentials = null
        this.sendClientMsg({
          type: "Handshake",
          protocol_version: WS_PROTOCOL_VERSION,
          token: this.authToken,
          username: creds?.username ?? "",
          password: creds?.password ?? "",
          last_seen_seq: this.lastSeenSeq,
          subscribe_catalogs: this.subscribeCatalogs,
        })
        // Don't mark connected yet — wait for HandshakeComplete from server.
        break
      }

      case "HandshakeComplete": {
        // M17: Reset backoff on successful handshake, not TCP open.
        this.reconnectDelay = 1000
        // Store the token for future reconnects.
        this.authToken = msg.token
        this._handshakeComplete = true
        this.startHeartbeat()
        this.resetHeartbeatTimeout()
        useConnectionStore.getState().setState("connected")
        useConnectionStore.getState().resetReconnectAttempts()
        // Dispatch to handlers so they know the connection is ready
        for (const handler of this.handlers) {
          handler(msg)
        }
        break
      }

      case "Response": {
        const pending = this.pendingRequests.get(msg.request_id)
        if (pending) {
          this.pendingRequests.delete(msg.request_id)
          clearTimeout(pending.timer)
          if (msg.status === "ok") {
            pending.resolve(msg as unknown as Record<string, unknown>)
          } else if (msg.status === "error") {
            // Auto-retry on 429 rate limit (up to 3 times with backoff)
            if (msg.code === 429 && pending.retries < 3) {
              pending.retries++
              const delay = pending.retries * 250
              const newId = this.allocRequestId()
              const newMsg = { ...pending.msg, request_id: newId } as ClientMessage & { request_id: number }
              const timer = setTimeout(() => {
                this.pendingRequests.delete(newId)
                pending.reject(new Error(`[429] Rate limited after ${pending.retries} retries`))
              }, this.requestTimeoutMs)
              setTimeout(() => {
                this.pendingRequests.set(newId, { ...pending, timer, msg: newMsg })
                this.sendClientMsg(newMsg)
              }, delay)
            } else {
              pending.reject(new Error(`[${msg.code}] ${msg.message}`))
            }
          } else {
            // C7: Unknown status — reject instead of leaking the promise.
            pending.reject(new Error(`Unknown response status: ${(msg as Record<string, unknown>).status}`))
          }
        } else {
          console.warn(`[bisque-ws] Received response for unknown request_id=${msg.request_id}`)
        }
        break
      }

      case "Heartbeat": {
        this.resetHeartbeatTimeout()
        break
      }

      case "SnapshotRequired": {
        console.warn(
          `[bisque-ws] Server requires snapshot for catalog="${msg.catalog}". Client fell behind.`,
        )
        // Dispatch to handlers so UI can trigger a catalog re-fetch
        for (const handler of this.handlers) {
          handler(msg)
        }
        break
      }

      case "SqlResultHeader": {
        const stream = this.pendingSqlStreams.get(msg.request_id)
        if (stream) {
          // Streaming path — remove from pendingRequests so fast-path doesn't interfere
          this.pendingRequests.delete(msg.request_id)
          stream.columns = msg.columns
          stream.headerReceived = true
          stream.onHeader?.(msg.columns)
          // Reset timeout since we're receiving data
          clearTimeout(stream.timer)
          stream.timer = setTimeout(() => {
            this.pendingSqlStreams.delete(msg.request_id)
            stream.reject(new Error("SQL stream timed out"))
          }, this.requestTimeoutMs)
        }
        break
      }

      case "SqlResultChunk": {
        const stream = this.pendingSqlStreams.get(msg.request_id)
        if (stream) {
          stream.rows.push(...msg.rows)
          stream.onChunk?.(stream.rows, msg.rows_so_far)
          // Reset timeout
          clearTimeout(stream.timer)
          stream.timer = setTimeout(() => {
            this.pendingSqlStreams.delete(msg.request_id)
            stream.reject(new Error("SQL stream timed out"))
          }, this.requestTimeoutMs)
        }
        break
      }

      case "SqlResultComplete": {
        const stream = this.pendingSqlStreams.get(msg.request_id)
        if (stream) {
          this.pendingSqlStreams.delete(msg.request_id)
          clearTimeout(stream.timer)
          stream.resolve({
            columns: stream.columns,
            rows: stream.rows,
          })
        }
        break
      }

      case "Close": {
        this.serverCloseReason = msg.reason
        if (msg.reason === "ttl_refresh") {
          // Transparent TTL refresh — reconnect immediately with short delay
          // lastSeenSeq is preserved so server can replay from WAL
          this.reconnectDelay = 100
          this.ws?.close()
          // onclose handler will trigger scheduleReconnect
        } else {
          console.warn("[bisque-ws] Server closed WebSocket:", msg.reason)
          this.disconnect()
        }
        break
      }

      default:
        // Push message: dispatch to all handlers
        for (const handler of this.handlers) {
          handler(msg)
        }
    }
  }

  private sendClientMsg(msg: ClientMessage): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(encode(msg))
      } catch (err) {
        // E5: Send failure likely means a broken connection — trigger reconnect.
        console.error("[bisque-ws] Failed to send message, closing connection:", err)
        this.ws?.close()
      }
    }
  }

  private startHandshakeTimeout(): void {
    this.clearHandshakeTimeout()
    this.handshakeTimeoutTimer = setTimeout(() => {
      if (!this._handshakeComplete) {
        console.error(
          `[bisque-ws] Handshake timeout after ${this.handshakeTimeoutMs}ms. Closing connection.`,
        )
        this.ws?.close()
      }
    }, this.handshakeTimeoutMs)
  }

  private clearHandshakeTimeout(): void {
    if (this.handshakeTimeoutTimer) {
      clearTimeout(this.handshakeTimeoutTimer)
      this.handshakeTimeoutTimer = null
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat()
    this.heartbeatTimer = setInterval(() => {
      this.sendClientMsg({ type: "Heartbeat", last_seen_seq: this.lastSeenSeq })
    }, this.heartbeatIntervalMs)
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  private resetHeartbeatTimeout(): void {
    this.stopHeartbeatTimeout()
    this.heartbeatTimeoutTimer = setTimeout(() => {
      console.error(
        `[bisque-ws] No server heartbeat received in ${this.heartbeatTimeoutMs}ms. Reconnecting.`,
      )
      this.ws?.close()
    }, this.heartbeatTimeoutMs)
  }

  private stopHeartbeatTimeout(): void {
    if (this.heartbeatTimeoutTimer) {
      clearTimeout(this.heartbeatTimeoutTimer)
      this.heartbeatTimeoutTimer = null
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return
    // Jittered exponential backoff
    const jitter = 0.75 + Math.random() * 0.5 // 0.75..1.25
    const delay = this.reconnectDelay * jitter
    console.info(`[bisque-ws] Reconnecting in ${Math.round(delay)}ms (lastSeenSeq=${this.lastSeenSeq})`)
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay)
      this.connect()
    }, delay)
  }

  /** On disconnect, retry eligible pending requests; reject the rest. */
  private handlePendingOnDisconnect(): void {
    const retryable: PendingRequest[] = []
    for (const [id, pending] of this.pendingRequests) {
      clearTimeout(pending.timer)
      if (pending.retries < this.maxRequestRetries) {
        pending.retries++
        retryable.push(pending)
      } else {
        pending.reject(new Error("Connection lost, max retries exceeded"))
      }
      this.pendingRequests.delete(id)
    }
    // R3: Re-send retryable requests after reconnect, tracking the subscription
    // so it can be cleaned up if disconnect() is called before reconnect.
    if (retryable.length > 0) {
      const unsub = this.onPush((msg) => {
        if (msg.type === "HandshakeComplete") {
          unsub()
          // Remove from tracked unsubs since it fired successfully
          this.retryUnsubs = this.retryUnsubs.filter((u) => u !== unsub)
          for (const pending of retryable) {
            const newId = this.allocRequestId()
            const newMsg = { ...pending.msg, request_id: newId } as ClientMessage & {
              request_id: number
            }
            const timer = setTimeout(() => {
              this.pendingRequests.delete(newId)
              pending.reject(new Error(`Retry request ${newId} timed out`))
            }, this.requestTimeoutMs)
            this.pendingRequests.set(newId, { ...pending, timer, msg: newMsg })
            this.sendClientMsg(newMsg)
          }
        }
      })
      this.retryUnsubs.push(unsub)
    }
  }

  private rejectAllPending(reason: string): void {
    for (const [, pending] of this.pendingRequests) {
      clearTimeout(pending.timer)
      pending.reject(new Error(reason))
    }
    this.pendingRequests.clear()
    for (const [, stream] of this.pendingSqlStreams) {
      clearTimeout(stream.timer)
      stream.reject(new Error(reason))
    }
    this.pendingSqlStreams.clear()
  }
}

/** Convert an Apache Arrow Table to an array of plain JS row objects. */
function arrowTableToRows(table: import("apache-arrow").Table): Record<string, unknown>[] {
  const numRows = table.numRows
  const fields = table.schema.fields
  // Pre-fetch column vectors once — avoids O(n*m) getChild lookups
  const columns = fields.map(f => table.getChild(f.name))
  const names = fields.map(f => f.name)
  const rows: Record<string, unknown>[] = new Array(numRows)
  for (let i = 0; i < numRows; i++) {
    const row: Record<string, unknown> = {}
    for (let c = 0; c < columns.length; c++) {
      const col = columns[c]
      row[names[c]] = col ? col.get(i) : null
    }
    rows[i] = row
  }
  return rows
}

import { isMock } from "./api"
import { mockWsClient } from "./mock-ws"

const realWsClient = new BisqueWsClient()

/** Returns the active WS client (mock or real) based on the active cluster config. */
export function getWsClient(): BisqueWsClient | typeof mockWsClient {
  return isMock() ? mockWsClient : realWsClient
}

/** @deprecated Use getWsClient() for runtime switching. */
export const wsClient = new Proxy(realWsClient, {
  get(_target, prop, receiver) {
    const target = isMock() ? mockWsClient : realWsClient
    return Reflect.get(target, prop, receiver)
  },
}) as BisqueWsClient
