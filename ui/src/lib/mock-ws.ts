import type { ServerMessage, CatalogEventKind } from "./ws-protocol"
import { WS_PROTOCOL_VERSION } from "./ws-protocol"
import { MOCK_CATALOG_TABLES } from "./mock-data"
import type { Operation } from "./api"

type PushHandler = (message: ServerMessage) => void

const CATALOGS = Object.keys(MOCK_CATALOG_TABLES)

function randomEvent(): { event: CatalogEventKind; catalog: string } {
  const catalog = CATALOGS[Math.floor(Math.random() * CATALOGS.length)]
  const tables = Object.keys(MOCK_CATALOG_TABLES[catalog])
  const table = tables[Math.floor(Math.random() * tables.length)]
  const info = MOCK_CATALOG_TABLES[catalog][table]

  const eventTypes = [
    "active_version_bumped",
    "segment_sealed",
    "table_created",
  ] as const
  const type = eventTypes[Math.floor(Math.random() * eventTypes.length)]

  switch (type) {
    case "active_version_bumped":
      return { event: { type, table, version: info.active_version + 1 }, catalog }
    case "segment_sealed":
      return {
        event: {
          type,
          table,
          active_version: info.active_version,
          sealed_version: info.sealed_version + 1,
        },
        catalog,
      }
    case "table_created":
      return { event: { type, table: `new_table_${Date.now()}`, schema_ipc: [] }, catalog }
    default:
      return { event: { type: "active_version_bumped", table, version: info.active_version + 1 }, catalog }
  }
}

export class MockBisqueWsClient {
  private handlers: Set<PushHandler> = new Set()
  private interval: ReturnType<typeof setInterval> | null = null
  private _connected = false
  private _handshakeComplete = false
  private seq = 0

  get connected() {
    return this._connected && this._handshakeComplete
  }

  connect() {
    this._connected = true
    this._handshakeComplete = true

    // Emit Handshake + initial OperationsSnapshot
    setTimeout(() => {
      for (const handler of this.handlers) {
        handler({
          type: "Handshake",
          protocol_version: WS_PROTOCOL_VERSION,
          session_id: 1,
          catalog_seq: 0,
          server_seq: 0,
        })
      }
      this.seq++
      for (const handler of this.handlers) {
        handler({
          type: "OperationsSnapshot",
          seq: this.seq,
          operations: [],
        })
      }
    }, 100)

    // Emit random catalog events
    this.interval = setInterval(() => {
      this.seq++
      const { event, catalog } = randomEvent()
      for (const handler of this.handlers) {
        handler({
          type: "CatalogEvent",
          seq: this.seq,
          event,
          catalog,
        } as ServerMessage)
      }
    }, 3000 + Math.random() * 2000)
  }

  disconnect() {
    if (this.interval) {
      clearInterval(this.interval)
      this.interval = null
    }
    this._connected = false
    this._handshakeComplete = false
  }

  subscribe(handler: PushHandler): () => void {
    this.handlers.add(handler)
    return () => this.handlers.delete(handler)
  }

  onPush(handler: PushHandler): () => void {
    return this.subscribe(handler)
  }

  setSubscribeCatalogs(_catalogs: string[]): void {
    // no-op
  }

  // Mock request/response methods
  async listOperations(): Promise<Operation[]> {
    await delay()
    return []
  }

  async getOperation(_opId: string): Promise<Operation> {
    await delay()
    throw new Error("Not found")
  }

  async cancelOperation(opId: string): Promise<{ op_id: string; message: string }> {
    await delay()
    return { op_id: opId, message: "cancelled" }
  }

  async submitReindex(_bucket: string, table: string): Promise<{ op_id: string; message: string }> {
    await delay()
    return { op_id: `mock-${Date.now()}`, message: `Reindex queued for '${table}'` }
  }

  async submitCompact(_bucket: string, table: string): Promise<{ op_id: string; message: string }> {
    await delay()
    return { op_id: `mock-${Date.now()}`, message: `Compact queued for '${table}'` }
  }

  async getCatalog(_bucket: string): Promise<Record<string, unknown>> {
    await delay()
    return {}
  }

  async getClusterStatus(): Promise<unknown> {
    await delay()
    return { cluster_name: "mock", nodes: [], total_catalogs: 0, total_tables: 0, total_raft_groups: 0 }
  }
}

function delay(ms = 80 + Math.random() * 120): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

export const mockWsClient = new MockBisqueWsClient()
