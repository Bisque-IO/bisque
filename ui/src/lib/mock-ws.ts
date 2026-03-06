import type { ServerWsMessage, CatalogEvent } from "./ws"
import { MOCK_CATALOG_TABLES } from "./mock-data"

type WsEventHandler = (message: ServerWsMessage) => void

const CATALOGS = Object.keys(MOCK_CATALOG_TABLES)

function randomEvent(): CatalogEvent {
  const catalog = CATALOGS[Math.floor(Math.random() * CATALOGS.length)]
  const tables = Object.keys(MOCK_CATALOG_TABLES[catalog])
  const table = tables[Math.floor(Math.random() * tables.length)]
  const info = MOCK_CATALOG_TABLES[catalog][table]

  const eventTypes: CatalogEvent["type"][] = [
    "ActiveVersionBumped",
    "SegmentSealed",
    "TableCreated",
  ]
  const type = eventTypes[Math.floor(Math.random() * eventTypes.length)]

  switch (type) {
    case "ActiveVersionBumped":
      return { type, catalog, table, version: info.active_version + 1 }
    case "SegmentSealed":
      return {
        type,
        catalog,
        table,
        active_version: info.active_version,
        sealed_version: info.sealed_version + 1,
      }
    case "TableCreated":
      return { type, catalog, table: `new_table_${Date.now()}`, schema_ipc: [] }
    default:
      return { type: "ActiveVersionBumped", catalog, table, version: info.active_version + 1 }
  }
}

export class MockBisqueWsClient {
  private handlers: Set<WsEventHandler> = new Set()
  private interval: ReturnType<typeof setInterval> | null = null
  private _connected = false

  get connected() {
    return this._connected
  }

  connect() {
    this._connected = true

    // Emit a Session message immediately
    setTimeout(() => {
      for (const handler of this.handlers) {
        handler({ type: "Session", session_id: 1 })
      }
    }, 100)

    // Emit random events every 3-5 seconds
    this.interval = setInterval(() => {
      const event = randomEvent()
      for (const handler of this.handlers) {
        handler({ type: "Event", event })
      }
    }, 3000 + Math.random() * 2000)
  }

  disconnect() {
    if (this.interval) {
      clearInterval(this.interval)
      this.interval = null
    }
    this._connected = false
  }

  subscribe(handler: WsEventHandler): () => void {
    this.handlers.add(handler)
    return () => this.handlers.delete(handler)
  }
}

export const mockWsClient = new MockBisqueWsClient()
