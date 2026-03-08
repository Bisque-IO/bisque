import type { ServerMessage, CatalogEventKind } from "./ws-protocol"
import { WS_PROTOCOL_VERSION } from "./ws-protocol"
import { MOCK_CATALOG_TABLES, MOCK_TENANT, MOCK_API_KEYS, MOCK_CLUSTER_STATUS, MOCK_ACCOUNTS } from "./mock-data"
import type { Operation, Tenant, ApiKeyEntry, TenantLimits, Account, SqlResult, ClusterStatus } from "./api"

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

  async getCatalog(bucket: string): Promise<Record<string, unknown>> {
    await delay()
    const tables = MOCK_CATALOG_TABLES[bucket] ?? {}
    return { tables }
  }

  async getClusterStatus(): Promise<ClusterStatus> {
    await delay()
    return MOCK_CLUSTER_STATUS
  }

  async listTenants(_accountId: number): Promise<Tenant[]> {
    await delay()
    return [MOCK_TENANT as Tenant]
  }

  async updateTenantLimits(_tenantId: number, _limits: TenantLimits): Promise<void> {
    await delay()
  }

  async deleteTenant(_tenantId: number): Promise<void> {
    await delay()
  }

  async deleteCatalog(_tenantId: number, _catalogId: number): Promise<void> {
    await delay()
  }

  async listApiKeys(_tenantId: number): Promise<ApiKeyEntry[]> {
    await delay()
    return MOCK_API_KEYS.map((k) => ({
      key_id: k.key_id,
      tenant_id: k.tenant_id,
      scopes: k.scopes,
      revoked: k.revoked,
      created_at: Date.parse(k.created_at),
    }))
  }

  async revokeApiKey(_keyId: number): Promise<void> {
    await delay()
  }

  async createTable(_catalog: string, table: string, _schemaJson: string): Promise<{ table: string }> {
    await delay()
    return { table }
  }

  async dropTable(_catalog: string, table: string): Promise<{ table: string }> {
    await delay()
    return { table }
  }

  async executeSql(_catalog: string, sql: string): Promise<SqlResult> {
    await delay(200)
    // Parse table name from SQL for mock data generation
    const match = sql.match(/FROM\s+(\w+)/i)
    const tableName = match?.[1] ?? "unknown"
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i)
    const limit = limitMatch ? parseInt(limitMatch[1]) : 10

    const columns = [
      { name: "id", type: "Int64", nullable: false },
      { name: "name", type: "Utf8", nullable: true },
      { name: "value", type: "Float64", nullable: true },
      { name: "created_at", type: "Timestamp", nullable: false },
    ]
    const rows = Array.from({ length: Math.min(limit, 25) }, (_, i) => ({
      id: i + 1,
      name: `${tableName}_row_${i + 1}`,
      value: Math.round(Math.random() * 10000) / 100,
      created_at: new Date(Date.now() - i * 86400000).toISOString(),
    }))
    return { columns, rows, row_count: rows.length }
  }

  async enableOtel(_catalog: string): Promise<{ tables_created: string[] }> {
    await delay()
    return {
      tables_created: [
        "otel_counters", "otel_gauges", "otel_histograms", "otel_exp_histograms",
        "otel_summaries", "otel_exemplars", "otel_spans", "otel_span_events",
        "otel_span_links", "otel_logs",
      ],
    }
  }

  async listAccounts(): Promise<Account[]> {
    await delay()
    return MOCK_ACCOUNTS
  }
}

function delay(ms = 80 + Math.random() * 120): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

export const mockWsClient = new MockBisqueWsClient()
