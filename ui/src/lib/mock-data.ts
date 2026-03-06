import type {
  Account,
  User,
  AccountMembership,
  Tenant,
  CatalogEntry,
  TempoTraceResult,
  PromResult,
  LokiStream,
} from "./api"

// ---------------------------------------------------------------------------
// Accounts
// ---------------------------------------------------------------------------

export const MOCK_ACCOUNTS: Account[] = [
  { id: 1, name: "Acme Corp", created_at: "2025-11-01T00:00:00Z" },
  { id: 2, name: "Globex Inc", created_at: "2025-12-15T00:00:00Z" },
]

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

export const MOCK_USERS: User[] = [
  { id: 1, username: "admin", disabled: false, created_at: "2025-11-01T00:00:00Z" },
  { id: 2, username: "alice", disabled: false, created_at: "2025-12-01T00:00:00Z" },
  { id: 3, username: "bob", disabled: false, created_at: "2026-01-10T00:00:00Z" },
]

// ---------------------------------------------------------------------------
// Account Memberships
// ---------------------------------------------------------------------------

export const MOCK_MEMBERSHIPS: AccountMembership[] = [
  { user_id: 1, account_id: 1, role: "admin" },
  { user_id: 1, account_id: 2, role: "admin" },
  { user_id: 2, account_id: 1, role: "member" },
  { user_id: 3, account_id: 1, role: "member" },
]

// ---------------------------------------------------------------------------
// Tenants
// ---------------------------------------------------------------------------

export const MOCK_TENANT: Tenant = {
  id: 1,
  account_id: 1,
  name: "Acme Corp",
  limits: { max_catalogs: 10, max_api_keys: 50 },
  created_at: "2025-12-01T00:00:00Z",
}

// ---------------------------------------------------------------------------
// Catalogs
// ---------------------------------------------------------------------------

export const MOCK_CATALOGS: CatalogEntry[] = [
  {
    id: 1,
    tenant_id: 1,
    name: "analytics",
    engine: "Lance",
    config: "",
    raft_group_id: 100,
  },
  {
    id: 2,
    tenant_id: 1,
    name: "events",
    engine: "Lance",
    config: "",
    raft_group_id: 101,
  },
  {
    id: 3,
    tenant_id: 1,
    name: "otel",
    engine: "Lance",
    config: "",
    raft_group_id: 102,
  },
]

// ---------------------------------------------------------------------------
// Tables (per catalog)
// ---------------------------------------------------------------------------

export interface StorageTierInfo {
  size_bytes: number
  row_count: number
  segment_count: number
}

export interface IngestionMetrics {
  ingest_rate_rows_sec: number
  ingest_latency_p50_ms: number
  ingest_latency_p99_ms: number
  async_lag_rows: number
  async_lag_ms: number
  compaction_pending: number
  last_compaction_ms: number
  last_ingest_at: string
}

export interface MockTableInfo {
  active_version: number
  sealed_version: number
  schema: { name: string; type: string; nullable: boolean }[]
  storage: {
    hot: StorageTierInfo
    warm: StorageTierInfo
    cold: StorageTierInfo
  }
  metrics: IngestionMetrics
}

function mockStorage(hotMB: number, warmMB: number, coldMB: number, hotRows: number, warmRows: number, coldRows: number): MockTableInfo["storage"] {
  return {
    hot:  { size_bytes: hotMB * 1024 * 1024,  row_count: hotRows,  segment_count: Math.ceil(hotRows / 100_000) },
    warm: { size_bytes: warmMB * 1024 * 1024,  row_count: warmRows, segment_count: Math.ceil(warmRows / 500_000) },
    cold: { size_bytes: coldMB * 1024 * 1024,  row_count: coldRows, segment_count: Math.ceil(coldRows / 1_000_000) },
  }
}

function mockMetrics(rateRows: number, p50: number, p99: number, lagRows: number, lagMs: number): IngestionMetrics {
  return {
    ingest_rate_rows_sec: rateRows,
    ingest_latency_p50_ms: p50,
    ingest_latency_p99_ms: p99,
    async_lag_rows: lagRows,
    async_lag_ms: lagMs,
    compaction_pending: Math.floor(Math.random() * 5),
    last_compaction_ms: 200 + Math.floor(Math.random() * 800),
    last_ingest_at: new Date(Date.now() - Math.floor(Math.random() * 60_000)).toISOString(),
  }
}

export const MOCK_CATALOG_TABLES: Record<string, Record<string, MockTableInfo>> = {
  analytics: {
    page_views: {
      active_version: 42,
      sealed_version: 40,
      schema: [
        { name: "timestamp", type: "Timestamp(Microsecond, UTC)", nullable: false },
        { name: "user_id", type: "Utf8", nullable: false },
        { name: "page_url", type: "Utf8", nullable: false },
        { name: "referrer", type: "Utf8", nullable: true },
        { name: "duration_ms", type: "Int64", nullable: false },
        { name: "country", type: "Utf8", nullable: true },
      ],
      storage: mockStorage(128, 512, 2048, 850_000, 3_200_000, 12_500_000),
      metrics: mockMetrics(1200, 3.2, 18.5, 450, 120),
    },
    conversions: {
      active_version: 18,
      sealed_version: 16,
      schema: [
        { name: "timestamp", type: "Timestamp(Microsecond, UTC)", nullable: false },
        { name: "user_id", type: "Utf8", nullable: false },
        { name: "event_name", type: "Utf8", nullable: false },
        { name: "revenue_cents", type: "Int64", nullable: true },
        { name: "campaign", type: "Utf8", nullable: true },
      ],
      storage: mockStorage(32, 96, 384, 120_000, 450_000, 1_800_000),
      metrics: mockMetrics(180, 2.1, 12.0, 80, 45),
    },
    user_profiles: {
      active_version: 7,
      sealed_version: 5,
      schema: [
        { name: "user_id", type: "Utf8", nullable: false },
        { name: "email", type: "Utf8", nullable: false },
        { name: "plan", type: "Utf8", nullable: false },
        { name: "signup_date", type: "Date32", nullable: false },
        { name: "embedding", type: "FixedSizeList(Float32, 384)", nullable: true },
      ],
      storage: mockStorage(256, 128, 64, 50_000, 25_000, 10_000),
      metrics: mockMetrics(15, 8.5, 42.0, 0, 0),
    },
  },
  events: {
    clicks: {
      active_version: 103,
      sealed_version: 100,
      schema: [
        { name: "timestamp", type: "Timestamp(Microsecond, UTC)", nullable: false },
        { name: "session_id", type: "Utf8", nullable: false },
        { name: "element_id", type: "Utf8", nullable: false },
        { name: "x", type: "Int32", nullable: false },
        { name: "y", type: "Int32", nullable: false },
      ],
      storage: mockStorage(256, 1024, 4096, 2_000_000, 8_000_000, 32_000_000),
      metrics: mockMetrics(5500, 1.8, 9.2, 1200, 220),
    },
    purchases: {
      active_version: 55,
      sealed_version: 53,
      schema: [
        { name: "timestamp", type: "Timestamp(Microsecond, UTC)", nullable: false },
        { name: "order_id", type: "Utf8", nullable: false },
        { name: "user_id", type: "Utf8", nullable: false },
        { name: "total_cents", type: "Int64", nullable: false },
        { name: "items", type: "List(Utf8)", nullable: false },
      ],
      storage: mockStorage(64, 256, 1024, 300_000, 1_200_000, 4_800_000),
      metrics: mockMetrics(350, 4.5, 22.0, 150, 85),
    },
    impressions: {
      active_version: 210,
      sealed_version: 208,
      schema: [
        { name: "timestamp", type: "Timestamp(Microsecond, UTC)", nullable: false },
        { name: "ad_id", type: "Utf8", nullable: false },
        { name: "placement", type: "Utf8", nullable: false },
        { name: "viewport_pct", type: "Float32", nullable: true },
      ],
      storage: mockStorage(512, 2048, 8192, 4_000_000, 16_000_000, 64_000_000),
      metrics: mockMetrics(12000, 1.2, 6.5, 3500, 290),
    },
  },
  otel: {
    otel_spans: {
      active_version: 320,
      sealed_version: 318,
      schema: [
        { name: "trace_id", type: "Utf8", nullable: false },
        { name: "span_id", type: "Utf8", nullable: false },
        { name: "parent_span_id", type: "Utf8", nullable: true },
        { name: "operation_name", type: "Utf8", nullable: false },
        { name: "service_name", type: "Utf8", nullable: false },
        { name: "start_time", type: "Timestamp(Nanosecond, UTC)", nullable: false },
        { name: "duration_ns", type: "Int64", nullable: false },
        { name: "status_code", type: "Int32", nullable: false },
      ],
      storage: mockStorage(384, 1536, 6144, 3_000_000, 12_000_000, 48_000_000),
      metrics: mockMetrics(8500, 2.0, 11.0, 2100, 250),
    },
    otel_logs: {
      active_version: 150,
      sealed_version: 148,
      schema: [
        { name: "timestamp", type: "Timestamp(Nanosecond, UTC)", nullable: false },
        { name: "severity", type: "Utf8", nullable: false },
        { name: "body", type: "Utf8", nullable: false },
        { name: "service_name", type: "Utf8", nullable: false },
        { name: "trace_id", type: "Utf8", nullable: true },
        { name: "attributes", type: "Utf8", nullable: true },
      ],
      storage: mockStorage(192, 768, 3072, 1_500_000, 6_000_000, 24_000_000),
      metrics: mockMetrics(4200, 1.5, 8.0, 800, 190),
    },
    otel_metrics: {
      active_version: 88,
      sealed_version: 86,
      schema: [
        { name: "timestamp", type: "Timestamp(Nanosecond, UTC)", nullable: false },
        { name: "metric_name", type: "Utf8", nullable: false },
        { name: "value", type: "Float64", nullable: false },
        { name: "labels", type: "Utf8", nullable: true },
      ],
      storage: mockStorage(96, 384, 1536, 750_000, 3_000_000, 12_000_000),
      metrics: mockMetrics(2800, 1.0, 5.5, 400, 95),
    },
  },
}

// ---------------------------------------------------------------------------
// Segment files (per catalog/table)
// ---------------------------------------------------------------------------

export function mockSegmentFiles(catalog: string, table: string): string[] {
  const info = MOCK_CATALOG_TABLES[catalog]?.[table]
  if (!info) return []
  const files: string[] = []
  for (let v = Math.max(1, info.sealed_version - 2); v <= info.sealed_version; v++) {
    files.push(`${table}/sealed/${v}/data.lance`)
    files.push(`${table}/sealed/${v}/index.idx`)
  }
  files.push(`${table}/active/${info.active_version}/data.lance`)
  files.push(`${table}/active/${info.active_version}/index.idx`)
  return files
}

// ---------------------------------------------------------------------------
// Traces
// ---------------------------------------------------------------------------

const SERVICES = ["api-gateway", "user-service", "order-service", "payment-service", "inventory-service"]
const OPERATIONS = ["GET /api/users", "POST /api/orders", "GET /api/products", "ProcessPayment", "CheckInventory", "ValidateToken", "GetUserProfile", "CreateOrder"]

function randomHex(len: number): string {
  return Array.from({ length: len }, () => Math.floor(Math.random() * 16).toString(16)).join("")
}

function generateTrace(index: number): {
  summary: TempoTraceResult
  spans: {
    traceId: string
    spanId: string
    parentSpanId?: string
    name: string
    serviceName: string
    startTimeUnixNano: string
    endTimeUnixNano: string
    status: { code: number }
  }[]
} {
  const traceId = randomHex(32)
  const now = Date.now() * 1e6
  const baseTime = now - (index * 60_000_000_000) // spread across last N minutes
  const spanCount = 3 + Math.floor(Math.random() * 6)
  const rootOp = OPERATIONS[index % OPERATIONS.length]
  const rootService = SERVICES[0]

  const spans: {
    traceId: string
    spanId: string
    parentSpanId?: string
    name: string
    serviceName: string
    startTimeUnixNano: string
    endTimeUnixNano: string
    status: { code: number }
  }[] = []

  const rootSpanId = randomHex(16)
  const rootDuration = 50_000_000 + Math.floor(Math.random() * 450_000_000) // 50-500ms

  spans.push({
    traceId,
    spanId: rootSpanId,
    name: rootOp,
    serviceName: rootService,
    startTimeUnixNano: String(baseTime),
    endTimeUnixNano: String(baseTime + rootDuration),
    status: { code: index === 3 ? 2 : 1 }, // one error trace
  })

  let offset = 5_000_000 // 5ms initial offset
  for (let i = 1; i < spanCount; i++) {
    const dur = 10_000_000 + Math.floor(Math.random() * 100_000_000)
    spans.push({
      traceId,
      spanId: randomHex(16),
      parentSpanId: i === 1 ? rootSpanId : spans[Math.floor(Math.random() * i)].spanId,
      name: OPERATIONS[(index + i) % OPERATIONS.length],
      serviceName: SERVICES[i % SERVICES.length],
      startTimeUnixNano: String(baseTime + offset),
      endTimeUnixNano: String(baseTime + offset + dur),
      status: { code: 1 },
    })
    offset += dur + 2_000_000
  }

  return {
    summary: {
      traceID: traceId,
      rootServiceName: rootService,
      rootTraceName: rootOp,
      startTimeUnixNano: String(baseTime),
      durationMs: Math.round(rootDuration / 1e6),
    },
    spans,
  }
}

const GENERATED_TRACES = Array.from({ length: 15 }, (_, i) => generateTrace(i))

export const MOCK_TRACE_SUMMARIES: TempoTraceResult[] = GENERATED_TRACES.map((t) => t.summary)

export function mockTraceSpans(traceId: string) {
  const trace = GENERATED_TRACES.find((t) => t.summary.traceID === traceId)
  if (!trace) return { batches: [] }

  // Group spans by service for OTLP batch format
  const byService = new Map<string, typeof trace.spans>()
  for (const span of trace.spans) {
    const existing = byService.get(span.serviceName) ?? []
    existing.push(span)
    byService.set(span.serviceName, existing)
  }

  const batches = Array.from(byService.entries()).map(([service, spans]) => ({
    resource: {
      attributes: [{ key: "service.name", value: { stringValue: service } }],
    },
    scopeSpans: [{ spans }],
  }))

  return { batches }
}

// ---------------------------------------------------------------------------
// Metrics (time-series)
// ---------------------------------------------------------------------------

function generateTimeSeries(name: string, baseValue: number, variance: number): PromResult {
  const now = Math.floor(Date.now() / 1000)
  const step = 60 // 1 minute
  const points = 60 // 1 hour of data
  const values: [number, string][] = []

  for (let i = 0; i < points; i++) {
    const ts = now - (points - i) * step
    const val = baseValue + (Math.random() - 0.5) * variance * 2
    values.push([ts, val.toFixed(4)])
  }

  return {
    metric: { __name__: name, instance: "bisque:3200", job: "bisque" },
    values,
  }
}

export const MOCK_METRICS: PromResult[] = [
  generateTimeSeries("http_requests_total", 150, 50),
  generateTimeSeries("process_cpu_seconds_total", 0.35, 0.15),
  generateTimeSeries("process_resident_memory_bytes", 256_000_000, 32_000_000),
]

export const MOCK_METRIC_LABELS = [
  "__name__",
  "instance",
  "job",
  "method",
  "path",
  "status_code",
  "service_name",
]

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

const LOG_LEVELS = ["info", "info", "info", "warn", "error", "debug"]
const LOG_MESSAGES = [
  "Request completed successfully",
  "Connected to database",
  "Starting background compaction",
  "Segment sealed version=42",
  "Active version bumped to 43",
  "WebSocket client connected session=abc123",
  "Raft log replicated index=1024",
  "Query executed in 12ms rows=1500",
  "Token verified tenant_id=1 key_id=5",
  "OTLP batch received spans=25",
  "Compaction completed table=page_views duration=350ms",
  "Version pin registered session=def456 table=clicks version=103",
  "Failed to connect to peer node_id=2",
  "Retrying connection attempt=3 backoff=2s",
  "Memory usage above threshold current=85%",
  "Index rebuild started table=user_profiles",
  "Flight SQL query parsed statement=SELECT",
  "S3 GetObject request key=clicks/sealed/100/data.lance",
  "Prometheus remote write received samples=150",
  "Loki push received streams=3 entries=47",
]

export function generateMockLogs(count: number): LokiStream[] {
  const now = Date.now() * 1e6
  const values: [string, string][] = []

  for (let i = 0; i < count; i++) {
    const ts = now - i * 500_000_000 // 500ms apart
    const level = LOG_LEVELS[Math.floor(Math.random() * LOG_LEVELS.length)]
    const msg = LOG_MESSAGES[Math.floor(Math.random() * LOG_MESSAGES.length)]
    values.push([String(ts), `level=${level} msg="${msg}"`])
  }

  return [
    {
      stream: { job: "bisque", instance: "bisque:3200", level: "info" },
      values: values.filter((v) => v[1].includes("level=info")),
    },
    {
      stream: { job: "bisque", instance: "bisque:3200", level: "warn" },
      values: values.filter((v) => v[1].includes("level=warn")),
    },
    {
      stream: { job: "bisque", instance: "bisque:3200", level: "error" },
      values: values.filter((v) => v[1].includes("level=error")),
    },
    {
      stream: { job: "bisque", instance: "bisque:3200", level: "debug" },
      values: values.filter((v) => v[1].includes("level=debug")),
    },
  ].filter((s) => s.values.length > 0)
}

// ---------------------------------------------------------------------------
// API Keys
// ---------------------------------------------------------------------------

export interface MockApiKey {
  id: number
  tenant_id: number
  scopes: ("SuperAdmin" | "TenantAdmin" | { AccountAdmin: number } | { Catalog: string } | { CatalogRead: string })[]
  revoked: boolean
  created_at: string
}

export const MOCK_API_KEYS: MockApiKey[] = [
  { id: 1, tenant_id: 1, scopes: ["TenantAdmin"], revoked: false, created_at: "2025-12-01T00:00:00Z" },
  { id: 2, tenant_id: 1, scopes: ["TenantAdmin"], revoked: false, created_at: "2025-12-05T10:30:00Z" },
  { id: 3, tenant_id: 1, scopes: [{ Catalog: "analytics" }], revoked: false, created_at: "2026-01-10T14:00:00Z" },
  { id: 4, tenant_id: 1, scopes: [{ CatalogRead: "events" }], revoked: false, created_at: "2026-02-01T09:00:00Z" },
  { id: 5, tenant_id: 1, scopes: [{ Catalog: "otel" }], revoked: true, created_at: "2026-01-20T16:45:00Z" },
]
