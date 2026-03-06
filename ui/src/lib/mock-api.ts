import type {
  Account,
  LoginResponse,
  Tenant,
  TenantLimits,
  Scope,
  TempoSearchResponse,
  TempoTraceResponse,
  TempoTagScope,
  TempoTagValue,
  PromResponse,
  LokiResponse,
  LokiLabelsResponse,
} from "./api"
import {
  MOCK_ACCOUNTS,
  MOCK_USERS,
  MOCK_MEMBERSHIPS,
  MOCK_TENANT,
  MOCK_CATALOGS,
  MOCK_CATALOG_TABLES,
  MOCK_TRACE_SUMMARIES,
  MOCK_METRICS,
  MOCK_METRIC_LABELS,
  MOCK_API_KEYS,
  mockTraceSpans,
  mockSegmentFiles,
  generateMockLogs,
  type MockApiKey,
} from "./mock-data"

// Simulated async delay
function delay<T>(value: T, ms = 80 + Math.random() * 120): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms))
}

// In-memory mutable state so mutations feel real
let nextAccountId = MOCK_ACCOUNTS.length + 1
const accounts = new Map<number, Account>(MOCK_ACCOUNTS.map((a) => [a.id, { ...a }]))
let nextTenantId = 2
const tenants = new Map<number, Tenant>([[1, { ...MOCK_TENANT }]])
let nextCatalogId = MOCK_CATALOGS.length + 1
const catalogs = [...MOCK_CATALOGS]
let nextKeyId = MOCK_API_KEYS.length + 1
const apiKeys = [...MOCK_API_KEYS]

// ---------------------------------------------------------------------------
// Auth API
// ---------------------------------------------------------------------------

export const mockAuthApi = {
  login: (username: string, _password: string) =>
    delay<LoginResponse>((() => {
      const user = MOCK_USERS.find((u) => u.username === username)
      if (!user) throw new Error("invalid credentials")
      const userAccounts = MOCK_MEMBERSHIPS
        .filter((m) => m.user_id === user.id)
        .map((m) => accounts.get(m.account_id)!)
        .filter(Boolean)
      return {
        user_id: user.id,
        token: `eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.mock.user${user.id}`,
        accounts: userAccounts,
      }
    })()),
}

// ---------------------------------------------------------------------------
// Account API
// ---------------------------------------------------------------------------

export const mockAccountApi = {
  list: () => delay<Account[]>([...accounts.values()]),

  get: (accountId: number) =>
    delay((() => {
      const a = accounts.get(accountId)
      if (!a) throw new Error("not found")
      return a
    })()),

  create: (name: string) =>
    delay((() => {
      const id = nextAccountId++
      accounts.set(id, { id, name, created_at: new Date().toISOString() })
      return { account_id: id }
    })()),
}

// ---------------------------------------------------------------------------
// Tenant API
// ---------------------------------------------------------------------------

export const mockTenantApi = {
  create: (accountId: number, name: string, limits?: Partial<TenantLimits>) =>
    delay((() => {
      const id = nextTenantId++
      tenants.set(id, {
        id,
        account_id: accountId,
        name,
        limits: { max_catalogs: limits?.max_catalogs ?? 10, max_api_keys: limits?.max_api_keys ?? 50 },
        created_at: new Date().toISOString(),
      })
      return { tenant_id: id }
    })()),

  get: (tenantId: number) =>
    delay((() => {
      const t = tenants.get(tenantId)
      if (!t) throw new Error("not found")
      return t
    })()),
}

// ---------------------------------------------------------------------------
// Catalog API
// ---------------------------------------------------------------------------

export const mockCatalogApi = {
  list: (tenantId: number) =>
    delay(catalogs.filter((c) => c.tenant_id === tenantId)),

  create: (tenantId: number, name: string, engine: string, _config = "") =>
    delay((() => {
      const id = nextCatalogId++
      const raft_group_id = 100 + id
      catalogs.push({ id, tenant_id: tenantId, name, engine, config: _config, raft_group_id })
      // Seed empty tables for new catalog
      MOCK_CATALOG_TABLES[name] = {}
      return { catalog_id: id, raft_group_id }
    })()),
}

// ---------------------------------------------------------------------------
// API Key API
// ---------------------------------------------------------------------------

export const mockApiKeyApi = {
  create: (tenantId: number, scopes: Scope[], _ttlSecs?: number) =>
    delay((() => {
      const id = nextKeyId++
      const key: MockApiKey = {
        id,
        tenant_id: tenantId,
        scopes,
        revoked: false,
        created_at: new Date().toISOString(),
      }
      apiKeys.push(key)
      return {
        key_id: id,
        raw_key: `bsk_${Array.from({ length: 32 }, () => Math.random().toString(36)[2]).join("")}`,
        token: `eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.mock.${id}`,
      }
    })()),

  revoke: (keyId: number) =>
    delay((() => {
      const key = apiKeys.find((k) => k.id === keyId)
      if (key) key.revoked = true
    })()),
}

// ---------------------------------------------------------------------------
// Tempo API
// ---------------------------------------------------------------------------

export const mockTempoApi = {
  searchTraces: (_params: Record<string, string>) =>
    delay<TempoSearchResponse>({ traces: MOCK_TRACE_SUMMARIES }),

  getTrace: (traceId: string) =>
    delay<TempoTraceResponse>(mockTraceSpans(traceId)),

  getTags: () =>
    delay<{ scopes: TempoTagScope[] }>({
      scopes: [
        { name: "resource", tags: ["service.name", "host.name", "deployment.environment"] },
        { name: "span", tags: ["http.method", "http.status_code", "http.url", "db.system", "rpc.method"] },
      ],
    }),

  getTagValues: (tag: string) =>
    delay<{ tagValues: TempoTagValue[] }>((() => {
      const values: Record<string, string[]> = {
        "service.name": ["api-gateway", "user-service", "order-service", "payment-service"],
        "http.method": ["GET", "POST", "PUT", "DELETE"],
        "http.status_code": ["200", "201", "400", "404", "500"],
      }
      return {
        tagValues: (values[tag] ?? ["value1", "value2"]).map((v) => ({ type: "string", value: v })),
      }
    })()),
}

// ---------------------------------------------------------------------------
// Prometheus API
// ---------------------------------------------------------------------------

export const mockPromApi = {
  query: (query: string, _time?: string) =>
    delay<PromResponse>({
      status: "success",
      data: {
        resultType: "vector",
        result: MOCK_METRICS.filter((m) =>
          query.includes(m.metric.__name__)
        ).map((m) => ({
          metric: m.metric,
          value: m.values ? m.values[m.values.length - 1] : [0, "0"],
        })),
      },
    }),

  queryRange: (_query: string, start: string, end: string, _step: string) =>
    delay<PromResponse>({
      status: "success",
      data: {
        resultType: "matrix",
        result: MOCK_METRICS.map((m) => ({
          metric: m.metric,
          values: (m.values ?? []).filter(
            ([ts]) => ts >= Number(start) && ts <= Number(end)
          ),
        })),
      },
    }),

  labels: () =>
    delay<PromResponse>({
      status: "success",
      data: { result: MOCK_METRIC_LABELS.map((l) => ({ metric: { __name__: l } })) },
    }),

  labelValues: (name: string) =>
    delay<PromResponse>({
      status: "success",
      data: {
        result: name === "__name__"
          ? MOCK_METRICS.map((m) => ({ metric: { __name__: m.metric.__name__ } }))
          : [{ metric: { [name]: "bisque:3200" } }],
      },
    }),

  metadata: () =>
    delay<PromResponse>({ status: "success", data: {} }),
}

// ---------------------------------------------------------------------------
// Loki API
// ---------------------------------------------------------------------------

export const mockLokiApi = {
  query: (_query: string, limit?: number, _time?: string) =>
    delay<LokiResponse>({
      status: "success",
      data: { resultType: "streams", result: generateMockLogs(limit ?? 50) },
    }),

  queryRange: (_query: string, _start: string, _end: string, limit?: number) =>
    delay<LokiResponse>({
      status: "success",
      data: { resultType: "streams", result: generateMockLogs(limit ?? 50) },
    }),

  labels: () =>
    delay<LokiLabelsResponse>({
      status: "success",
      data: ["job", "instance", "level", "service_name"],
    }),

  labelValues: (name: string) =>
    delay<LokiLabelsResponse>({
      status: "success",
      data: name === "level"
        ? ["info", "warn", "error", "debug"]
        : name === "job"
          ? ["bisque"]
          : ["bisque:3200"],
    }),
}

// ---------------------------------------------------------------------------
// S3 / Catalog API
// ---------------------------------------------------------------------------

export const mockS3Api = {
  getCatalog: (bucket: string) =>
    delay<Record<string, unknown>>({
      tables: MOCK_CATALOG_TABLES[bucket] ?? {},
    }),

  listObjects: (bucket: string, prefix?: string) =>
    delay((() => {
      // Find table name from prefix
      const table = prefix?.replace(/\/$/, "") ?? ""
      const files = table
        ? mockSegmentFiles(bucket, table)
        : Object.keys(MOCK_CATALOG_TABLES[bucket] ?? {}).flatMap((t) =>
            mockSegmentFiles(bucket, t)
          )

      // Return minimal S3 ListObjectsV2 XML
      const keys = files.map((f) => `<Contents><Key>${f}</Key><Size>1048576</Size></Contents>`).join("\n")
      return `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>${bucket}</Name>
  <KeyCount>${files.length}</KeyCount>
  ${keys}
</ListBucketResult>`
    })()),
}
