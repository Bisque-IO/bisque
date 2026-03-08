import ky from "ky"
import { useAuthStore } from "@/stores/auth"
import { useClusterStore } from "@/stores/cluster"

// ---------------------------------------------------------------------------
// Mock mode — runtime toggle based on active cluster config
// ---------------------------------------------------------------------------

/** Check if mock mode is active for the current cluster. */
export function isMock(): boolean {
  return useClusterStore.getState().activeCluster.mock ?? false
}

/** @deprecated Use isMock() for runtime checks. Kept for backwards compat. */
export const MOCK = import.meta.env.VITE_MOCK === "true"

// ---------------------------------------------------------------------------
// Real HTTP client — delegates to active cluster via Proxy
// ---------------------------------------------------------------------------

function getApi() {
  const cluster = useClusterStore.getState().activeCluster
  const prefixUrl = cluster.url || "/"
  return ky.create({
    prefixUrl,
    hooks: {
      beforeRequest: [
        (request) => {
          const token = useAuthStore.getState().token
          if (token) {
            request.headers.set("Authorization", `Bearer ${token}`)
          }
        },
      ],
      afterResponse: [
        (_request, _options, response) => {
          if (response.status === 401) {
            useAuthStore.getState().logout()
          }
        },
      ],
    },
  })
}

export const api = new Proxy({} as ReturnType<typeof ky.create>, {
  get(_target, prop, receiver) {
    return Reflect.get(getApi(), prop, receiver)
  },
})

// ---------------------------------------------------------------------------
// Type exports (shared by real and mock implementations)
// ---------------------------------------------------------------------------

export interface Account {
  account_id: u64
  name: string
  created_at: number
}

export interface User {
  user_id: u64
  username: string
  disabled: boolean
  created_at: number
}

export interface AccountMembership {
  user_id: u64
  account_id: u64
  role: "admin" | "member"
}

export interface LoginResponse {
  user_id: number
  token: string
  accounts: Account[]
  default_tenant_id?: number
  default_tenant_name?: string
}

export interface Tenant {
  tenant_id: u64
  account_id: u64
  name: string
  limits: TenantLimits
  api_keys: u64[]
  created_at: number
}

export interface TenantLimits {
  max_disk_bytes: number
  max_deep_storage_bytes: number
  max_concurrent_queries: number
  max_query_memory_bytes: number
  max_catalogs: number
}

export interface SqlColumn {
  name: string
  type: string
  nullable: boolean
}

export interface SqlResult {
  columns: SqlColumn[]
  rows: Record<string, unknown>[]
  row_count: number
}

export interface CatalogEntry {
  catalog_id: u64
  tenant_id: u64
  name: string
  engine: string
  config: string
  raft_group_id: u64
}

export interface ApiKeyEntry {
  key_id: u64
  tenant_id: u64
  scopes: Scope[]
  revoked: boolean
  created_at: number
}

export type Scope =
  | "SuperAdmin"
  | "TenantAdmin"
  | { AccountAdmin: number }
  | { Catalog: string }
  | { CatalogRead: string }

type u64 = number

export interface TempoSearchResponse {
  traces: TempoTraceResult[]
}

export interface TempoTraceResult {
  traceID: string
  rootServiceName: string
  rootTraceName: string
  startTimeUnixNano: string
  durationMs: number
  spanSets?: { spans: unknown[] }[]
}

export interface TempoTraceResponse {
  batches: unknown[]
}

export interface TempoTagScope {
  name: string
  tags: string[]
}

export interface TempoTagValue {
  type: string
  value: string
}

export interface PromResponse {
  status: string
  data: {
    resultType?: string
    result?: PromResult[]
  }
}

export interface PromResult {
  metric: Record<string, string>
  value?: [number, string]
  values?: [number, string][]
}

export interface LokiResponse {
  status: string
  data: {
    resultType: string
    result: LokiStream[]
  }
}

export interface LokiStream {
  stream: Record<string, string>
  values: [string, string][]
}

export interface LokiLabelsResponse {
  status: string
  data: string[]
}

// ---------------------------------------------------------------------------
// Table index types
// ---------------------------------------------------------------------------

export type IndexType =
  | "BTree"
  | "Bitmap"
  | "Inverted"
  | "NGram"
  | "IvfFlat"
  | "IvfSq"
  | "IvfPq"
  | "IvfHnswSq"
  | "IvfHnswPq"
  | "IvfHnswFlat"
  | "IvfRq"
  | "RTree"
  | "Scalar"
  | "LabelList"

export interface TableIndex {
  name: string
  columns: string[]
  index_type: IndexType
  dataset_version: number
  fragment_count: number
  total_fragments: number
}

// ---------------------------------------------------------------------------
// Cluster types
// ---------------------------------------------------------------------------

export type RaftRole = "leader" | "follower" | "candidate" | "learner"

export interface ClusterNode {
  node_id: number
  address: string
  raft_role: RaftRole
  raft_term: number
  raft_applied_index: number
  raft_commit_index: number
  current_leader_id: number | null
  uptime_seconds: number
  catalogs: number
  tables: number
  requests_total: number
  cpu_usage_pct: number
  memory_used_bytes: number
  memory_total_bytes: number
  version: string
  started_at: string
}

export interface ClusterStatus {
  cluster_name: string
  nodes: ClusterNode[]
  total_catalogs: number
  total_tables: number
  total_raft_groups: number
}

// ---------------------------------------------------------------------------
// Real API implementations
// ---------------------------------------------------------------------------

const realAuthApi = {
  login: (username: string, password: string) =>
    api.post("_bisque/v1/auth/login", { json: { username, password } }).json<LoginResponse>(),
}

const realAccountApi = {
  list: () => api.get("_bisque/v1/accounts").json<Account[]>(),
  get: (accountId: number) => api.get(`_bisque/v1/accounts/${accountId}`).json<Account>(),
  create: (name: string) =>
    api.post("_bisque/v1/accounts", { json: { name } }).json<{ account_id: number }>(),
}

const realTenantApi = {
  create: (accountId: number, name: string, limits?: Partial<TenantLimits>) =>
    api.post("_bisque/v1/tenants", { json: { account_id: accountId, name, limits } }).json<{ tenant_id: number }>(),

  get: (tenantId: number) =>
    api.get(`_bisque/v1/tenants/${tenantId}`).json<Tenant>(),
}

const realCatalogApi = {
  list: (tenantId: number) =>
    api.get(`_bisque/v1/tenants/${tenantId}/catalogs`).json<CatalogEntry[]>(),

  create: (tenantId: number, name: string, engine: string, config = "") =>
    api
      .post(`_bisque/v1/tenants/${tenantId}/catalogs`, {
        json: { name, engine, config },
      })
      .json<{ catalog_id: number; raft_group_id: number }>(),
}

const realApiKeyApi = {
  create: (tenantId: number, scopes: Scope[], ttlSecs?: number) =>
    api
      .post(`_bisque/v1/tenants/${tenantId}/api-keys`, {
        json: { scopes, ttl_secs: ttlSecs },
      })
      .json<{ key_id: number; raw_key: string; token: string }>(),

  revoke: async (keyId: number) => { await api.delete(`_bisque/v1/api-keys/${keyId}`) },
}

const realTempoApi = {
  searchTraces: (params: Record<string, string>) =>
    api.get("api/search", { searchParams: params }).json<TempoSearchResponse>(),

  getTrace: (traceId: string) =>
    api.get(`api/traces/${traceId}`).json<TempoTraceResponse>(),

  getTags: () =>
    api.get("api/v2/search/tags").json<{ scopes: TempoTagScope[] }>(),

  getTagValues: (tag: string) =>
    api.get(`api/v2/search/tag/${tag}/values`).json<{ tagValues: TempoTagValue[] }>(),
}

const realPromApi = {
  query: (query: string, time?: string) =>
    api
      .get("api/v1/query", { searchParams: { query, ...(time ? { time } : {}) } })
      .json<PromResponse>(),

  queryRange: (query: string, start: string, end: string, step: string) =>
    api
      .get("api/v1/query_range", { searchParams: { query, start, end, step } })
      .json<PromResponse>(),

  labels: () => api.get("api/v1/labels").json<PromResponse>(),

  labelValues: (name: string) =>
    api.get(`api/v1/label/${name}/values`).json<PromResponse>(),

  metadata: () => api.get("api/v1/metadata").json<PromResponse>(),
}

const realLokiApi = {
  query: (query: string, limit?: number, time?: string) =>
    api
      .get("loki/api/v1/query", {
        searchParams: { query, ...(limit ? { limit: String(limit) } : {}), ...(time ? { time } : {}) },
      })
      .json<LokiResponse>(),

  queryRange: (query: string, start: string, end: string, limit?: number) =>
    api
      .get("loki/api/v1/query_range", {
        searchParams: { query, start, end, ...(limit ? { limit: String(limit) } : {}) },
      })
      .json<LokiResponse>(),

  labels: () => api.get("loki/api/v1/labels").json<LokiLabelsResponse>(),

  labelValues: (name: string) =>
    api.get(`loki/api/v1/label/${name}/values`).json<LokiLabelsResponse>(),
}

const realClusterApi = {
  getStatus: () =>
    api.get("_bisque/v1/cluster/status").json<ClusterStatus>(),
}

export interface SubmitResponse {
  op_id: string
  message: string
}

// ---------------------------------------------------------------------------
// Operation types (for the Operations page)
// ---------------------------------------------------------------------------

export type OpType = "reindex" | "compact" | "flush"
export type OpTier = "hot" | "warm" | "cold"
export type OpStatus = "queued" | "running" | "done" | "failed" | "cancelled"

export interface Operation {
  id: string
  node_id: number
  op_type: OpType
  tier: OpTier
  tenant: string
  catalog: string
  catalog_type: string
  table: string
  status: OpStatus
  progress: number
  created_at: string
  started_at?: string
  finished_at?: string
  error?: string
  fragments_done?: number
  fragments_total?: number
}

const realS3Api = {
  getCatalog: (bucket: string) =>
    api.get(`${bucket}/_bisque/catalog`).json<Record<string, unknown>>(),

  listObjects: (bucket: string, prefix?: string) =>
    api
      .get(bucket, { searchParams: prefix ? { prefix, "list-type": "2" } : { "list-type": "2" } })
      .text(),

  reindexTable: (bucket: string, table: string) =>
    api.post(`${bucket}/_bisque/reindex/${table}`).json<SubmitResponse>(),

  compactTable: (bucket: string, table: string) =>
    api.post(`${bucket}/_bisque/compact/${table}`).json<SubmitResponse>(),
}

const realOperationsApi = {
  list: (params?: { type?: OpType; tier?: OpTier; status?: OpStatus }) =>
    api
      .get("_bisque/v1/operations", { searchParams: params as Record<string, string> ?? {} })
      .json<Operation[]>(),

  get: (opId: string) =>
    api.get(`_bisque/v1/operations/${opId}`).json<Operation>(),

  cancel: (opId: string) =>
    api.delete(`_bisque/v1/operations/${opId}`).json<{ message: string; op_id: string }>(),
}

// ---------------------------------------------------------------------------
// Conditional exports — runtime proxy that delegates to mock or real
// ---------------------------------------------------------------------------

import {
  mockAuthApi,
  mockAccountApi,
  mockTenantApi,
  mockCatalogApi,
  mockApiKeyApi,
  mockTempoApi,
  mockPromApi,
  mockLokiApi,
  mockS3Api,
  mockClusterApi,
  mockOperationsApi,
} from "./mock-api"

function runtimeProxy<T extends object>(real: T, mock: T): T {
  return new Proxy(real, {
    get(_target, prop, receiver) {
      const target = isMock() ? mock : real
      return Reflect.get(target, prop, receiver)
    },
  }) as T
}

export const authApi = runtimeProxy(realAuthApi, mockAuthApi)
export const accountApi = runtimeProxy(realAccountApi, mockAccountApi)
export const tenantApi = runtimeProxy(realTenantApi, mockTenantApi)
export const catalogApi = runtimeProxy(realCatalogApi, mockCatalogApi)
export const apiKeyApi = runtimeProxy(realApiKeyApi, mockApiKeyApi)
export const tempoApi = runtimeProxy(realTempoApi, mockTempoApi)
export const promApi = runtimeProxy(realPromApi, mockPromApi)
export const lokiApi = runtimeProxy(realLokiApi, mockLokiApi)
export const s3Api = runtimeProxy(realS3Api, mockS3Api)
export const clusterApi = runtimeProxy(realClusterApi, mockClusterApi)
export const operationsApi = runtimeProxy(realOperationsApi, mockOperationsApi)
