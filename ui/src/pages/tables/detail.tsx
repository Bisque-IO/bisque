import { useEffect, useState } from "react"
import { useParams } from "react-router"
import { s3Api, type TableIndex } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { RefreshCw } from "lucide-react"
import { toast } from "sonner"
import { HeaderActions } from "@/components/layout/header-actions"
import type { StorageTierInfo, IngestionMetrics } from "@/lib/mock-data"

interface TableInfo {
  active_version: number
  sealed_version: number
  schema?: SchemaField[]
  storage?: {
    hot: StorageTierInfo
    warm: StorageTierInfo
    cold: StorageTierInfo
  }
  metrics?: IngestionMetrics
  indexes?: TableIndex[]
}

interface SchemaField {
  name: string
  type: string
  nullable: boolean
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B"
  const units = ["B", "KB", "MB", "GB", "TB"]
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  const val = bytes / Math.pow(1024, i)
  return `${val < 10 ? val.toFixed(1) : Math.round(val)} ${units[i]}`
}

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

function formatRate(n: number): string {
  return `${formatNumber(n)}/s`
}

function indexBadgeVariant(indexType: string): "default" | "secondary" | "outline" {
  if (indexType.startsWith("Ivf") || indexType === "Vector") return "default"
  if (indexType === "Inverted" || indexType === "NGram") return "secondary"
  return "outline"
}

export function TableDetailPage() {
  const { catalogName, tableName } = useParams()
  const [tableInfo, setTableInfo] = useState<TableInfo | null>(null)
  const [files, setFiles] = useState<string[]>([])
  const [reindexing, setReindexing] = useState(false)

  const handleReindex = async () => {
    if (!catalogName || !tableName) return
    setReindexing(true)
    try {
      const result = await s3Api.reindexTable(catalogName, tableName)
      toast.success(result.message)
    } catch {
      toast.error("Reindex failed")
    } finally {
      setReindexing(false)
    }
  }

  useEffect(() => {
    if (!catalogName || !tableName) return

    s3Api
      .getCatalog(catalogName)
      .then((data) => {
        const tables = (data as { tables?: Record<string, TableInfo> }).tables ?? {}
        setTableInfo(tables[tableName] ?? null)
      })
      .catch(() => {})

    s3Api
      .listObjects(catalogName, `${tableName}/`)
      .then((xml) => {
        const parser = new DOMParser()
        const doc = parser.parseFromString(xml, "text/xml")
        const keys = Array.from(doc.querySelectorAll("Key")).map(
          (el) => el.textContent ?? "",
        )
        setFiles(keys)
      })
      .catch(() => {})
  }, [catalogName, tableName])

  const storage = tableInfo?.storage
  const metrics = tableInfo?.metrics

  const totalSize = storage
    ? storage.hot.size_bytes + storage.warm.size_bytes + storage.cold.size_bytes
    : 0
  const totalRows = storage
    ? storage.hot.row_count + storage.warm.row_count + storage.cold.row_count
    : 0
  const totalSegments = storage
    ? storage.hot.segment_count + storage.warm.segment_count + storage.cold.segment_count
    : 0

  return (
    <div className="space-y-6">
      <HeaderActions>
        <Button
          size="sm"
          variant="secondary"
          disabled={reindexing}
          onClick={handleReindex}
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${reindexing ? "animate-spin" : ""}`} />
          {reindexing ? "Reindexing..." : "Reindex Cold Storage"}
        </Button>
      </HeaderActions>

      <Badge variant="outline">{catalogName}</Badge>

      {/* Summary row */}
      {tableInfo && (
        <div className="grid gap-4 md:grid-cols-4">
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Total Size</p>
              <p className="text-2xl font-bold">{formatBytes(totalSize)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Total Rows</p>
              <p className="text-2xl font-bold">{formatNumber(totalRows)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Active Version</p>
              <p className="text-2xl font-bold">{tableInfo.active_version}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Sealed Version</p>
              <p className="text-2xl font-bold">{tableInfo.sealed_version}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Storage tiers */}
      {storage && (
        <Card>
          <CardHeader>
            <CardTitle>Storage Tiers</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Tier</TableHead>
                  <TableHead className="text-right">Size</TableHead>
                  <TableHead className="text-right">Rows</TableHead>
                  <TableHead className="text-right">Segments</TableHead>
                  <TableHead className="text-right">% of Total</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(
                  [
                    ["Hot", storage.hot, "text-orange-500"],
                    ["Warm", storage.warm, "text-yellow-500"],
                    ["Cold", storage.cold, "text-blue-500"],
                  ] as [string, StorageTierInfo, string][]
                ).map(([name, tier, color]) => (
                  <TableRow key={name}>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <span className={`h-2 w-2 rounded-full ${color.replace("text-", "bg-")}`} />
                        <span className="font-medium">{name}</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {formatBytes(tier.size_bytes)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {formatNumber(tier.row_count)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {tier.segment_count}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {totalSize > 0 ? `${((tier.size_bytes / totalSize) * 100).toFixed(1)}%` : "0%"}
                    </TableCell>
                  </TableRow>
                ))}
                <TableRow className="font-semibold">
                  <TableCell>Total</TableCell>
                  <TableCell className="text-right font-mono text-sm">
                    {formatBytes(totalSize)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-sm">
                    {formatNumber(totalRows)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-sm">
                    {totalSegments}
                  </TableCell>
                  <TableCell className="text-right font-mono text-sm">100%</TableCell>
                </TableRow>
              </TableBody>
            </Table>

            {/* Visual bar */}
            {totalSize > 0 && (
              <div className="mt-4 flex h-3 w-full overflow-hidden rounded-full">
                <div
                  className="bg-orange-500"
                  style={{ width: `${(storage.hot.size_bytes / totalSize) * 100}%` }}
                />
                <div
                  className="bg-yellow-500"
                  style={{ width: `${(storage.warm.size_bytes / totalSize) * 100}%` }}
                />
                <div
                  className="bg-blue-500"
                  style={{ width: `${(storage.cold.size_bytes / totalSize) * 100}%` }}
                />
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Ingestion Metrics */}
      {metrics && (
        <Card>
          <CardHeader>
            <CardTitle>Ingestion Metrics</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
              <div>
                <p className="text-sm text-muted-foreground">Ingest Rate</p>
                <p className="text-xl font-bold font-mono">
                  {formatRate(metrics.ingest_rate_rows_sec)}
                </p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Latency (p50 / p99)</p>
                <p className="text-xl font-bold font-mono">
                  {metrics.ingest_latency_p50_ms}ms{" "}
                  <span className="text-muted-foreground">/</span>{" "}
                  {metrics.ingest_latency_p99_ms}ms
                </p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Async Lag</p>
                <p className="text-xl font-bold font-mono">
                  {formatNumber(metrics.async_lag_rows)} rows
                  <span className="text-sm text-muted-foreground ml-1">
                    ({metrics.async_lag_ms}ms)
                  </span>
                </p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Compaction</p>
                <p className="text-xl font-bold font-mono">
                  {metrics.compaction_pending} pending
                  <span className="text-sm text-muted-foreground ml-1">
                    (last: {metrics.last_compaction_ms}ms)
                  </span>
                </p>
              </div>
            </div>
            <div className="mt-4 text-xs text-muted-foreground">
              Last ingest: {new Date(metrics.last_ingest_at).toLocaleString()}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Schema */}
      {tableInfo?.schema && tableInfo.schema.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Schema ({tableInfo.schema.length} columns)</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Column</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Nullable</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tableInfo.schema.map((field) => (
                  <TableRow key={field.name}>
                    <TableCell className="font-mono text-sm">{field.name}</TableCell>
                    <TableCell className="font-mono text-sm">{field.type}</TableCell>
                    <TableCell>
                      <Badge variant={field.nullable ? "secondary" : "outline"}>
                        {field.nullable ? "yes" : "no"}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* Indexes */}
      {tableInfo?.indexes && tableInfo.indexes.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Indexes ({tableInfo.indexes.length})</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Columns</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead className="text-right">Coverage</TableHead>
                  <TableHead className="text-right">Dataset Version</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tableInfo.indexes.map((idx) => (
                  <TableRow key={idx.name}>
                    <TableCell className="font-mono text-sm">{idx.name}</TableCell>
                    <TableCell className="font-mono text-sm">
                      {idx.columns.join(", ")}
                    </TableCell>
                    <TableCell>
                      <Badge variant={indexBadgeVariant(idx.index_type)}>
                        {idx.index_type}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {idx.total_fragments > 0
                        ? `${idx.fragment_count}/${idx.total_fragments} (${Math.round((idx.fragment_count / idx.total_fragments) * 100)}%)`
                        : "—"}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      v{idx.dataset_version}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <p className="mt-3 text-xs text-muted-foreground">
              Indexes are built on sealed (warm) segments and carried over to cold storage.
            </p>
          </CardContent>
        </Card>
      )}

      {/* Segment Files */}
      <Card>
        <CardHeader>
          <CardTitle>Segment Files ({files.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {files.length === 0 ? (
            <p className="text-sm text-muted-foreground">No segment files found.</p>
          ) : (
            <div className="space-y-1 max-h-80 overflow-auto">
              {files.map((f) => (
                <p key={f} className="font-mono text-xs text-muted-foreground">
                  {f}
                </p>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
