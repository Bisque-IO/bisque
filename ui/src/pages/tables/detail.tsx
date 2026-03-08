import { useEffect, useState } from "react"
import { useParams } from "react-router"
import { s3Api, isMock, type TableIndex } from "@/lib/api"
import { wsClient } from "@/lib/ws"
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

interface TableInfo {
  active_version: number | null
  sealed_version: number | null
  s3_version: number
  active_segment: number
  sealed_segment: number | null
  s3_dataset_uri: string
  active_rows: number
  sealed_rows: number
  s3_rows: number
  num_columns: number
  schema?: SchemaField[]
  indexes?: TableIndex[]
}

interface SchemaField {
  name: string
  type: string
  nullable: boolean
}

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return String(n)
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
  const [compacting, setCompacting] = useState(false)

  const handleReindex = async () => {
    if (!catalogName || !tableName) return
    setReindexing(true)
    try {
      const result = isMock()
        ? await s3Api.reindexTable(catalogName, tableName)
        : await wsClient.submitReindex(catalogName, tableName)
      toast.success(result.message)
    } catch {
      toast.error("Reindex failed")
    } finally {
      setReindexing(false)
    }
  }

  const handleCompact = async () => {
    if (!catalogName || !tableName) return
    setCompacting(true)
    try {
      const result = isMock()
        ? await s3Api.compactTable(catalogName, tableName)
        : await wsClient.submitCompact(catalogName, tableName)
      toast.success(result.message)
    } catch {
      toast.error("Compact failed")
    } finally {
      setCompacting(false)
    }
  }

  useEffect(() => {
    if (!catalogName || !tableName) return

    wsClient
      .getCatalog(catalogName)
      .then((data) => {
        const tables = (data as { tables?: Record<string, TableInfo> }).tables ?? {}
        setTableInfo(tables[tableName] ?? null)
      })
      .catch((err) => console.error("Failed to load table info:", err))

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

  const activeRows = tableInfo?.active_rows ?? 0
  const sealedRows = tableInfo?.sealed_rows ?? 0
  const s3Rows = tableInfo?.s3_rows ?? 0
  const totalRows = activeRows + sealedRows + s3Rows

  return (
    <div className="space-y-6">
      <HeaderActions>
        <Button
          size="sm"
          variant="secondary"
          disabled={compacting}
          onClick={handleCompact}
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${compacting ? "animate-spin" : ""}`} />
          {compacting ? "Compacting..." : "Compact"}
        </Button>
        <Button
          size="sm"
          variant="secondary"
          disabled={reindexing}
          onClick={handleReindex}
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${reindexing ? "animate-spin" : ""}`} />
          {reindexing ? "Reindexing..." : "Reindex"}
        </Button>
      </HeaderActions>

      <Badge variant="outline">{catalogName}</Badge>

      {/* Summary row */}
      {tableInfo && (
        <div className="grid gap-4 md:grid-cols-4">
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Total Rows</p>
              <p className="text-2xl font-bold">{formatNumber(totalRows)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Columns</p>
              <p className="text-2xl font-bold">{tableInfo.num_columns ?? tableInfo.schema?.length ?? 0}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Indexes</p>
              <p className="text-2xl font-bold">{tableInfo.indexes?.length ?? 0}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">S3 Dataset</p>
              {tableInfo.s3_dataset_uri ? (
                <p className="text-xs font-mono text-muted-foreground mt-1 break-all">{tableInfo.s3_dataset_uri}</p>
              ) : (
                <p className="text-sm text-muted-foreground">Not configured</p>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* Storage Tiers */}
      {tableInfo && (
        <Card>
          <CardHeader>
            <CardTitle>Storage Tiers</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Tier</TableHead>
                  <TableHead className="text-right">Rows</TableHead>
                  <TableHead className="text-right">Version</TableHead>
                  <TableHead className="text-right">Segment</TableHead>
                  <TableHead className="text-right">% of Total</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {([
                  ["Active", activeRows, tableInfo.active_version, tableInfo.active_segment, "text-orange-500"],
                  ["Sealed", sealedRows, tableInfo.sealed_version, tableInfo.sealed_segment, "text-yellow-500"],
                  ["S3", s3Rows, tableInfo.s3_version || null, null, "text-blue-500"],
                ] as [string, number, number | null, number | null, string][]).map(([name, rows, version, segment, color]) => (
                  <TableRow key={name}>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <span className={`h-2 w-2 rounded-full ${color.replace("text-", "bg-")}`} />
                        <span className="font-medium">{name}</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {formatNumber(rows)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {version != null ? `v${version}` : "\u2014"}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {segment != null ? `#${segment}` : "\u2014"}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {totalRows > 0 ? `${((rows / totalRows) * 100).toFixed(1)}%` : "0%"}
                    </TableCell>
                  </TableRow>
                ))}
                <TableRow className="font-semibold">
                  <TableCell>Total</TableCell>
                  <TableCell className="text-right font-mono text-sm">
                    {formatNumber(totalRows)}
                  </TableCell>
                  <TableCell />
                  <TableCell />
                  <TableCell className="text-right font-mono text-sm">100%</TableCell>
                </TableRow>
              </TableBody>
            </Table>

            {/* Visual bar */}
            {totalRows > 0 && (
              <div className="mt-4 flex h-3 w-full overflow-hidden rounded-full bg-muted">
                <div
                  className="bg-orange-500"
                  style={{ width: `${(activeRows / totalRows) * 100}%` }}
                />
                <div
                  className="bg-yellow-500"
                  style={{ width: `${(sealedRows / totalRows) * 100}%` }}
                />
                <div
                  className="bg-blue-500"
                  style={{ width: `${(s3Rows / totalRows) * 100}%` }}
                />
              </div>
            )}
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
                        : "\u2014"}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      v{idx.dataset_version}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
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
