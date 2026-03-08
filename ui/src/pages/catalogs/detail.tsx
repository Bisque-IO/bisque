import { useEffect, useState } from "react"
import { useParams, Link } from "react-router"
import { wsClient } from "@/lib/ws"
import { useConnectionStore } from "@/stores/connection"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Table2, Plus, Trash2, PlusCircle, X, Activity, Check } from "lucide-react"
import { toast } from "sonner"
import { HeaderActions } from "@/components/layout/header-actions"

interface OtelInfo {
  enabled: boolean
  enabled_at?: number
  enabled_by?: string
}

interface CatalogInfo {
  tables: Record<string, TableInfo>
  otel?: OtelInfo
}

interface TableInfo {
  active_version: number
  sealed_version: number
  schema?: unknown
}

interface ColumnDef {
  name: string
  type: string
  nullable: boolean
}

const COLUMN_TYPES = [
  "Utf8", "Int32", "Int64", "Float32", "Float64",
  "Boolean", "Date32", "Timestamp", "Binary",
]

export function CatalogDetailPage() {
  const { catalogName } = useParams()
  const [catalog, setCatalog] = useState<CatalogInfo | null>(null)
  const [error, setError] = useState("")
  const [createOpen, setCreateOpen] = useState(false)
  const [tableName, setTableName] = useState("")
  const [columns, setColumns] = useState<ColumnDef[]>([
    { name: "", type: "Utf8", nullable: false },
  ])
  const [creating, setCreating] = useState(false)
  const [enablingOtel, setEnablingOtel] = useState(false)

  const connState = useConnectionStore((s) => s.state)

  const fetchCatalog = () => {
    if (!catalogName || connState !== "connected") return
    setError("")
    wsClient
      .getCatalog(catalogName)
      .then((data) => setCatalog(data as unknown as CatalogInfo))
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load catalog"))
  }

  useEffect(fetchCatalog, [catalogName, connState])

  const handleCreateTable = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!catalogName || !tableName.trim()) return
    const validColumns = columns.filter((c) => c.name.trim())
    if (validColumns.length === 0) {
      toast.error("Add at least one column")
      return
    }
    setCreating(true)
    try {
      const schemaJson = JSON.stringify(validColumns.map((c) => ({
        name: c.name.trim(),
        type: c.type,
        nullable: c.nullable,
      })))
      await wsClient.createTable(catalogName, tableName.trim(), schemaJson)
      toast.success(`Table "${tableName}" created`)
      setCreateOpen(false)
      setTableName("")
      setColumns([{ name: "", type: "Utf8", nullable: false }])
      fetchCatalog()
    } catch {
      toast.error("Failed to create table")
    } finally {
      setCreating(false)
    }
  }

  const handleDropTable = async (table: string) => {
    if (!catalogName) return
    if (!confirm(`Drop table "${table}"? This cannot be undone.`)) return
    try {
      await wsClient.dropTable(catalogName, table)
      toast.success(`Table "${table}" dropped`)
      fetchCatalog()
    } catch {
      toast.error("Failed to drop table")
    }
  }

  const handleEnableOtel = async () => {
    if (!catalogName) return
    setEnablingOtel(true)
    try {
      const result = await wsClient.enableOtel(catalogName)
      toast.success(`OTel enabled: ${result.tables_created.length} tables created`)
      fetchCatalog()
    } catch {
      toast.error("Failed to enable OTel")
    } finally {
      setEnablingOtel(false)
    }
  }

  const addColumn = () => setColumns([...columns, { name: "", type: "Utf8", nullable: false }])
  const removeColumn = (i: number) => setColumns(columns.filter((_, idx) => idx !== i))
  const updateColumn = (i: number, updates: Partial<ColumnDef>) =>
    setColumns(columns.map((c, idx) => (idx === i ? { ...c, ...updates } : c)))

  if (error) {
    return <p className="text-destructive">{error}</p>
  }

  return (
    <div className="space-y-6">
      <HeaderActions>
        {catalog?.otel?.enabled ? (
          <Badge variant="secondary" className="gap-1">
            <Check className="h-3 w-3" />
            OTel Enabled
            {catalog.otel.enabled_at && (
              <span className="text-muted-foreground ml-1">
                {new Date(catalog.otel.enabled_at * 1000).toLocaleDateString()}
              </span>
            )}
          </Badge>
        ) : (
          <Button
            size="sm"
            variant="secondary"
            disabled={enablingOtel}
            onClick={handleEnableOtel}
          >
            <Activity className={`h-4 w-4 mr-2 ${enablingOtel ? "animate-pulse" : ""}`} />
            {enablingOtel ? "Enabling..." : "Enable OTel"}
          </Button>
        )}
        <Button size="sm" variant="secondary" onClick={() => setCreateOpen(true)}>
          <Plus className="h-4 w-4 mr-2" />
          Create Table
        </Button>
      </HeaderActions>

      {!catalog ? (
        <p className="text-muted-foreground">Loading catalog...</p>
      ) : (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {Object.entries(catalog.tables ?? {}).map(([name, info]) => (
              <div key={name} className="relative group">
                <Link to={`/tables/${catalogName}/${name}`}>
                  <Card className="hover:border-primary/50 transition-colors">
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2 text-base">
                        <Table2 className="h-4 w-4" />
                        {name}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-1">
                      <div className="flex gap-2">
                        <Badge variant="outline">Active v{info.active_version}</Badge>
                        <Badge variant="secondary">Sealed v{info.sealed_version}</Badge>
                      </div>
                    </CardContent>
                  </Card>
                </Link>
                <Button
                  variant="ghost"
                  size="icon"
                  className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity"
                  onClick={() => handleDropTable(name)}
                >
                  <Trash2 className="h-4 w-4 text-destructive" />
                </Button>
              </div>
            ))}
          </div>

          {Object.keys(catalog.tables ?? {}).length === 0 && (
            <Card>
              <CardContent className="py-12 text-center">
                <Table2 className="mx-auto h-12 w-12 text-muted-foreground/50" />
                <p className="mt-4 text-muted-foreground">No tables in this catalog yet.</p>
              </CardContent>
            </Card>
          )}
        </>
      )}

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Create Table</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleCreateTable} className="space-y-4">
            <div className="space-y-2">
              <Label>Table Name</Label>
              <Input
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
                placeholder="events"
              />
            </div>
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label>Columns</Label>
                <Button type="button" variant="ghost" size="sm" onClick={addColumn}>
                  <PlusCircle className="h-4 w-4 mr-1" />
                  Add
                </Button>
              </div>
              <div className="space-y-2 max-h-60 overflow-auto">
                {columns.map((col, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <Input
                      value={col.name}
                      onChange={(e) => updateColumn(i, { name: e.target.value })}
                      placeholder="column_name"
                      className="flex-1"
                    />
                    <Select value={col.type} onValueChange={(v) => updateColumn(i, { type: v })}>
                      <SelectTrigger className="w-[130px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {COLUMN_TYPES.map((t) => (
                          <SelectItem key={t} value={t}>{t}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      onClick={() => updateColumn(i, { nullable: !col.nullable })}
                      className="text-xs w-16"
                    >
                      {col.nullable ? "NULL" : "NOT NULL"}
                    </Button>
                    {columns.length > 1 && (
                      <Button type="button" variant="ghost" size="icon" onClick={() => removeColumn(i)}>
                        <X className="h-3 w-3" />
                      </Button>
                    )}
                  </div>
                ))}
              </div>
            </div>
            <Button type="submit" disabled={creating} className="w-full">
              {creating ? "Creating..." : "Create Table"}
            </Button>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  )
}
