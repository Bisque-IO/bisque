import { useEffect, useState } from "react"
import { useParams, Link, useLocation } from "react-router"
import { useAuthStore } from "@/stores/auth"
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
import { Table2, Plus, Trash2, PlusCircle, X, Activity, Database, MessageSquare } from "lucide-react"
import { toast } from "sonner"
import { HeaderActions } from "@/components/layout/header-actions"

interface CatalogInfo {
  tables: Record<string, TableInfo>
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
  const location = useLocation()
  const tenantId = useAuthStore((s) => s.tenantId)
  const connState = useConnectionStore((s) => s.state)

  const [engine, setEngine] = useState<string>(
    (location.state as { engine?: string })?.engine ?? "",
  )
  const [catalog, setCatalog] = useState<CatalogInfo | null>(null)
  const [error, setError] = useState("")
  const [createOpen, setCreateOpen] = useState(false)
  const [tableName, setTableName] = useState("")
  const [columns, setColumns] = useState<ColumnDef[]>([
    { name: "", type: "Utf8", nullable: false },
  ])
  const [creating, setCreating] = useState(false)

  const isSystemOtel = catalogName === "sys"

  // Resolve engine from catalog list if not passed via route state
  useEffect(() => {
    if (engine || !tenantId || !catalogName || connState !== "connected") return
    wsClient
      .listCatalogs(tenantId)
      .then((entries) => {
        const entry = entries.find((e) => e.name === catalogName)
        if (entry) setEngine(entry.engine)
      })
      .catch(() => {})
  }, [engine, tenantId, catalogName, connState])

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

  const addColumn = () => setColumns([...columns, { name: "", type: "Utf8", nullable: false }])
  const removeColumn = (i: number) => setColumns(columns.filter((_, idx) => idx !== i))
  const updateColumn = (i: number, updates: Partial<ColumnDef>) =>
    setColumns(columns.map((c, idx) => (idx === i ? { ...c, ...updates } : c)))

  if (error) {
    return <p className="text-destructive">{error}</p>
  }

  return (
    <div className="space-y-6">
      {/* Engine badge + actions */}
      <HeaderActions>
        {isSystemOtel ? (
          <Badge variant="secondary" className="gap-1">
            <Activity className="h-3 w-3" />
            System OTel
          </Badge>
        ) : engine ? (
          <Badge variant="outline" className="text-xs">
            {engine}
          </Badge>
        ) : null}
        {engine === "Lance" && !isSystemOtel && (
          <Button size="sm" variant="secondary" onClick={() => setCreateOpen(true)}>
            <Plus className="h-4 w-4 mr-2" />
            Create Table
          </Button>
        )}
      </HeaderActions>

      {!catalog ? (
        <p className="text-muted-foreground">Loading catalog...</p>
      ) : isSystemOtel ? (
        <OtelCatalogView catalogName={catalogName!} catalog={catalog} />
      ) : engine === "Lance" ? (
        <LanceCatalogView
          catalogName={catalogName!}
          catalog={catalog}
          onDropTable={handleDropTable}
        />
      ) : engine === "Mq" ? (
        <MqCatalogView catalogName={catalogName!} />
      ) : engine === "LibSql" ? (
        <LibSqlCatalogView catalogName={catalogName!} />
      ) : (
        <GenericCatalogView catalog={catalog} />
      )}

      {/* Create table dialog (Lance only, not for system OTel) */}
      {engine === "Lance" && !isSystemOtel && (
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
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// OTel system catalog view — read-only table list
// ---------------------------------------------------------------------------

function OtelCatalogView({
  catalogName,
  catalog,
}: {
  catalogName: string
  catalog: CatalogInfo
}) {
  const tableEntries = Object.entries(catalog.tables ?? {})

  if (tableEntries.length === 0) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Activity className="mx-auto h-12 w-12 text-muted-foreground/50" />
          <p className="mt-4 text-muted-foreground">
            OTel tables are auto-created when the server starts.
          </p>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
      {tableEntries.map(([name, info]) => (
        <Link key={name} to={`/tables/${catalogName}/${name}`}>
          <Card className="hover:border-primary/50 transition-colors">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-base">
                <Activity className="h-4 w-4" />
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
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Lance catalog view — tables grid
// ---------------------------------------------------------------------------

function LanceCatalogView({
  catalogName,
  catalog,
  onDropTable,
}: {
  catalogName: string
  catalog: CatalogInfo
  onDropTable: (table: string) => void
}) {
  const tableEntries = Object.entries(catalog.tables ?? {})

  if (tableEntries.length === 0) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Table2 className="mx-auto h-12 w-12 text-muted-foreground/50" />
          <p className="mt-4 text-muted-foreground">No tables in this catalog yet.</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
      {tableEntries.map(([name, info]) => (
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
            onClick={() => onDropTable(name)}
          >
            <Trash2 className="h-4 w-4 text-destructive" />
          </Button>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Mq catalog view
// ---------------------------------------------------------------------------

function MqCatalogView({ catalogName }: { catalogName: string }) {
  return (
    <Card>
      <CardContent className="py-12 text-center">
        <MessageSquare className="mx-auto h-12 w-12 text-muted-foreground/50" />
        <p className="mt-4 font-medium">{catalogName}</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Message queue catalog. Topic and consumer management coming soon.
        </p>
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// LibSql catalog view
// ---------------------------------------------------------------------------

function LibSqlCatalogView({ catalogName }: { catalogName: string }) {
  return (
    <Card>
      <CardContent className="py-12 text-center">
        <Database className="mx-auto h-12 w-12 text-muted-foreground/50" />
        <p className="mt-4 font-medium">{catalogName}</p>
        <p className="mt-1 text-sm text-muted-foreground">
          LibSQL catalog. Database management coming soon.
        </p>
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Generic fallback (engine unknown or not yet resolved)
// ---------------------------------------------------------------------------

function GenericCatalogView({ catalog }: { catalog: CatalogInfo }) {
  const tableCount = Object.keys(catalog.tables ?? {}).length

  return (
    <Card>
      <CardContent className="py-8">
        <p className="text-sm text-muted-foreground">
          {tableCount} table(s) in this catalog.
        </p>
      </CardContent>
    </Card>
  )
}
