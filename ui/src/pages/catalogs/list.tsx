import { useEffect, useState } from "react"
import { useAuthStore } from "@/stores/auth"
import type { CatalogEntry } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Plus, Database, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { Link } from "react-router"
import { HeaderActions } from "@/components/layout/header-actions"
import { wsClient } from "@/lib/ws"
import { useConnectionStore } from "@/stores/connection"

export function CatalogListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const tenantName = useAuthStore((s) => s.tenantName)
  const connState = useConnectionStore((s) => s.state)
  const [catalogs, setCatalogs] = useState<CatalogEntry[]>([])
  const [open, setOpen] = useState(false)
  const [name, setName] = useState("")
  const [engine, setEngine] = useState("Lance")
  const [creating, setCreating] = useState(false)

  const fetchCatalogs = () => {
    if (!tenantId || connState !== "connected") return
    wsClient.listCatalogs(tenantId).then(setCatalogs).catch(() => {})
  }

  useEffect(fetchCatalogs, [tenantId, connState])

  const handleDelete = async (catalogId: number) => {
    if (!tenantId) return
    if (!confirm("Delete this catalog? This cannot be undone.")) return
    try {
      await wsClient.deleteCatalog(tenantId, catalogId)
      toast.success("Catalog deleted")
      fetchCatalogs()
    } catch {
      toast.error("Failed to delete catalog")
    }
  }

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!tenantId || !name.trim()) return
    setCreating(true)
    try {
      await wsClient.createCatalog(tenantId, name.trim(), engine)
      toast.success(`Catalog "${name}" created`)
      setOpen(false)
      setName("")
      fetchCatalogs()
    } catch {
      toast.error("Failed to create catalog")
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="space-y-6">
      <HeaderActions>
        <Dialog open={open} onOpenChange={setOpen}>
          <DialogTrigger asChild>
            <Button size="sm" variant="secondary">
              <Plus className="h-4 w-4 mr-2" />
              Create Catalog
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create Catalog</DialogTitle>
            </DialogHeader>
            <form onSubmit={handleCreate} className="space-y-4">
              <div className="space-y-2">
                <Label>Name</Label>
                <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="analytics" />
              </div>
              <div className="space-y-2">
                <Label>Engine</Label>
                <Select value={engine} onValueChange={setEngine}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Lance">Lance</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <Button type="submit" disabled={creating} className="w-full">
                {creating ? "Creating..." : "Create"}
              </Button>
            </form>
          </DialogContent>
        </Dialog>
      </HeaderActions>

      <div className="space-y-4">
        <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">
          {tenantName ?? `Tenant #${tenantId}`}
        </h2>
        {catalogs.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center">
              <Database className="mx-auto h-12 w-12 text-muted-foreground/50" />
              <p className="mt-4 text-muted-foreground">No catalogs yet. Create one to get started.</p>
            </CardContent>
          </Card>
        ) : (
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {catalogs.map((c) => (
              <div key={c.catalog_id} className="relative group">
                <Link to={`/catalogs/${c.name}`} state={{ engine: c.engine }}>
                  <Card className="hover:border-primary/50 transition-colors">
                    <CardHeader>
                      <CardTitle className="flex items-center justify-between">
                        {c.name}
                        <Badge variant="secondary">{c.engine}</Badge>
                      </CardTitle>
                      <CardDescription>Raft group #{c.raft_group_id}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <p className="text-xs text-muted-foreground">Catalog ID: {c.catalog_id}</p>
                    </CardContent>
                  </Card>
                </Link>
                <Button
                  variant="ghost"
                  size="icon"
                  className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity"
                  onClick={() => handleDelete(c.catalog_id)}
                >
                  <Trash2 className="h-4 w-4 text-destructive" />
                </Button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
