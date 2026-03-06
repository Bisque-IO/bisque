import { useEffect, useState } from "react"
import { useAuthStore } from "@/stores/auth"
import { catalogApi, type CatalogEntry } from "@/lib/api"
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
import { Plus, Database } from "lucide-react"
import { toast } from "sonner"
import { Link } from "react-router"
import { HeaderActions } from "@/components/layout/header-actions"

export function CatalogListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const tenantName = useAuthStore((s) => s.tenantName)
  const [catalogs, setCatalogs] = useState<CatalogEntry[]>([])
  const [open, setOpen] = useState(false)
  const [name, setName] = useState("")
  const [engine, setEngine] = useState("Lance")
  const [creating, setCreating] = useState(false)

  const fetchCatalogs = () => {
    if (!tenantId) return
    catalogApi.list(tenantId).then(setCatalogs).catch(() => {})
  }

  useEffect(fetchCatalogs, [tenantId])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!tenantId || !name.trim()) return
    setCreating(true)
    try {
      await catalogApi.create(tenantId, name.trim(), engine)
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
              <Link key={c.id} to={`/catalogs/${c.name}`}>
                <Card className="hover:border-primary/50 transition-colors">
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between">
                      {c.name}
                      <Badge variant="secondary">{c.engine}</Badge>
                    </CardTitle>
                    <CardDescription>Raft group #{c.raft_group_id}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p className="text-xs text-muted-foreground">Catalog ID: {c.id}</p>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
