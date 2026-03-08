import { useEffect, useState } from "react"
import { useParams, useNavigate } from "react-router"
import { tenantApi, catalogApi, type Tenant, type CatalogEntry, type TenantLimits } from "@/lib/api"
import { wsClient } from "@/lib/ws"
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
import { Link } from "react-router"
import { Pencil, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { HeaderActions } from "@/components/layout/header-actions"

export function TenantDetailPage() {
  const { tenantId } = useParams()
  const navigate = useNavigate()
  const [tenant, setTenant] = useState<Tenant | null>(null)
  const [catalogs, setCatalogs] = useState<CatalogEntry[]>([])
  const [limitsOpen, setLimitsOpen] = useState(false)
  const [limits, setLimits] = useState<TenantLimits>({
    max_disk_bytes: 0,
    max_deep_storage_bytes: 0,
    max_concurrent_queries: 0,
    max_query_memory_bytes: 0,
    max_catalogs: 0,
  })

  useEffect(() => {
    if (!tenantId) return
    const id = Number(tenantId)
    tenantApi.get(id).then((t) => {
      setTenant(t)
      if (t.limits) setLimits(t.limits)
    }).catch(() => {})
    catalogApi.list(id).then(setCatalogs).catch(() => {})
  }, [tenantId])

  const handleUpdateLimits = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!tenantId) return
    try {
      await wsClient.updateTenantLimits(Number(tenantId), limits)
      toast.success("Tenant limits updated")
      setLimitsOpen(false)
      // Refresh tenant
      tenantApi.get(Number(tenantId)).then(setTenant).catch(() => {})
    } catch {
      toast.error("Failed to update limits")
    }
  }

  const handleDelete = async () => {
    if (!tenantId) return
    if (!confirm("Are you sure you want to delete this tenant?")) return
    try {
      await wsClient.deleteTenant(Number(tenantId))
      toast.success("Tenant deleted")
      navigate("/tenants")
    } catch {
      toast.error("Failed to delete tenant")
    }
  }

  if (!tenant) {
    return <p className="text-muted-foreground">Loading tenant...</p>
  }

  return (
    <div className="space-y-6">
      <HeaderActions>
        <Button size="sm" variant="secondary" onClick={() => setLimitsOpen(true)}>
          <Pencil className="h-4 w-4 mr-2" />
          Edit Limits
        </Button>
        <Button size="sm" variant="destructive" onClick={handleDelete}>
          <Trash2 className="h-4 w-4 mr-2" />
          Delete Tenant
        </Button>
      </HeaderActions>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Tenant ID</p>
            <p className="text-2xl font-bold">{tenant.tenant_id}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Max Catalogs</p>
            <p className="text-2xl font-bold">{tenant.limits?.max_catalogs ?? "—"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Max Concurrent Queries</p>
            <p className="text-2xl font-bold">{tenant.limits?.max_concurrent_queries ?? "—"}</p>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Catalogs ({catalogs.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {catalogs.length === 0 ? (
            <p className="text-sm text-muted-foreground">No catalogs</p>
          ) : (
            <div className="space-y-2">
              {catalogs.map((c) => (
                <Link
                  key={c.catalog_id}
                  to={`/catalogs/${c.name}`}
                  className="flex items-center justify-between rounded-md border p-3 hover:bg-accent/50 transition-colors"
                >
                  <div>
                    <span className="font-medium">{c.name}</span>
                    <span className="ml-2 text-xs text-muted-foreground">
                      Raft group #{c.raft_group_id}
                    </span>
                  </div>
                  <Badge variant="secondary">{c.engine}</Badge>
                </Link>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={limitsOpen} onOpenChange={setLimitsOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Tenant Limits</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleUpdateLimits} className="space-y-4">
            <div className="space-y-2">
              <Label>Max Catalogs</Label>
              <Input
                type="number"
                value={limits.max_catalogs}
                onChange={(e) => setLimits({ ...limits, max_catalogs: Number(e.target.value) })}
              />
            </div>
            <div className="space-y-2">
              <Label>Max Concurrent Queries</Label>
              <Input
                type="number"
                value={limits.max_concurrent_queries}
                onChange={(e) => setLimits({ ...limits, max_concurrent_queries: Number(e.target.value) })}
              />
            </div>
            <div className="space-y-2">
              <Label>Max Disk Bytes</Label>
              <Input
                type="number"
                value={limits.max_disk_bytes}
                onChange={(e) => setLimits({ ...limits, max_disk_bytes: Number(e.target.value) })}
              />
            </div>
            <div className="space-y-2">
              <Label>Max Deep Storage Bytes</Label>
              <Input
                type="number"
                value={limits.max_deep_storage_bytes}
                onChange={(e) => setLimits({ ...limits, max_deep_storage_bytes: Number(e.target.value) })}
              />
            </div>
            <div className="space-y-2">
              <Label>Max Query Memory Bytes</Label>
              <Input
                type="number"
                value={limits.max_query_memory_bytes}
                onChange={(e) => setLimits({ ...limits, max_query_memory_bytes: Number(e.target.value) })}
              />
            </div>
            <Button type="submit" className="w-full">Save Limits</Button>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  )
}
