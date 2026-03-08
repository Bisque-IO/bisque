import { useEffect, useState, useCallback } from "react"
import { useAuthStore } from "@/stores/auth"
import { tenantApi, type Tenant } from "@/lib/api"
import { wsClient } from "@/lib/ws"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Plus, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { Link } from "react-router"
import { HeaderActions } from "@/components/layout/header-actions"

export function TenantListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const accountId = useAuthStore((s) => s.accountId)
  const [tenants, setTenants] = useState<Tenant[]>([])
  const [open, setOpen] = useState(false)
  const [name, setName] = useState("")
  const [creating, setCreating] = useState(false)

  const fetchTenants = useCallback(async () => {
    try {
      const list = await wsClient.listTenants(accountId ?? 1)
      setTenants(list)
    } catch {
      // fallback: show nothing
    }
  }, [accountId])

  useEffect(() => {
    fetchTenants()
  }, [fetchTenants])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim()) return
    setCreating(true)
    try {
      const result = await tenantApi.create(accountId ?? 1, name.trim())
      toast.success(`Tenant created with ID ${result.tenant_id}`)
      setOpen(false)
      setName("")
      fetchTenants()
    } catch {
      toast.error("Failed to create tenant")
    } finally {
      setCreating(false)
    }
  }

  const handleDelete = async (id: number) => {
    try {
      await wsClient.deleteTenant(id)
      toast.success("Tenant deleted")
      fetchTenants()
    } catch {
      toast.error("Failed to delete tenant")
    }
  }

  return (
    <div className="space-y-6">
      <HeaderActions>
        <Dialog open={open} onOpenChange={setOpen}>
          <DialogTrigger asChild>
            <Button size="sm" variant="secondary">
              <Plus className="h-4 w-4 mr-2" />
              Create Tenant
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create Tenant</DialogTitle>
            </DialogHeader>
            <form onSubmit={handleCreate} className="space-y-4">
              <div className="space-y-2">
                <Label>Name</Label>
                <Input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="my-tenant"
                />
              </div>
              <Button type="submit" disabled={creating} className="w-full">
                {creating ? "Creating..." : "Create"}
              </Button>
            </form>
          </DialogContent>
        </Dialog>
      </HeaderActions>

      <Card>
        <CardHeader>
          <CardTitle>Tenants ({tenants.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {tenants.length === 0 ? (
            <p className="text-sm text-muted-foreground">No tenants found.</p>
          ) : (
            <div className="space-y-2">
              {tenants.map((t) => (
                <div
                  key={t.tenant_id}
                  className="flex items-center gap-4 rounded-md border p-4 hover:bg-accent/50 transition-colors"
                >
                  <Link to={`/tenants/${t.tenant_id}`} className="flex-1 min-w-0">
                    <p className="font-medium">
                      {t.name}
                      {t.tenant_id === tenantId && (
                        <Badge variant="default" className="ml-2 text-xs">current</Badge>
                      )}
                    </p>
                    <p className="text-sm text-muted-foreground">
                      ID: {t.tenant_id} &middot; Max catalogs: {t.limits?.max_catalogs ?? "—"}
                    </p>
                  </Link>
                  {t.tenant_id !== tenantId && (
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleDelete(t.tenant_id)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  )}
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
