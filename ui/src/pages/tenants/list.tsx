import { useState } from "react"
import { useAuthStore } from "@/stores/auth"
import { tenantApi } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Plus } from "lucide-react"
import { toast } from "sonner"
import { Link } from "react-router"

export function TenantListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const tenantName = useAuthStore((s) => s.tenantName)
  const accountId = useAuthStore((s) => s.accountId)
  const [open, setOpen] = useState(false)
  const [name, setName] = useState("")
  const [creating, setCreating] = useState(false)

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim()) return
    setCreating(true)
    try {
      const result = await tenantApi.create(accountId ?? 1, name.trim())
      toast.success(`Tenant created with ID ${result.tenant_id}`)
      setOpen(false)
      setName("")
    } catch {
      toast.error("Failed to create tenant")
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Tenants</h1>
        <Dialog open={open} onOpenChange={setOpen}>
          <DialogTrigger asChild>
            <Button>
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
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Current Tenant</CardTitle>
        </CardHeader>
        <CardContent>
          <Link
            to={`/tenants/${tenantId}`}
            className="flex items-center gap-4 rounded-md border p-4 hover:bg-accent/50 transition-colors"
          >
            <div>
              <p className="font-medium">{tenantName ?? `Tenant #${tenantId}`}</p>
              <p className="text-sm text-muted-foreground">ID: {tenantId}</p>
            </div>
          </Link>
        </CardContent>
      </Card>
    </div>
  )
}
