import { useState } from "react"
import { useAuthStore } from "@/stores/auth"
import { apiKeyApi } from "@/lib/api"
import type { Scope } from "@/lib/api"
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Plus, Copy, Key } from "lucide-react"
import { toast } from "sonner"
import { HeaderActions } from "@/components/layout/header-actions"

export function ApiKeyListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const [open, setOpen] = useState(false)
  const [scopeType, setScopeType] = useState<string>("TenantAdmin")
  const [scopeValue, setScopeValue] = useState("")
  const [ttl, setTtl] = useState("")
  const [creating, setCreating] = useState(false)
  const [createdKey, setCreatedKey] = useState<{ key_id: number; raw_key: string; token: string } | null>(null)

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!tenantId) return
    setCreating(true)

    try {
      let scopes: Scope[]
      if (scopeType === "TenantAdmin") {
        scopes = ["TenantAdmin"]
      } else if (scopeType === "Catalog") {
        scopes = [{ Catalog: scopeValue }]
      } else {
        scopes = [{ CatalogRead: scopeValue }]
      }

      const result = await apiKeyApi.create(
        tenantId,
        scopes,
        ttl ? Number(ttl) : undefined
      )
      setCreatedKey(result)
      toast.success(`API key #${result.key_id} created`)
    } catch {
      toast.error("Failed to create API key")
    } finally {
      setCreating(false)
    }
  }

  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text)
    toast.success(`${label} copied to clipboard`)
  }

  return (
    <div className="space-y-6">
      <HeaderActions>
        <Dialog
          open={open}
          onOpenChange={(v) => {
            setOpen(v)
            if (!v) setCreatedKey(null)
          }}
        >
          <DialogTrigger asChild>
            <Button size="sm" variant="secondary">
              <Plus className="h-4 w-4 mr-2" />
              Create API Key
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create API Key</DialogTitle>
            </DialogHeader>
            {createdKey ? (
              <div className="space-y-4">
                <div className="rounded-md bg-muted p-4 space-y-3">
                  <div>
                    <p className="text-xs text-muted-foreground mb-1">Key ID</p>
                    <p className="font-mono text-sm">{createdKey.key_id}</p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground mb-1">Raw Key (save this!)</p>
                    <div className="flex items-center gap-2">
                      <code className="text-xs break-all flex-1">{createdKey.raw_key}</code>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => copyToClipboard(createdKey.raw_key, "Raw key")}
                      >
                        <Copy className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground mb-1">Bearer Token</p>
                    <div className="flex items-center gap-2">
                      <code className="text-xs break-all flex-1">{createdKey.token}</code>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => copyToClipboard(createdKey.token, "Token")}
                      >
                        <Copy className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </div>
                <p className="text-xs text-destructive">
                  Save the raw key now. It won't be shown again.
                </p>
              </div>
            ) : (
              <form onSubmit={handleCreate} className="space-y-4">
                <div className="space-y-2">
                  <Label>Scope</Label>
                  <Select value={scopeType} onValueChange={setScopeType}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="TenantAdmin">TenantAdmin</SelectItem>
                      <SelectItem value="Catalog">Catalog (read/write)</SelectItem>
                      <SelectItem value="CatalogRead">CatalogRead (read-only)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {scopeType !== "TenantAdmin" && (
                  <div className="space-y-2">
                    <Label>Catalog Name</Label>
                    <Input
                      value={scopeValue}
                      onChange={(e) => setScopeValue(e.target.value)}
                      placeholder="analytics"
                    />
                  </div>
                )}
                <div className="space-y-2">
                  <Label>TTL (seconds, optional)</Label>
                  <Input
                    type="number"
                    value={ttl}
                    onChange={(e) => setTtl(e.target.value)}
                    placeholder="3600"
                  />
                </div>
                <Button type="submit" disabled={creating} className="w-full">
                  {creating ? "Creating..." : "Create API Key"}
                </Button>
              </form>
            )}
          </DialogContent>
        </Dialog>
      </HeaderActions>

      <Card>
        <CardHeader>
          <CardTitle>API Key Management</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-4 p-8 text-center justify-center text-muted-foreground">
            <Key className="h-8 w-8" />
            <p className="text-sm">
              Create and manage API keys for your tenant. Use the button above to create a new key.
              <br />
              <span className="text-xs">
                Note: The API key list endpoint is not yet available. Keys can be created and revoked.
              </span>
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
