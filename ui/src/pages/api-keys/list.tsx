import { useState, useEffect, useCallback } from "react"
import { useAuthStore } from "@/stores/auth"
import { apiKeyApi, type ApiKeyEntry } from "@/lib/api"
import type { Scope } from "@/lib/api"
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Plus, Copy, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { HeaderActions } from "@/components/layout/header-actions"

function formatScope(scope: Scope): string {
  if (typeof scope === "string") return scope
  if ("AccountAdmin" in scope) return `AccountAdmin(${scope.AccountAdmin})`
  if ("Catalog" in scope) return `Catalog(${scope.Catalog})`
  if ("CatalogRead" in scope) return `CatalogRead(${scope.CatalogRead})`
  return JSON.stringify(scope)
}

export function ApiKeyListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const [keys, setKeys] = useState<ApiKeyEntry[]>([])
  const [open, setOpen] = useState(false)
  const [scopeType, setScopeType] = useState<string>("TenantAdmin")
  const [scopeValue, setScopeValue] = useState("")
  const [ttl, setTtl] = useState("")
  const [creating, setCreating] = useState(false)
  const [createdKey, setCreatedKey] = useState<{ key_id: number; raw_key: string; token: string } | null>(null)

  const fetchKeys = useCallback(async () => {
    if (!tenantId) return
    try {
      const list = await wsClient.listApiKeys(tenantId)
      setKeys(list)
    } catch {
      // ignore
    }
  }, [tenantId])

  useEffect(() => {
    fetchKeys()
  }, [fetchKeys])

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
      fetchKeys()
    } catch {
      toast.error("Failed to create API key")
    } finally {
      setCreating(false)
    }
  }

  const handleRevoke = async (keyId: number) => {
    if (!confirm("Revoke this API key? This cannot be undone.")) return
    try {
      await wsClient.revokeApiKey(keyId)
      toast.success("API key revoked")
      fetchKeys()
    } catch {
      toast.error("Failed to revoke API key")
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
          <CardTitle>API Keys ({keys.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {keys.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">
              No API keys found. Create one to get started.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Scopes</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead className="w-[50px]" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {keys.map((k) => (
                  <TableRow key={k.key_id}>
                    <TableCell className="font-mono text-sm">{k.key_id}</TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {k.scopes.map((s, i) => (
                          <Badge key={i} variant="secondary" className="text-xs">
                            {formatScope(s)}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell>
                      {k.revoked ? (
                        <Badge variant="destructive">Revoked</Badge>
                      ) : (
                        <Badge className="bg-green-500/15 text-green-600 border-green-500/30">Active</Badge>
                      )}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {new Date(k.created_at).toLocaleDateString()}
                    </TableCell>
                    <TableCell>
                      {!k.revoked && (
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleRevoke(k.key_id)}
                        >
                          <Trash2 className="h-4 w-4 text-destructive" />
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
