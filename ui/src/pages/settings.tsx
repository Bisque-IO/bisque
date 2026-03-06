import { useState } from "react"
import { useAuthStore } from "@/stores/auth"
import { useClusterStore, type ClusterConfig } from "@/stores/cluster"
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
import { Trash2, Pencil, Plus } from "lucide-react"
import { toast } from "sonner"

export function SettingsPage() {
  const { tenantId, tenantName, token } = useAuthStore()
  const { clusters, activeId, addCluster, removeCluster, updateCluster, setActive } =
    useClusterStore()
  const activeCluster = clusters.find((c) => c.id === activeId) ?? clusters[0]

  const [dialogOpen, setDialogOpen] = useState(false)
  const [editId, setEditId] = useState<string | null>(null)
  const [name, setName] = useState("")
  const [url, setUrl] = useState("")

  const openAdd = () => {
    setEditId(null)
    setName("")
    setUrl("")
    setDialogOpen(true)
  }

  const openEdit = (cluster: ClusterConfig) => {
    setEditId(cluster.id)
    setName(cluster.name)
    setUrl(cluster.url)
    setDialogOpen(true)
  }

  const handleSave = (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim()) return

    if (editId) {
      updateCluster(editId, { name: name.trim(), url: url.trim().replace(/\/+$/, "") })
      toast.success("Cluster updated")
    } else {
      if (!url.trim()) return
      const id = name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-")
      addCluster({ id, name: name.trim(), url: url.trim().replace(/\/+$/, "") })
      toast.success("Cluster added")
    }
    setDialogOpen(false)
  }

  const handleRemove = (id: string) => {
    removeCluster(id)
    toast.success("Cluster removed")
  }

  return (
    <div className="space-y-6">


      <Card>
        <CardHeader>
          <CardTitle>Connection Info</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <p className="text-sm text-muted-foreground">Cluster</p>
              <p className="font-medium">
                {activeCluster.name}{" "}
                <Badge variant="outline" className="ml-1">
                  {activeCluster.url || window.location.origin}
                </Badge>
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Tenant</p>
              <p className="font-medium">
                {tenantName ?? "Unknown"}{" "}
                <Badge variant="outline" className="ml-1">
                  #{tenantId}
                </Badge>
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Token</p>
              <p className="font-mono text-xs text-muted-foreground">
                {token ? `${token.slice(0, 20)}...` : "Not set"}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Clusters</CardTitle>
          <Button size="sm" onClick={openAdd}>
            <Plus className="h-4 w-4 mr-2" />
            Add
          </Button>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {clusters.map((cluster) => (
              <div
                key={cluster.id}
                className="flex items-center gap-4 rounded-md border p-3"
              >
                <div className="flex-1 min-w-0">
                  <p className="font-medium flex items-center gap-2">
                    {cluster.name}
                    {cluster.id === activeId && (
                      <Badge variant="default" className="text-xs">
                        active
                      </Badge>
                    )}
                  </p>
                  <p className="text-xs text-muted-foreground font-mono truncate">
                    {cluster.url || "local (same origin)"}
                  </p>
                </div>
                <div className="flex gap-1">
                  {cluster.id !== activeId && (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setActive(cluster.id)}
                    >
                      Switch
                    </Button>
                  )}
                  {cluster.id !== "local" && (
                    <>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => openEdit(cluster)}
                      >
                        <Pencil className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => handleRemove(cluster.id)}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </>
                  )}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Server Ports</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <div className="flex justify-between border-b pb-2">
            <span className="text-muted-foreground">HTTP (Management, S3, OTLP HTTP)</span>
            <span className="font-mono">3200</span>
          </div>
          <div className="flex justify-between border-b pb-2">
            <span className="text-muted-foreground">Flight SQL gRPC</span>
            <span className="font-mono">50051</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">OTLP gRPC</span>
            <span className="font-mono">4317</span>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>About</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Bisque is a unified analytics server combining Lance (OLAP, time-series, vector search,
            full-text search), observability (traces, metrics, logs via OpenTelemetry), and multi-tenant
            management into a single binary.
          </p>
        </CardContent>
      </Card>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editId ? "Edit Cluster" : "Add Cluster"}</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSave} className="space-y-4">
            <div className="space-y-2">
              <Label>Name</Label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="production"
              />
            </div>
            <div className="space-y-2">
              <Label>URL</Label>
              <Input
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://bisque.example.com"
                disabled={editId === "local"}
              />
              {editId === "local" && (
                <p className="text-xs text-muted-foreground">
                  Local cluster always uses the current origin.
                </p>
              )}
            </div>
            <Button type="submit" className="w-full">
              {editId ? "Save" : "Add Cluster"}
            </Button>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  )
}
