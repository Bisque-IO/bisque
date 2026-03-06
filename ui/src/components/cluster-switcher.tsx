import { useState } from "react"
import { useClusterStore, type ClusterConfig } from "@/stores/cluster"
import { useAuthStore } from "@/stores/auth"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Server, Plus, Check } from "lucide-react"

export function ClusterSwitcher() {
  const { clusters, activeId, setActive, addCluster } = useClusterStore()
  const token = useAuthStore((s) => s.token)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [name, setName] = useState("")
  const [url, setUrl] = useState("")

  const activeCluster = clusters.find((c) => c.id === activeId) ?? clusters[0]

  const handleAdd = (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim() || !url.trim()) return
    const id = name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-")
    const cluster: ClusterConfig = {
      id,
      name: name.trim(),
      url: url.trim().replace(/\/+$/, ""),
    }
    addCluster(cluster)
    setActive(id)
    setDialogOpen(false)
    setName("")
    setUrl("")
  }

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm" className="gap-2">
            <Server className="h-3.5 w-3.5" />
            <span className="max-w-[120px] truncate">{activeCluster.name}</span>
            {token && (
              <span className="h-2 w-2 rounded-full bg-green-500" />
            )}
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          {clusters.map((cluster) => (
            <DropdownMenuItem
              key={cluster.id}
              onClick={() => setActive(cluster.id)}
              className="gap-2"
            >
              {cluster.id === activeId ? (
                <Check className="h-3.5 w-3.5" />
              ) : (
                <span className="w-3.5" />
              )}
              <span>{cluster.name}</span>
              {cluster.url && (
                <span className="ml-auto text-xs text-muted-foreground font-mono">
                  {new URL(cluster.url).host}
                </span>
              )}
            </DropdownMenuItem>
          ))}
          <DropdownMenuSeparator />
          <DropdownMenuItem onClick={() => setDialogOpen(true)} className="gap-2">
            <Plus className="h-3.5 w-3.5" />
            Add Cluster
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Cluster</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleAdd} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="cluster-name">Name</Label>
              <Input
                id="cluster-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="production"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="cluster-url">URL</Label>
              <Input
                id="cluster-url"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://bisque.example.com"
              />
            </div>
            <Button type="submit" className="w-full" disabled={!name.trim() || !url.trim()}>
              Add Cluster
            </Button>
          </form>
        </DialogContent>
      </Dialog>
    </>
  )
}
