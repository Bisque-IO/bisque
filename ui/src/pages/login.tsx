import { useState, useEffect } from "react"
import { useNavigate } from "react-router"
import { useAuthStore } from "@/stores/auth"
import { authApi, isMock } from "@/lib/api"
import { useClusterStore } from "@/stores/cluster"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Plus, ToggleLeft, ToggleRight } from "lucide-react"
import { toast } from "sonner"

export function LoginPage() {
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState("")
  const [loading, setLoading] = useState(false)
  const login = useAuthStore((s) => s.login)
  const { clusters, activeId, setActive, addCluster, updateCluster } = useClusterStore()
  const activeCluster = clusters.find((c) => c.id === activeId) ?? clusters[0]
  const navigate = useNavigate()
  const mock = isMock()

  // Add cluster dialog
  const [addOpen, setAddOpen] = useState(false)
  const [newName, setNewName] = useState("")
  const [newUrl, setNewUrl] = useState("")

  // Auto-login in mock mode
  useEffect(() => {
    if (mock) {
      authApi.login("admin", "admin").then((res) => {
        login({
          token: res.token,
          userId: res.user_id,
          username: "admin",
          accounts: res.accounts,
          tenantId: 1,
          tenantName: "Acme Corp",
        })
        navigate("/")
      })
    }
  }, [mock, login, navigate])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError("")
    setLoading(true)

    try {
      if (!username.trim() || !password.trim()) {
        setError("Both fields are required")
        return
      }

      const res = await authApi.login(username.trim(), password.trim())
      login({
        token: res.token,
        userId: res.user_id,
        username: username.trim(),
        accounts: res.accounts,
        tenantId: res.default_tenant_id,
        tenantName: res.default_tenant_name,
      })
      navigate("/")
    } catch {
      setError("Invalid username or password")
    } finally {
      setLoading(false)
    }
  }

  const handleAddCluster = (e: React.FormEvent) => {
    e.preventDefault()
    if (!newName.trim() || !newUrl.trim()) return
    const id = newName.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-")
    addCluster({ id, name: newName.trim(), url: newUrl.trim().replace(/\/+$/, ""), mock: false })
    setActive(id)
    setAddOpen(false)
    setNewName("")
    setNewUrl("")
    toast.success("Cluster added")
  }

  const toggleMock = () => {
    updateCluster(activeCluster.id, { mock: !activeCluster.mock })
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <CardTitle className="text-2xl">bisque</CardTitle>
          <CardDescription>Sign in to your cluster</CardDescription>
          {mock && (
            <Badge variant="secondary" className="mx-auto mt-2">
              Mock Mode — auto-logging in...
            </Badge>
          )}
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Cluster selector */}
          <div className="space-y-2">
            <Label>Cluster</Label>
            <div className="flex gap-2">
              <Select value={activeId} onValueChange={setActive}>
                <SelectTrigger className="flex-1">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {clusters.map((c) => (
                    <SelectItem key={c.id} value={c.id}>
                      {c.name}
                      {c.mock ? " (mock)" : ""}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                onClick={toggleMock}
                title={activeCluster.mock ? "Switch to live" : "Switch to mock"}
              >
                {activeCluster.mock ? (
                  <ToggleLeft className="h-4 w-4 text-muted-foreground" />
                ) : (
                  <ToggleRight className="h-4 w-4 text-green-500" />
                )}
              </Button>
              <Button
                type="button"
                variant="outline"
                size="icon"
                onClick={() => setAddOpen(true)}
                title="Add cluster"
              >
                <Plus className="h-4 w-4" />
              </Button>
            </div>
            <p className="text-xs text-muted-foreground font-mono">
              {activeCluster.url || window.location.origin}
            </p>
          </div>

          {/* Login form */}
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="username">Username</Label>
              <Input
                id="username"
                type="text"
                placeholder="admin"
                autoComplete="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <Input
                id="password"
                type="password"
                placeholder="Enter your password..."
                autoComplete="current-password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>
            {error && (
              <p className="text-sm text-destructive">{error}</p>
            )}
            <Button type="submit" className="w-full" disabled={loading}>
              {loading ? "Signing in..." : "Sign In"}
            </Button>
          </form>
        </CardContent>
      </Card>

      {/* Add cluster dialog */}
      <Dialog open={addOpen} onOpenChange={setAddOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Cluster</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleAddCluster} className="space-y-4">
            <div className="space-y-2">
              <Label>Name</Label>
              <Input
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="production"
              />
            </div>
            <div className="space-y-2">
              <Label>URL</Label>
              <Input
                value={newUrl}
                onChange={(e) => setNewUrl(e.target.value)}
                placeholder="https://bisque.example.com"
              />
            </div>
            <Button type="submit" className="w-full">
              Add Cluster
            </Button>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  )
}
