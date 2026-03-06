import { useState, useEffect } from "react"
import { useNavigate } from "react-router"
import { useAuthStore } from "@/stores/auth"
import { authApi, MOCK } from "@/lib/api"
import { useClusterStore } from "@/stores/cluster"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"

export function LoginPage() {
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState("")
  const [loading, setLoading] = useState(false)
  const login = useAuthStore((s) => s.login)
  const activeCluster = useClusterStore((s) => s.activeCluster)
  const navigate = useNavigate()

  // Auto-login in mock mode
  useEffect(() => {
    if (MOCK) {
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
  }, [login, navigate])

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
      })
      navigate("/")
    } catch {
      setError("Invalid username or password")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <CardTitle className="text-2xl">bisque</CardTitle>
          <CardDescription>
            Sign in to <span className="font-medium">{activeCluster.name}</span>
            {activeCluster.url && (
              <span className="block text-xs font-mono mt-1">{activeCluster.url}</span>
            )}
          </CardDescription>
          {MOCK && (
            <Badge variant="secondary" className="mx-auto mt-2">
              Mock Mode — auto-logging in...
            </Badge>
          )}
        </CardHeader>
        <CardContent>
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
    </div>
  )
}
