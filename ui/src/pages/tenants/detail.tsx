import { useEffect, useState } from "react"
import { useParams } from "react-router"
import { tenantApi, catalogApi, type Tenant, type CatalogEntry } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Link } from "react-router"

export function TenantDetailPage() {
  const { tenantId } = useParams()
  const [tenant, setTenant] = useState<Tenant | null>(null)
  const [catalogs, setCatalogs] = useState<CatalogEntry[]>([])

  useEffect(() => {
    if (!tenantId) return
    const id = Number(tenantId)
    tenantApi.get(id).then(setTenant).catch(() => {})
    catalogApi.list(id).then(setCatalogs).catch(() => {})
  }, [tenantId])

  if (!tenant) {
    return <p className="text-muted-foreground">Loading tenant...</p>
  }

  return (
    <div className="space-y-6">


      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Tenant ID</p>
            <p className="text-2xl font-bold">{tenant.id}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Max Catalogs</p>
            <p className="text-2xl font-bold">{tenant.limits.max_catalogs}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Max API Keys</p>
            <p className="text-2xl font-bold">{tenant.limits.max_api_keys}</p>
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
                  key={c.id}
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
    </div>
  )
}
