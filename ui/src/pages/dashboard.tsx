import { useEffect, useState } from "react"
import { useAuthStore } from "@/stores/auth"
import { useCatalogsStore } from "@/stores/catalogs"
import { tenantApi, catalogApi, type Tenant, type CatalogEntry } from "@/lib/api"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Database, Key, Table2, Activity } from "lucide-react"
import { Link } from "react-router"

export function DashboardPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const [tenant, setTenant] = useState<Tenant | null>(null)
  const [catalogs, setCatalogs] = useState<CatalogEntry[]>([])
  const events = useCatalogsStore((s) => s.events)

  useEffect(() => {
    if (!tenantId) return
    tenantApi.get(tenantId).then(setTenant).catch(() => {})
    catalogApi.list(tenantId).then(setCatalogs).catch(() => {})
  }, [tenantId])

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatCard
          icon={<Database className="h-4 w-4" />}
          title="Catalogs"
          value={catalogs.length}
        />
        <StatCard
          icon={<Table2 className="h-4 w-4" />}
          title="Max Catalogs"
          value={tenant?.limits.max_catalogs ?? "-"}
        />
        <StatCard
          icon={<Key className="h-4 w-4" />}
          title="Max API Keys"
          value={tenant?.limits.max_api_keys ?? "-"}
        />
        <StatCard
          icon={<Activity className="h-4 w-4" />}
          title="Recent Events"
          value={events.length}
        />
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Catalogs</CardTitle>
            <CardDescription>Active catalogs in your tenant</CardDescription>
          </CardHeader>
          <CardContent>
            {catalogs.length === 0 ? (
              <p className="text-sm text-muted-foreground">No catalogs yet.</p>
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
                      <span className="ml-2 text-xs text-muted-foreground">#{c.id}</span>
                    </div>
                    <Badge variant="secondary">{c.engine}</Badge>
                  </Link>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Recent Events</CardTitle>
            <CardDescription>Live catalog events from WebSocket</CardDescription>
          </CardHeader>
          <CardContent>
            {events.length === 0 ? (
              <p className="text-sm text-muted-foreground">No events yet. Events will appear here in real-time.</p>
            ) : (
              <div className="space-y-2 max-h-80 overflow-auto">
                {events.slice(0, 20).map((event, i) => (
                  <div key={i} className="flex items-center gap-2 text-sm border-b pb-2">
                    <Badge variant="outline" className="text-xs shrink-0">
                      {event.type}
                    </Badge>
                    <span className="text-muted-foreground truncate">
                      {event.catalog}/{event.table}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function StatCard({
  icon,
  title,
  value,
}: {
  icon: React.ReactNode
  title: string
  value: number | string
}) {
  return (
    <Card>
      <CardContent className="pt-6">
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground">{icon}</span>
          <span className="text-sm text-muted-foreground">{title}</span>
        </div>
        <p className="mt-2 text-3xl font-bold">{value}</p>
      </CardContent>
    </Card>
  )
}
