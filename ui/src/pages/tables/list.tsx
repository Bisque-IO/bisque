import { useEffect, useState } from "react"
import { useAuthStore } from "@/stores/auth"
import { catalogApi, type CatalogEntry } from "@/lib/api"
import { wsClient } from "@/lib/ws"
import { useConnectionStore } from "@/stores/connection"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Table2 } from "lucide-react"
import { Link } from "react-router"

interface TableInfo {
  active_version: number
  sealed_version: number
}

interface CatalogTables {
  catalogName: string
  tables: Record<string, TableInfo>
}

export function TableListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const connState = useConnectionStore((s) => s.state)
  const [catalogs, setCatalogs] = useState<CatalogTables[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!tenantId || connState !== "connected") return

    catalogApi
      .list(tenantId)
      .then(async (entries: CatalogEntry[]) => {
        const results: CatalogTables[] = []
        for (const entry of entries) {
          try {
            const data = await wsClient.getCatalog(entry.name)
            results.push({
              catalogName: entry.name,
              tables: (data as { tables?: Record<string, TableInfo> }).tables ?? {},
            })
          } catch {
            results.push({ catalogName: entry.name, tables: {} })
          }
        }
        setCatalogs(results)
      })
      .catch((err) => console.error("Failed to load tables:", err))
      .finally(() => setLoading(false))
  }, [tenantId, connState])

  if (loading) {
    return <p className="text-muted-foreground">Loading tables...</p>
  }

  const allTables = catalogs.flatMap((c) =>
    Object.entries(c.tables).map(([name, info]) => ({
      catalog: c.catalogName,
      name,
      ...info,
    }))
  )

  return (
    <div className="space-y-6">


      {allTables.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center">
            <Table2 className="mx-auto h-12 w-12 text-muted-foreground/50" />
            <p className="mt-4 text-muted-foreground">No tables found across any catalog.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {allTables.map((t) => (
            <Link key={`${t.catalog}/${t.name}`} to={`/tables/${t.catalog}/${t.name}`}>
              <Card className="hover:border-primary/50 transition-colors">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Table2 className="h-4 w-4" />
                    {t.name}
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  <Badge variant="outline">{t.catalog}</Badge>
                  <div className="flex gap-2 text-xs text-muted-foreground">
                    <span>Active v{t.active_version}</span>
                    <span>Sealed v{t.sealed_version}</span>
                  </div>
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>
      )}
    </div>
  )
}
