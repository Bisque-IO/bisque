import { useEffect, useState } from "react"
import { useParams, Link } from "react-router"
import { s3Api } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Table2 } from "lucide-react"

interface CatalogInfo {
  tables: Record<string, TableInfo>
}

interface TableInfo {
  active_version: number
  sealed_version: number
  schema?: unknown
}

export function CatalogDetailPage() {
  const { catalogName } = useParams()
  const [catalog, setCatalog] = useState<CatalogInfo | null>(null)
  const [error, setError] = useState("")

  useEffect(() => {
    if (!catalogName) return
    s3Api
      .getCatalog(catalogName)
      .then((data) => setCatalog(data as unknown as CatalogInfo))
      .catch(() => setError("Failed to load catalog"))
  }, [catalogName])

  if (error) {
    return <p className="text-destructive">{error}</p>
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">{catalogName}</h1>

      {!catalog ? (
        <p className="text-muted-foreground">Loading catalog...</p>
      ) : (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {Object.entries(catalog.tables ?? {}).map(([tableName, info]) => (
              <Link key={tableName} to={`/tables/${catalogName}/${tableName}`}>
                <Card className="hover:border-primary/50 transition-colors">
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-base">
                      <Table2 className="h-4 w-4" />
                      {tableName}
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-1">
                    <div className="flex gap-2">
                      <Badge variant="outline">Active v{info.active_version}</Badge>
                      <Badge variant="secondary">Sealed v{info.sealed_version}</Badge>
                    </div>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>

          {Object.keys(catalog.tables ?? {}).length === 0 && (
            <Card>
              <CardContent className="py-12 text-center">
                <Table2 className="mx-auto h-12 w-12 text-muted-foreground/50" />
                <p className="mt-4 text-muted-foreground">No tables in this catalog yet.</p>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  )
}
