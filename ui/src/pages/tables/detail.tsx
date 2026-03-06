import { useEffect, useState } from "react"
import { useParams } from "react-router"
import { s3Api } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

interface TableInfo {
  active_version: number
  sealed_version: number
  schema?: SchemaField[]
}

interface SchemaField {
  name: string
  type: string
  nullable: boolean
}

export function TableDetailPage() {
  const { catalogName, tableName } = useParams()
  const [tableInfo, setTableInfo] = useState<TableInfo | null>(null)
  const [files, setFiles] = useState<string[]>([])

  useEffect(() => {
    if (!catalogName || !tableName) return

    s3Api
      .getCatalog(catalogName)
      .then((data) => {
        const tables = (data as { tables?: Record<string, TableInfo> }).tables ?? {}
        setTableInfo(tables[tableName] ?? null)
      })
      .catch(() => {})

    // List segment files for this table
    s3Api
      .listObjects(catalogName, `${tableName}/`)
      .then((xml) => {
        const parser = new DOMParser()
        const doc = parser.parseFromString(xml, "text/xml")
        const keys = Array.from(doc.querySelectorAll("Key")).map(
          (el) => el.textContent ?? ""
        )
        setFiles(keys)
      })
      .catch(() => {})
  }, [catalogName, tableName])

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <h1 className="text-2xl font-bold">{tableName}</h1>
        <Badge variant="outline">{catalogName}</Badge>
      </div>

      {tableInfo && (
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Active Version</p>
              <p className="text-2xl font-bold">{tableInfo.active_version}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-sm text-muted-foreground">Sealed Version</p>
              <p className="text-2xl font-bold">{tableInfo.sealed_version}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {tableInfo?.schema && tableInfo.schema.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Schema</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Column</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Nullable</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tableInfo.schema.map((field) => (
                  <TableRow key={field.name}>
                    <TableCell className="font-mono text-sm">{field.name}</TableCell>
                    <TableCell className="font-mono text-sm">{field.type}</TableCell>
                    <TableCell>
                      <Badge variant={field.nullable ? "secondary" : "outline"}>
                        {field.nullable ? "yes" : "no"}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Segment Files ({files.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {files.length === 0 ? (
            <p className="text-sm text-muted-foreground">No segment files found.</p>
          ) : (
            <div className="space-y-1 max-h-80 overflow-auto">
              {files.map((f) => (
                <p key={f} className="font-mono text-xs text-muted-foreground">
                  {f}
                </p>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
