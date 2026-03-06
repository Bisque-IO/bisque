import { useState, useCallback } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Textarea } from "@/components/ui/textarea"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Play, Loader2 } from "lucide-react"

export function QueryPage() {
  const [sql, setSql] = useState("SELECT 1")
  const [results, setResults] = useState<Record<string, unknown>[] | null>(null)
  const [columns] = useState<string[]>([])
  const [error, setError] = useState("")
  const [running, setRunning] = useState(false)

  const runQuery = useCallback(async () => {
    if (!sql.trim()) return
    setRunning(true)
    setError("")
    setResults(null)

    try {
      // For now, show a placeholder since we need a query HTTP endpoint
      // In the future, this will connect to Flight SQL or an HTTP query endpoint
      setError(
        "SQL query execution requires a Flight SQL connection or HTTP query endpoint. " +
        "This feature will be available once the bisque server exposes an HTTP SQL query API."
      )
    } finally {
      setRunning(false)
    }
  }, [sql])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault()
      runQuery()
    }
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">SQL Query</h1>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            Query Editor
            <Button onClick={runQuery} disabled={running} size="sm">
              {running ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Play className="h-4 w-4 mr-2" />
              )}
              Run (Ctrl+Enter)
            </Button>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            onKeyDown={handleKeyDown}
            className="font-mono text-sm min-h-[200px] resize-y"
            placeholder="SELECT * FROM my_table LIMIT 100"
          />
        </CardContent>
      </Card>

      {error && (
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">{error}</p>
          </CardContent>
        </Card>
      )}

      {results && columns.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Results ({results.length} rows)</CardTitle>
          </CardHeader>
          <CardContent className="overflow-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  {columns.map((col) => (
                    <TableHead key={col}>{col}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {results.map((row, i) => (
                  <TableRow key={i}>
                    {columns.map((col) => (
                      <TableCell key={col} className="font-mono text-xs">
                        {String(row[col] ?? "")}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
