import { useState } from "react"
import { lokiApi, type LokiStream } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
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
import { ScrollArea } from "@/components/ui/scroll-area"
import { Search, Loader2 } from "lucide-react"

const TIME_RANGES = [
  { label: "Last 15m", value: "15m", seconds: 900 },
  { label: "Last 1h", value: "1h", seconds: 3600 },
  { label: "Last 6h", value: "6h", seconds: 21600 },
  { label: "Last 24h", value: "24h", seconds: 86400 },
]

interface LogEntry {
  timestamp: string
  line: string
  labels: Record<string, string>
}

export function LogsPage() {
  const [query, setQuery] = useState("")
  const [timeRange, setTimeRange] = useState("1h")
  const [limit, setLimit] = useState("100")
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [searching, setSearching] = useState(false)
  const [searched, setSearched] = useState(false)
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null)

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!query.trim()) return
    setSearching(true)
    setSearched(true)

    try {
      const range = TIME_RANGES.find((r) => r.value === timeRange) ?? TIME_RANGES[1]
      const now = Math.floor(Date.now() / 1000)
      const start = now - range.seconds

      const result = await lokiApi.queryRange(
        query,
        String(start * 1e9),
        String(now * 1e9),
        Number(limit) || 100
      )

      const entries: LogEntry[] = []
      for (const stream of result.data.result ?? []) {
        for (const [ts, line] of (stream as LokiStream).values) {
          entries.push({
            timestamp: ts,
            line,
            labels: (stream as LokiStream).stream,
          })
        }
      }
      // Sort newest first
      entries.sort((a, b) => Number(BigInt(b.timestamp) - BigInt(a.timestamp)))
      setLogs(entries)
    } catch {
      setLogs([])
    } finally {
      setSearching(false)
    }
  }

  const formatTimestamp = (nanos: string) => {
    const ms = Number(BigInt(nanos) / BigInt(1e6))
    return new Date(ms).toLocaleString()
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Logs</h1>

      <Card>
        <CardHeader>
          <CardTitle>LogQL Query</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSearch} className="flex gap-4 items-end flex-wrap">
            <div className="flex-1 min-w-[300px] space-y-2">
              <Label>Query</Label>
              <Input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder='{job="bisque"}'
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <Label>Range</Label>
              <Select value={timeRange} onValueChange={setTimeRange}>
                <SelectTrigger className="w-32">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TIME_RANGES.map((r) => (
                    <SelectItem key={r.value} value={r.value}>
                      {r.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Limit</Label>
              <Input
                value={limit}
                onChange={(e) => setLimit(e.target.value)}
                className="w-20"
                type="number"
              />
            </div>
            <Button type="submit" disabled={searching}>
              {searching ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Search className="h-4 w-4 mr-2" />
              )}
              Search
            </Button>
          </form>
        </CardContent>
      </Card>

      {searched && (
        <Card>
          <CardHeader>
            <CardTitle>Log Lines ({logs.length})</CardTitle>
          </CardHeader>
          <CardContent>
            {logs.length === 0 ? (
              <p className="text-sm text-muted-foreground">No logs found.</p>
            ) : (
              <ScrollArea className="max-h-[600px]">
                <div className="space-y-1">
                  {logs.map((entry, i) => (
                    <div
                      key={i}
                      className="border-b py-1 cursor-pointer hover:bg-accent/30 transition-colors"
                      onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}
                    >
                      <div className="flex gap-2 items-start">
                        <span className="text-xs text-muted-foreground shrink-0 w-44">
                          {formatTimestamp(entry.timestamp)}
                        </span>
                        <span className="text-xs font-mono break-all">{entry.line}</span>
                      </div>
                      {expandedIdx === i && (
                        <div className="mt-1 ml-44 flex flex-wrap gap-1">
                          {Object.entries(entry.labels).map(([k, v]) => (
                            <Badge key={k} variant="outline" className="text-xs">
                              {k}={v}
                            </Badge>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </ScrollArea>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
