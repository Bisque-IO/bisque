import { useState } from "react"
import { Link } from "react-router"
import { tempoApi, type TempoTraceResult } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Search, Loader2 } from "lucide-react"

const TIME_RANGES = [
  { label: "Last 15m", value: "15m" },
  { label: "Last 1h", value: "1h" },
  { label: "Last 6h", value: "6h" },
  { label: "Last 24h", value: "24h" },
  { label: "Last 7d", value: "7d" },
]

function timeRangeToSeconds(range: string): number {
  const map: Record<string, number> = {
    "15m": 900,
    "1h": 3600,
    "6h": 21600,
    "24h": 86400,
    "7d": 604800,
  }
  return map[range] ?? 3600
}

export function TracesPage() {
  const [serviceName, setServiceName] = useState("")
  const [tags, setTags] = useState("")
  const [timeRange, setTimeRange] = useState("1h")
  const [minDuration, setMinDuration] = useState("")
  const [maxDuration, setMaxDuration] = useState("")
  const [traces, setTraces] = useState<TempoTraceResult[]>([])
  const [searching, setSearching] = useState(false)
  const [searched, setSearched] = useState(false)

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault()
    setSearching(true)
    setSearched(true)

    try {
      const now = Math.floor(Date.now() / 1000)
      const start = now - timeRangeToSeconds(timeRange)
      const params: Record<string, string> = {
        start: String(start),
        end: String(now),
        limit: "20",
      }
      if (serviceName) params["tags"] = `service.name="${serviceName}"`
      if (tags) params["tags"] = params["tags"] ? `${params["tags"]} ${tags}` : tags
      if (minDuration) params["minDuration"] = minDuration
      if (maxDuration) params["maxDuration"] = maxDuration

      const result = await tempoApi.searchTraces(params)
      setTraces(result.traces ?? [])
    } catch {
      setTraces([])
    } finally {
      setSearching(false)
    }
  }

  return (
    <div className="space-y-6">


      <Card>
        <CardHeader>
          <CardTitle>Search Traces</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSearch} className="grid gap-4 md:grid-cols-3">
            <div className="space-y-2">
              <Label>Service Name</Label>
              <Input
                value={serviceName}
                onChange={(e) => setServiceName(e.target.value)}
                placeholder="my-service"
              />
            </div>
            <div className="space-y-2">
              <Label>Tags</Label>
              <Input
                value={tags}
                onChange={(e) => setTags(e.target.value)}
                placeholder='http.status_code=200'
              />
            </div>
            <div className="space-y-2">
              <Label>Time Range</Label>
              <Select value={timeRange} onValueChange={setTimeRange}>
                <SelectTrigger>
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
              <Label>Min Duration</Label>
              <Input
                value={minDuration}
                onChange={(e) => setMinDuration(e.target.value)}
                placeholder="100ms"
              />
            </div>
            <div className="space-y-2">
              <Label>Max Duration</Label>
              <Input
                value={maxDuration}
                onChange={(e) => setMaxDuration(e.target.value)}
                placeholder="5s"
              />
            </div>
            <div className="flex items-end">
              <Button type="submit" disabled={searching} className="w-full">
                {searching ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Search className="h-4 w-4 mr-2" />
                )}
                Search
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>

      {searched && (
        <Card>
          <CardHeader>
            <CardTitle>Results ({traces.length})</CardTitle>
          </CardHeader>
          <CardContent className="overflow-auto">
            {traces.length === 0 ? (
              <p className="text-sm text-muted-foreground">No traces found.</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Trace ID</TableHead>
                    <TableHead>Service</TableHead>
                    <TableHead>Operation</TableHead>
                    <TableHead>Duration</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {traces.map((t) => (
                    <TableRow key={t.traceID}>
                      <TableCell>
                        <Link
                          to={`/traces/${t.traceID}`}
                          className="font-mono text-xs text-primary hover:underline"
                        >
                          {t.traceID.slice(0, 16)}...
                        </Link>
                      </TableCell>
                      <TableCell className="text-sm">{t.rootServiceName}</TableCell>
                      <TableCell className="text-sm">{t.rootTraceName}</TableCell>
                      <TableCell className="text-sm">{t.durationMs}ms</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
