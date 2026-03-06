import { useState } from "react"
import { promApi, type PromResult } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Search, Loader2 } from "lucide-react"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts"

const TIME_RANGES = [
  { label: "Last 15m", seconds: 900, step: "15s" },
  { label: "Last 1h", seconds: 3600, step: "60s" },
  { label: "Last 6h", seconds: 21600, step: "300s" },
  { label: "Last 24h", seconds: 86400, step: "600s" },
  { label: "Last 7d", seconds: 604800, step: "3600s" },
]

const COLORS = [
  "hsl(220, 70%, 50%)",
  "hsl(160, 60%, 45%)",
  "hsl(30, 80%, 55%)",
  "hsl(280, 60%, 55%)",
  "hsl(0, 70%, 50%)",
]

interface ChartPoint {
  time: number
  [key: string]: number
}

export function MetricsPage() {
  const [query, setQuery] = useState("")
  const [timeRange, setTimeRange] = useState("1h")
  const [chartData, setChartData] = useState<ChartPoint[]>([])
  const [seriesNames, setSeriesNames] = useState<string[]>([])
  const [searching, setSearching] = useState(false)
  const [searched, setSearched] = useState(false)
  const [error, setError] = useState("")

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!query.trim()) return
    setSearching(true)
    setSearched(true)
    setError("")

    try {
      const range = TIME_RANGES.find((r) => r.label.includes(timeRange)) ?? TIME_RANGES[1]
      const now = Math.floor(Date.now() / 1000)
      const start = now - range.seconds

      const result = await promApi.queryRange(
        query,
        String(start),
        String(now),
        range.step
      )

      if (result.status !== "success" || !result.data.result?.length) {
        setChartData([])
        setSeriesNames([])
        return
      }

      // Build chart data from matrix results
      const series = result.data.result
      const names = series.map((s: PromResult, i: number) => {
        const labels = Object.entries(s.metric)
          .filter(([k]) => k !== "__name__")
          .map(([k, v]) => `${k}="${v}"`)
          .join(", ")
        return labels || `series_${i}`
      })
      setSeriesNames(names)

      // Merge all timestamps
      const timeMap = new Map<number, ChartPoint>()
      series.forEach((s: PromResult, i: number) => {
        for (const [ts, val] of s.values ?? []) {
          if (!timeMap.has(ts)) {
            timeMap.set(ts, { time: ts })
          }
          timeMap.get(ts)![names[i]] = parseFloat(val)
        }
      })

      const data = Array.from(timeMap.values()).sort((a, b) => a.time - b.time)
      setChartData(data)
    } catch {
      setError("Query failed")
      setChartData([])
      setSeriesNames([])
    } finally {
      setSearching(false)
    }
  }

  return (
    <div className="space-y-6">


      <Card>
        <CardHeader>
          <CardTitle>PromQL Query</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSearch} className="flex gap-4 items-end">
            <div className="flex-1 space-y-2">
              <Label>Query</Label>
              <Input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder='rate(http_requests_total[5m])'
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
                    <SelectItem key={r.label} value={r.label.split(" ")[1]}>
                      {r.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button type="submit" disabled={searching}>
              {searching ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Search className="h-4 w-4 mr-2" />
              )}
              Query
            </Button>
          </form>
        </CardContent>
      </Card>

      {error && (
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-destructive">{error}</p>
          </CardContent>
        </Card>
      )}

      {searched && chartData.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Results</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis
                    dataKey="time"
                    tickFormatter={(ts: number) =>
                      new Date(ts * 1000).toLocaleTimeString()
                    }
                    className="text-xs"
                  />
                  <YAxis className="text-xs" />
                  <Tooltip
                    labelFormatter={(ts) =>
                      new Date(Number(ts) * 1000).toLocaleString()
                    }
                  />
                  <Legend />
                  {seriesNames.map((name, i) => (
                    <Line
                      key={name}
                      type="monotone"
                      dataKey={name}
                      stroke={COLORS[i % COLORS.length]}
                      dot={false}
                      strokeWidth={1.5}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      )}

      {searched && chartData.length === 0 && !error && (
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted-foreground">No data found for this query and time range.</p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
