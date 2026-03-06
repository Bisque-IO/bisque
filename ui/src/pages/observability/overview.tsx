import { useEffect, useState } from "react"
import { promApi } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Activity, BarChart3, FileText, Waypoints } from "lucide-react"

interface MetricValue {
  label: string
  value: string
}

export function ObservabilityOverviewPage() {
  const [metrics, setMetrics] = useState<MetricValue[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    // Try to fetch some basic metrics
    const fetchMetrics = async () => {
      const results: MetricValue[] = []
      try {
        const labels = await promApi.labels()
        if (labels.data && Array.isArray(labels.data)) {
          results.push({ label: "Metric Labels", value: String(labels.data.length) })
        }
      } catch {
        // Metrics may not be available
      }
      setMetrics(results)
      setLoading(false)
    }
    fetchMetrics()
  }, [])

  return (
    <div className="space-y-6">


      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <OverviewCard
          icon={<Activity className="h-5 w-5" />}
          title="Traces"
          description="Distributed trace search and analysis"
          link="/traces"
        />
        <OverviewCard
          icon={<BarChart3 className="h-5 w-5" />}
          title="Metrics"
          description="PromQL query and visualization"
          link="/metrics"
        />
        <OverviewCard
          icon={<FileText className="h-5 w-5" />}
          title="Logs"
          description="LogQL query and log viewer"
          link="/logs"
        />
        <OverviewCard
          icon={<Waypoints className="h-5 w-5" />}
          title="OTLP Ingest"
          description="OpenTelemetry data ingestion"
          link={undefined}
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>System Metrics</CardTitle>
          <CardDescription>
            {loading ? "Loading..." : `${metrics.length} metric sources detected`}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {metrics.length === 0 && !loading ? (
            <p className="text-sm text-muted-foreground">
              No metrics data available yet. Send OTLP or Prometheus data to see metrics here.
            </p>
          ) : (
            <div className="grid gap-4 md:grid-cols-2">
              {metrics.map((m) => (
                <div key={m.label} className="flex justify-between border-b pb-2">
                  <span className="text-sm text-muted-foreground">{m.label}</span>
                  <span className="font-medium">{m.value}</span>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Supported Protocols</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            <p>OTLP gRPC (port 4317) — traces, metrics, logs</p>
            <p>OTLP HTTP (POST /v1/traces, /v1/metrics, /v1/logs)</p>
            <p>Prometheus remote write (POST /api/v1/push)</p>
            <p>Prometheus remote read (POST /api/v1/read)</p>
            <p>Loki push (POST /loki/api/v1/push)</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Query APIs</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            <p>Tempo — GET /api/search, /api/traces/&#123;id&#125;</p>
            <p>PromQL — GET/POST /api/v1/query, /api/v1/query_range</p>
            <p>LogQL — GET /loki/api/v1/query, /loki/api/v1/query_range</p>
            <p>Flight SQL — gRPC port 50051</p>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function OverviewCard({
  icon,
  title,
  description,
  link,
}: {
  icon: React.ReactNode
  title: string
  description: string
  link: string | undefined
}) {
  const content = (
    <Card className={link ? "hover:border-primary/50 transition-colors" : ""}>
      <CardContent className="pt-6">
        <div className="flex items-center gap-3">
          <span className="text-muted-foreground">{icon}</span>
          <div>
            <p className="font-medium">{title}</p>
            <p className="text-xs text-muted-foreground">{description}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  )

  if (link) {
    return <a href={link}>{content}</a>
  }
  return content
}
