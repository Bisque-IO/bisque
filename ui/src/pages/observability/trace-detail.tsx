import { useEffect, useState } from "react"
import { useParams } from "react-router"
import { tempoApi } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"

interface Span {
  traceId: string
  spanId: string
  parentSpanId?: string
  operationName: string
  serviceName: string
  startTimeUnixNano: number
  durationNano: number
  status?: { code: number }
  attributes?: Record<string, unknown>
}

export function TraceDetailPage() {
  const { traceId } = useParams()
  const [spans, setSpans] = useState<Span[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")

  useEffect(() => {
    if (!traceId) return

    tempoApi
      .getTrace(traceId)
      .then((data) => {
        // Parse OTLP trace response into flat spans
        const parsed: Span[] = []
        for (const batch of data.batches ?? []) {
          const b = batch as {
            resource?: { attributes?: { key: string; value: { stringValue?: string } }[] }
            scopeSpans?: {
              spans?: {
                traceId: string
                spanId: string
                parentSpanId?: string
                name: string
                startTimeUnixNano: string
                endTimeUnixNano: string
                status?: { code: number }
                attributes?: { key: string; value: unknown }[]
              }[]
            }[]
          }
          const serviceName =
            b.resource?.attributes?.find((a) => a.key === "service.name")?.value
              ?.stringValue ?? "unknown"

          for (const scope of b.scopeSpans ?? []) {
            for (const span of scope.spans ?? []) {
              const startNano = Number(span.startTimeUnixNano)
              const endNano = Number(span.endTimeUnixNano)
              parsed.push({
                traceId: span.traceId,
                spanId: span.spanId,
                parentSpanId: span.parentSpanId,
                operationName: span.name,
                serviceName,
                startTimeUnixNano: startNano,
                durationNano: endNano - startNano,
                status: span.status,
              })
            }
          }
        }
        parsed.sort((a, b) => a.startTimeUnixNano - b.startTimeUnixNano)
        setSpans(parsed)
      })
      .catch(() => setError("Failed to load trace"))
      .finally(() => setLoading(false))
  }, [traceId])

  if (loading) return <p className="text-muted-foreground">Loading trace...</p>
  if (error) return <p className="text-destructive">{error}</p>

  const traceStart = spans.length > 0 ? spans[0].startTimeUnixNano : 0
  const traceEnd = spans.length > 0
    ? Math.max(...spans.map((s) => s.startTimeUnixNano + s.durationNano))
    : 0
  const traceDuration = traceEnd - traceStart

  return (
    <div className="space-y-6">
      <p className="font-mono text-sm text-muted-foreground">{traceId}</p>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Spans</p>
            <p className="text-2xl font-bold">{spans.length}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Duration</p>
            <p className="text-2xl font-bold">{(traceDuration / 1e6).toFixed(2)}ms</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Services</p>
            <p className="text-2xl font-bold">
              {new Set(spans.map((s) => s.serviceName)).size}
            </p>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Span Waterfall</CardTitle>
        </CardHeader>
        <CardContent>
          <ScrollArea className="max-h-[600px]">
            <div className="space-y-1">
              {spans.map((span) => {
                const offset =
                  traceDuration > 0
                    ? ((span.startTimeUnixNano - traceStart) / traceDuration) * 100
                    : 0
                const width =
                  traceDuration > 0
                    ? Math.max((span.durationNano / traceDuration) * 100, 0.5)
                    : 100
                const isError = span.status?.code === 2

                return (
                  <div key={span.spanId} className="flex items-center gap-2 group">
                    <div className="w-48 shrink-0 text-right pr-2">
                      <p className="text-xs font-medium truncate">{span.operationName}</p>
                      <p className="text-xs text-muted-foreground truncate">
                        {span.serviceName}
                      </p>
                    </div>
                    <div className="flex-1 h-6 bg-muted rounded relative">
                      <div
                        className={`absolute h-full rounded ${
                          isError ? "bg-destructive/70" : "bg-primary/70"
                        }`}
                        style={{ left: `${offset}%`, width: `${width}%` }}
                      />
                    </div>
                    <span className="w-20 text-xs text-muted-foreground shrink-0">
                      {(span.durationNano / 1e6).toFixed(1)}ms
                    </span>
                  </div>
                )
              })}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>
    </div>
  )
}
