import { useCallback, useState } from "react"
import { MOCK, type OpType, type OpTier, type OpStatus } from "@/lib/api"
import { wsClient } from "@/lib/ws"
import { useOperationsStore } from "@/stores/operations"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { RefreshCw, X, Loader2, CheckCircle2, XCircle, Clock, Ban } from "lucide-react"
import { toast } from "sonner"

function statusBadge(status: OpStatus) {
  switch (status) {
    case "running":
      return <Badge className="bg-blue-500/15 text-blue-600 border-blue-500/30"><Loader2 className="h-3 w-3 mr-1 animate-spin" />Running</Badge>
    case "queued":
      return <Badge variant="secondary"><Clock className="h-3 w-3 mr-1" />Queued</Badge>
    case "done":
      return <Badge className="bg-green-500/15 text-green-600 border-green-500/30"><CheckCircle2 className="h-3 w-3 mr-1" />Done</Badge>
    case "failed":
      return <Badge variant="destructive"><XCircle className="h-3 w-3 mr-1" />Failed</Badge>
    case "cancelled":
      return <Badge variant="outline"><Ban className="h-3 w-3 mr-1" />Cancelled</Badge>
  }
}

function typeBadge(opType: OpType) {
  switch (opType) {
    case "reindex":
      return <Badge variant="outline">Reindex</Badge>
    case "compact":
      return <Badge variant="secondary">Compact</Badge>
    case "flush":
      return <Badge className="bg-purple-500/15 text-purple-600 border-purple-500/30">Flush</Badge>
  }
}

const tierColors: Record<OpTier, string> = {
  hot: "bg-orange-500",
  warm: "bg-yellow-500",
  cold: "bg-blue-500",
}

function tierBadge(tier: OpTier) {
  return (
    <div className="flex items-center gap-1.5">
      <span className={`h-2 w-2 rounded-full ${tierColors[tier]}`} />
      <span className="text-xs font-medium capitalize">{tier}</span>
    </div>
  )
}

function ProgressBar({ progress }: { progress: number }) {
  const pct = Math.round(progress * 100)
  return (
    <div className="flex items-center gap-2 min-w-[100px]">
      <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden">
        <div
          className="h-full rounded-full bg-primary transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs font-mono text-muted-foreground w-8 text-right">{pct}%</span>
    </div>
  )
}

function formatDuration(startedAt?: string, finishedAt?: string): string {
  if (!startedAt) return "—"
  const start = new Date(startedAt).getTime()
  const end = finishedAt ? new Date(finishedAt).getTime() : Date.now()
  const seconds = Math.round((end - start) / 1000)
  if (seconds < 60) return `${seconds}s`
  const mins = Math.floor(seconds / 60)
  const secs = seconds % 60
  return `${mins}m ${secs}s`
}

function formatTime(iso?: string): string {
  if (!iso) return "—"
  return new Date(iso).toLocaleTimeString()
}

export function OperationsPage() {
  // Operations are populated via the unified WebSocket (OperationsSnapshot + OperationUpdate push).
  const operations = useOperationsStore((s) => s.operations)
  const [filterType, setFilterType] = useState<string>("all")
  const [filterTier, setFilterTier] = useState<string>("all")
  const [filterStatus, setFilterStatus] = useState<string>("all")
  const [refreshing, setRefreshing] = useState(false)

  const handleRefresh = useCallback(async () => {
    setRefreshing(true)
    try {
      if (!MOCK && wsClient.connected) {
        const ops = await wsClient.listOperations({
          type: filterType !== "all" ? filterType : undefined,
          tier: filterTier !== "all" ? filterTier : undefined,
          status: filterStatus !== "all" ? filterStatus : undefined,
        })
        useOperationsStore.getState().setOperations(ops)
      }
    } catch {
      // ignore
    } finally {
      setRefreshing(false)
    }
  }, [filterType, filterTier, filterStatus])

  const handleCancel = async (opId: string) => {
    try {
      if (MOCK) {
        toast.success("Operation cancelled")
        return
      }
      await wsClient.cancelOperation(opId)
      toast.success("Operation cancelled")
    } catch {
      toast.error("Failed to cancel operation")
    }
  }

  // Apply client-side filters (WS push delivers all ops)
  const filtered = operations.filter((op) => {
    if (filterType !== "all" && op.op_type !== filterType) return false
    if (filterTier !== "all" && op.tier !== filterTier) return false
    if (filterStatus !== "all" && op.status !== filterStatus) return false
    return true
  })

  const running = operations.filter((o) => o.status === "running").length
  const queued = operations.filter((o) => o.status === "queued").length
  const done = operations.filter((o) => o.status === "done").length
  const failed = operations.filter((o) => o.status === "failed").length

  return (
    <div className="flex flex-col -m-6 h-[calc(100vh-3.5rem)]">
      {/* Compact summary stats */}
      <div className="grid grid-cols-4 gap-3 px-4 pt-4 pb-3">
        <div className="flex items-center gap-2 rounded-md border bg-muted/30 px-3 py-2">
          <span className="text-xs text-muted-foreground">Running</span>
          <span className="ml-auto text-sm font-semibold">{running}</span>
        </div>
        <div className="flex items-center gap-2 rounded-md border bg-muted/30 px-3 py-2">
          <span className="text-xs text-muted-foreground">Queued</span>
          <span className="ml-auto text-sm font-semibold">{queued}</span>
        </div>
        <div className="flex items-center gap-2 rounded-md border bg-muted/30 px-3 py-2">
          <span className="text-xs text-muted-foreground">Completed</span>
          <span className="ml-auto text-sm font-semibold">{done}</span>
        </div>
        <div className="flex items-center gap-2 rounded-md border bg-muted/30 px-3 py-2">
          <span className="text-xs text-muted-foreground">Failed</span>
          <span className="ml-auto text-sm font-semibold">{failed}</span>
        </div>
      </div>

      {/* Filters toolbar */}
      <div className="flex items-center justify-between px-4 pb-2">
        <h2 className="text-sm font-medium">Operations</h2>
        <div className="flex items-center gap-2">
          <Select value={filterType} onValueChange={setFilterType}>
            <SelectTrigger className="w-[130px] h-7 text-xs">
              <SelectValue placeholder="Type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="reindex">Reindex</SelectItem>
              <SelectItem value="compact">Compact</SelectItem>
              <SelectItem value="flush">Flush</SelectItem>
            </SelectContent>
          </Select>
          <Select value={filterTier} onValueChange={setFilterTier}>
            <SelectTrigger className="w-[110px] h-7 text-xs">
              <SelectValue placeholder="Tier" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Tiers</SelectItem>
              <SelectItem value="hot">Hot</SelectItem>
              <SelectItem value="warm">Warm</SelectItem>
              <SelectItem value="cold">Cold</SelectItem>
            </SelectContent>
          </Select>
          <Select value={filterStatus} onValueChange={setFilterStatus}>
            <SelectTrigger className="w-[130px] h-7 text-xs">
              <SelectValue placeholder="Status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Status</SelectItem>
              <SelectItem value="running">Running</SelectItem>
              <SelectItem value="queued">Queued</SelectItem>
              <SelectItem value="done">Done</SelectItem>
              <SelectItem value="failed">Failed</SelectItem>
              <SelectItem value="cancelled">Cancelled</SelectItem>
            </SelectContent>
          </Select>
          <Button size="sm" variant="ghost" className="h-7 w-7 p-0" onClick={handleRefresh} disabled={refreshing}>
            <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? "animate-spin" : ""}`} />
          </Button>
        </div>
      </div>

      {/* Scrollable table area */}
      <div className="flex-1 min-h-0 overflow-auto border-t">
        {filtered.length === 0 ? (
          <p className="text-xs text-muted-foreground py-12 text-center">
            No operations found.
          </p>
        ) : (
          <table className="w-full border-collapse text-xs">
            <thead className="sticky top-0 z-10">
              <tr>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Node</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Type</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Tier</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Tenant</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Catalog</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Engine</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Table</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Status</th>
                <th className="bg-background border-b border-border px-3 py-2 text-left font-medium text-muted-foreground">Progress</th>
                <th className="bg-background border-b border-border px-3 py-2 text-right font-medium text-muted-foreground">Duration</th>
                <th className="bg-background border-b border-border px-3 py-2 text-right font-medium text-muted-foreground">Started</th>
                <th className="bg-background border-b border-border px-3 py-2 w-[40px]" />
              </tr>
            </thead>
            <tbody>
              {filtered.map((op) => (
                <tr
                  key={op.id}
                  className={`border-b border-border/50 hover:bg-accent/30 ${op.status === "failed" ? "bg-destructive/5" : ""}`}
                >
                  <td className="px-3 py-1.5 font-mono text-muted-foreground">{op.node_id}</td>
                  <td className="px-3 py-1.5">{typeBadge(op.op_type)}</td>
                  <td className="px-3 py-1.5">{tierBadge(op.tier)}</td>
                  <td className="px-3 py-1.5 font-mono truncate">{op.tenant}</td>
                  <td className="px-3 py-1.5 font-mono truncate">{op.catalog}</td>
                  <td className="px-3 py-1.5 text-muted-foreground">{op.catalog_type}</td>
                  <td className="px-3 py-1.5 font-mono truncate">{op.table}</td>
                  <td className="px-3 py-1.5">{statusBadge(op.status)}</td>
                  <td className="px-3 py-1.5">
                    {op.status === "running" || op.status === "failed" ? (
                      <ProgressBar progress={op.progress} />
                    ) : op.status === "done" ? (
                      <span className="text-muted-foreground">
                        {op.fragments_done ?? "—"} fragments
                      </span>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono">
                    {formatDuration(op.started_at, op.finished_at)}
                  </td>
                  <td className="px-3 py-1.5 text-right text-muted-foreground">
                    {formatTime(op.started_at)}
                  </td>
                  <td className="px-3 py-1.5">
                    {op.status === "queued" && (
                      <Button
                        size="icon"
                        variant="ghost"
                        className="h-6 w-6"
                        onClick={() => handleCancel(op.id)}
                        title="Cancel"
                      >
                        <X className="h-3 w-3" />
                      </Button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        {/* Error details for failed ops */}
        {filtered.some((o) => o.status === "failed" && o.error) && (
          <div className="p-3 space-y-2 border-t">
            {filtered
              .filter((o) => o.status === "failed" && o.error)
              .map((o) => (
                <div
                  key={o.id}
                  className="rounded-md border border-destructive/30 bg-destructive/5 p-2 text-xs"
                >
                  <span className="font-medium text-destructive">
                    {o.catalog}/{o.table}:
                  </span>{" "}
                  <span className="text-muted-foreground">{o.error}</span>
                </div>
              ))}
          </div>
        )}
      </div>
    </div>
  )
}
