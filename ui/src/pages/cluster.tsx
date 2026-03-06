import { useState, useEffect } from "react"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import { clusterApi, type ClusterNode, type ClusterStatus } from "@/lib/api"
import {
  Server,
  Crown,
  GitBranch,
  Table2,
} from "lucide-react"
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Cell,
} from "recharts"

export function ClusterPage() {
  const [status, setStatus] = useState<ClusterStatus | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    clusterApi
      .getStatus()
      .then(setStatus)
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        Loading cluster status...
      </div>
    )
  }

  if (!status) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        Unable to load cluster status.
      </div>
    )
  }

  const leader = status.nodes.find((n) => n.raft_role === "leader")
  const sortedNodes = [...status.nodes].sort((a, b) =>
    a.raft_role === "leader" ? -1 : b.raft_role === "leader" ? 1 : a.node_id - b.node_id,
  )

  const replicationData = sortedNodes.map((n) => ({
    name: `Node ${n.node_id}`,
    applied: n.raft_applied_index,
    commit: n.raft_commit_index,
    lag: n.raft_commit_index - n.raft_applied_index,
  }))

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatCard
          icon={<Server className="h-4 w-4 text-muted-foreground" />}
          title="Nodes"
          value={status.nodes.length}
        />
        <StatCard
          icon={<Crown className="h-4 w-4 text-muted-foreground" />}
          title="Leader"
          value={leader ? `Node ${leader.node_id}` : "None"}
          subtitle={leader?.address}
        />
        <StatCard
          icon={<GitBranch className="h-4 w-4 text-muted-foreground" />}
          title="Raft Groups"
          value={status.total_raft_groups}
        />
        <StatCard
          icon={<Table2 className="h-4 w-4 text-muted-foreground" />}
          title="Tables"
          value={status.total_tables}
          subtitle={`${status.total_catalogs} catalogs`}
        />
      </div>

      {/* Node cards */}
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {sortedNodes.map((node) => (
          <NodeCard key={node.node_id} node={node} />
        ))}
      </div>

      {/* Raft replication chart */}
      <Card>
        <CardHeader>
          <CardTitle>Raft Replication</CardTitle>
          <CardDescription>
            Applied index per node relative to commit index
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={replicationData}
                layout="vertical"
                margin={{ left: 10, right: 20, top: 5, bottom: 5 }}
              >
                <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                <XAxis type="number" className="text-xs" />
                <YAxis type="category" dataKey="name" width={70} className="text-xs" />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "hsl(var(--popover))",
                    border: "1px solid hsl(var(--border))",
                    borderRadius: "6px",
                    fontSize: "12px",
                  }}
                  labelStyle={{ color: "hsl(var(--popover-foreground))" }}
                />
                <Legend wrapperStyle={{ fontSize: "12px" }} />
                <Bar dataKey="applied" name="Applied Index" fill="hsl(220, 70%, 50%)" radius={[0, 2, 2, 0]}>
                  {replicationData.map((entry, i) => (
                    <Cell
                      key={i}
                      fill={entry.lag === 0 ? "hsl(160, 60%, 45%)" : "hsl(220, 70%, 50%)"}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          {replicationData.some((d) => d.lag > 0) && (
            <div className="mt-3 flex gap-4 text-xs text-muted-foreground">
              {replicationData
                .filter((d) => d.lag > 0)
                .map((d) => (
                  <span key={d.name}>
                    {d.name}: <span className="text-yellow-500">{d.lag} behind</span>
                  </span>
                ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function StatCard({
  icon,
  title,
  value,
  subtitle,
}: {
  icon: React.ReactNode
  title: string
  value: string | number
  subtitle?: string
}) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">{title}</CardTitle>
        {icon}
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value}</div>
        {subtitle && (
          <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>
        )}
      </CardContent>
    </Card>
  )
}

function NodeCard({ node }: { node: ClusterNode }) {
  const isLeader = node.raft_role === "leader"
  const lag = node.raft_commit_index - node.raft_applied_index
  const memPct = ((node.memory_used_bytes / node.memory_total_bytes) * 100).toFixed(1)

  const roleVariant = {
    leader: "default" as const,
    follower: "secondary" as const,
    candidate: "outline" as const,
    learner: "outline" as const,
  }[node.raft_role]

  return (
    <Card className={cn(isLeader && "border-primary/50")}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg">Node {node.node_id}</CardTitle>
          <Badge variant={roleVariant}>{node.raft_role}</Badge>
        </div>
        <CardDescription className="font-mono text-xs">
          {node.address}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
          <Stat label="Raft Term" value={node.raft_term} />
          <Stat label="Applied Index" value={node.raft_applied_index.toLocaleString()} />
          <Stat label="Commit Index" value={node.raft_commit_index.toLocaleString()} />
          <Stat
            label="Replication Lag"
            value={lag}
            className={lag > 0 ? "text-yellow-500" : "text-green-500"}
          />
          <Stat label="CPU" value={`${node.cpu_usage_pct.toFixed(1)}%`} />
          <Stat label="Memory" value={`${formatBytes(node.memory_used_bytes)} (${memPct}%)`} />
          <Stat label="Requests" value={node.requests_total.toLocaleString()} />
          <Stat label="Uptime" value={formatUptime(node.uptime_seconds)} />
          <Stat label="Version" value={node.version} />
          <Stat label="Catalogs" value={node.catalogs} />
        </div>
      </CardContent>
    </Card>
  )
}

function Stat({
  label,
  value,
  className,
}: {
  label: string
  value: string | number
  className?: string
}) {
  return (
    <div>
      <span className="text-muted-foreground text-xs">{label}</span>
      <p className={cn("font-medium", className)}>{value}</p>
    </div>
  )
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
}

function formatUptime(seconds: number): string {
  const d = Math.floor(seconds / 86400)
  const h = Math.floor((seconds % 86400) / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  if (d > 0) return `${d}d ${h}h`
  if (h > 0) return `${h}h ${m}m`
  return `${m}m`
}
