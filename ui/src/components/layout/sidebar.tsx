import { NavLink } from "react-router"
import {
  LayoutDashboard,
  Users,
  Database,
  Table2,
  Terminal,
  Activity,
  BarChart3,
  FileText,
  Key,
  Settings,
  Waypoints,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { ScrollArea } from "@/components/ui/scroll-area"

const navItems = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/tenants", icon: Users, label: "Tenants" },
  { to: "/catalogs", icon: Database, label: "Catalogs" },
  { to: "/tables", icon: Table2, label: "Tables" },
  { to: "/query", icon: Terminal, label: "Query" },
  { divider: true, label: "Observability" },
  { to: "/observability", icon: Waypoints, label: "Overview" },
  { to: "/traces", icon: Activity, label: "Traces" },
  { to: "/metrics", icon: BarChart3, label: "Metrics" },
  { to: "/logs", icon: FileText, label: "Logs" },
  { divider: true, label: "Settings" },
  { to: "/api-keys", icon: Key, label: "API Keys" },
  { to: "/settings", icon: Settings, label: "Settings" },
] as const

export function Sidebar() {
  return (
    <aside className="hidden md:flex w-60 flex-col border-r bg-sidebar text-sidebar-foreground">
      <div className="flex h-14 items-center border-b px-4">
        <span className="text-lg font-semibold tracking-tight">bisque</span>
      </div>
      <ScrollArea className="flex-1 py-2">
        <nav className="space-y-1 px-2">
          {navItems.map((item, i) => {
            if ("divider" in item) {
              return (
                <div key={i} className="pt-4 pb-1 px-3">
                  <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                    {item.label}
                  </span>
                </div>
              )
            }
            const Icon = item.icon
            return (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.to === "/"}
                className={({ isActive }) =>
                  cn(
                    "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                    isActive
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
                      : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
                  )
                }
              >
                <Icon className="h-4 w-4 shrink-0" />
                {item.label}
              </NavLink>
            )
          })}
        </nav>
      </ScrollArea>
    </aside>
  )
}
