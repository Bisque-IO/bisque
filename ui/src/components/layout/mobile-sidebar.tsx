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
  { to: "/observability", icon: Waypoints, label: "Overview" },
  { to: "/traces", icon: Activity, label: "Traces" },
  { to: "/metrics", icon: BarChart3, label: "Metrics" },
  { to: "/logs", icon: FileText, label: "Logs" },
  { to: "/api-keys", icon: Key, label: "API Keys" },
  { to: "/settings", icon: Settings, label: "Settings" },
]

export function MobileSidebar() {
  return (
    <ScrollArea className="h-full py-4">
      <div className="px-4 pb-4">
        <span className="text-lg font-semibold tracking-tight">bisque</span>
      </div>
      <nav className="space-y-1 px-2">
        {navItems.map((item) => {
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
                    ? "bg-accent text-accent-foreground"
                    : "text-muted-foreground hover:bg-accent/50"
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
  )
}
