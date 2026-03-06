import { NavLink, useNavigate } from "react-router"
import {
  LayoutDashboard,
  Network,
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
  User,
  LogOut,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { LogoIcon } from "@/components/logo"
import { useAuthStore } from "@/stores/auth"
import { ScrollArea } from "@/components/ui/scroll-area"

const navItems = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/cluster", icon: Network, label: "Cluster" },
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
  const { username, accountName, logout } = useAuthStore()
  const navigate = useNavigate()

  const handleLogout = () => {
    logout()
    navigate("/login")
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-4 py-4 flex items-center gap-2">
        <LogoIcon className="h-7 w-7" />
        <span className="text-lg font-semibold tracking-tight">bisque</span>
      </div>
      <ScrollArea className="flex-1">
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
      <div className="border-t px-2 py-2">
        <button
          className="flex items-center gap-3 w-full rounded-md px-3 py-2 text-sm font-medium text-muted-foreground hover:bg-accent/50 transition-colors"
        >
          <User className="h-4 w-4 shrink-0" />
          <span className="truncate flex-1 text-left">{username || accountName || "User"}</span>
          <LogOut className="h-4 w-4 shrink-0" onClick={handleLogout} />
        </button>
      </div>
    </div>
  )
}
