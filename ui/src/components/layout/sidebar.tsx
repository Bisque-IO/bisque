import { useState, useCallback } from "react"
import { NavLink, useNavigate } from "react-router"
import {
  LayoutDashboard,
  Network,
  ListChecks,
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
  PanelLeftClose,
  LogOut,
  User,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { LogoIcon } from "@/components/logo"
import { useAuthStore } from "@/stores/auth"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

const navItems = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/cluster", icon: Network, label: "Cluster" },
  { to: "/operations", icon: ListChecks, label: "Operations" },
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

function getInitialCollapsed(): boolean {
  try {
    return localStorage.getItem("bisque_sidebar_collapsed") === "true"
  } catch {
    return false
  }
}

export function Sidebar() {
  const [collapsed, setCollapsed] = useState(getInitialCollapsed)
  const { username, accountName, logout } = useAuthStore()
  const navigate = useNavigate()

  const handleLogout = () => {
    logout()
    navigate("/login")
  }

  const toggle = useCallback(() => {
    setCollapsed((prev) => {
      const next = !prev
      try { localStorage.setItem("bisque_sidebar_collapsed", String(next)) } catch { /* noop */ }
      return next
    })
  }, [])

  return (
    <TooltipProvider delayDuration={0}>
      <aside
        className={cn(
          "hidden md:flex flex-col border-r bg-sidebar text-sidebar-foreground transition-[width] duration-200",
          collapsed ? "w-14" : "w-60",
        )}
      >
        {/* Header */}
        <div className="flex h-14 items-center border-b px-3 justify-between">
          {collapsed ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  onClick={toggle}
                  className="flex items-center justify-center h-9 w-9 mx-auto rounded-md hover:bg-sidebar-accent/50 transition-colors"
                >
                  <LogoIcon className="h-7 w-7" />
                </button>
              </TooltipTrigger>
              <TooltipContent side="right" sideOffset={4}>
                Expand sidebar
              </TooltipContent>
            </Tooltip>
          ) : (
            <>
              <div className="flex items-center gap-2 pl-1">
                <LogoIcon className="h-7 w-7" />
                <span className="text-lg font-semibold tracking-tight translate-y-px">bisque</span>
              </div>
              <Tooltip>
                <TooltipTrigger asChild>
                  <button
                    onClick={toggle}
                    className="flex items-center justify-center h-8 w-8 rounded-md text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground transition-colors"
                  >
                    <PanelLeftClose className="h-4 w-4" />
                  </button>
                </TooltipTrigger>
                <TooltipContent side="right" sideOffset={4}>
                  Collapse sidebar
                </TooltipContent>
              </Tooltip>
            </>
          )}
        </div>

        {/* Navigation */}
        <ScrollArea className="flex-1 py-2">
          <nav className={cn("space-y-1 px-2", collapsed && "flex flex-col items-center px-0")}>
            {navItems.map((item, i) => {
              if ("divider" in item) {
                if (collapsed) {
                  return <div key={i} className="my-2 w-8 border-t" />
                }
                return (
                  <div key={i} className="pt-4 pb-1 px-3">
                    <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                      {item.label}
                    </span>
                  </div>
                )
              }
              const Icon = item.icon
              const link = (
                <NavLink
                  key={item.to}
                  to={item.to}
                  end={item.to === "/"}
                  className={({ isActive }) =>
                    cn(
                      "flex items-center rounded-md text-sm font-medium transition-colors",
                      collapsed
                        ? "justify-center h-9 w-9"
                        : "gap-3 px-3 py-2",
                      isActive
                        ? "bg-sidebar-accent text-sidebar-accent-foreground"
                        : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground",
                    )
                  }
                >
                  <Icon className="h-4 w-4 shrink-0" />
                  {!collapsed && item.label}
                </NavLink>
              )

              if (collapsed) {
                return (
                  <Tooltip key={item.to}>
                    <TooltipTrigger asChild>{link}</TooltipTrigger>
                    <TooltipContent side="right" sideOffset={4}>
                      {item.label}
                    </TooltipContent>
                  </Tooltip>
                )
              }

              return link
            })}
          </nav>
        </ScrollArea>

        {/* User */}
        <div className="border-t px-2 py-2">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              {collapsed ? (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <button className="flex items-center justify-center h-9 w-9 mx-auto rounded-md text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground transition-colors">
                      <User className="h-4 w-4" />
                    </button>
                  </TooltipTrigger>
                  <TooltipContent side="right" sideOffset={4}>
                    {username || "User"}
                  </TooltipContent>
                </Tooltip>
              ) : (
                <button className="flex items-center gap-3 w-full rounded-md px-3 py-2 text-sm font-medium text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground transition-colors">
                  <User className="h-4 w-4 shrink-0" />
                  <span className="truncate">{username || accountName || "User"}</span>
                </button>
              )}
            </DropdownMenuTrigger>
            <DropdownMenuContent side="right" align="end" sideOffset={8}>
              {username && (
                <>
                  <DropdownMenuItem disabled className="text-xs text-muted-foreground">
                    {username}
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                </>
              )}
              <DropdownMenuItem onClick={handleLogout}>
                <LogOut className="h-4 w-4 mr-2" />
                Logout
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </aside>
    </TooltipProvider>
  )
}
