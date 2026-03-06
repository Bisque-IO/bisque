import { useState, useCallback } from "react"
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
  PanelLeftClose,
  PanelLeftOpen,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

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

function getInitialCollapsed(): boolean {
  try {
    return localStorage.getItem("bisque_sidebar_collapsed") === "true"
  } catch {
    return false
  }
}

export function Sidebar() {
  const [collapsed, setCollapsed] = useState(getInitialCollapsed)

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
          {!collapsed && (
            <span className="text-lg font-semibold tracking-tight pl-1">bisque</span>
          )}
          <Tooltip>
            <TooltipTrigger asChild>
              <button
                onClick={toggle}
                className={cn(
                  "flex items-center justify-center h-8 w-8 rounded-md text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground transition-colors",
                  collapsed && "mx-auto",
                )}
              >
                {collapsed ? (
                  <PanelLeftOpen className="h-4 w-4" />
                ) : (
                  <PanelLeftClose className="h-4 w-4" />
                )}
              </button>
            </TooltipTrigger>
            <TooltipContent side="right" sideOffset={4}>
              {collapsed ? "Expand sidebar" : "Collapse sidebar"}
            </TooltipContent>
          </Tooltip>
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
      </aside>
    </TooltipProvider>
  )
}
