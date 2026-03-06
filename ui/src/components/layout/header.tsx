import { useLocation, useParams, Link } from "react-router"
import { Button } from "@/components/ui/button"
import { Menu, ChevronRight } from "lucide-react"
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet"
import { MobileSidebar } from "./mobile-sidebar"
import { ClusterSwitcher } from "../cluster-switcher"
import { useHeaderActionsRef } from "./header-actions"
import { useAuthStore } from "@/stores/auth"
import { type ReactNode } from "react"

interface Breadcrumb {
  label: string
  to?: string
}

const routeTitles: Record<string, string> = {
  "/": "Dashboard",
  "/cluster": "Cluster",
  "/tenants": "Tenants",
  "/catalogs": "Catalogs",
  "/tables": "Tables",
  "/query": "Query",
  "/observability": "Observability",
  "/traces": "Traces",
  "/metrics": "Metrics",
  "/logs": "Logs",
  "/api-keys": "API Keys",
  "/settings": "Settings",
}

function useBreadcrumbs(): Breadcrumb[] {
  const { pathname } = useLocation()
  const params = useParams()
  const tenantId = useAuthStore((s) => s.tenantId)
  const tenantName = useAuthStore((s) => s.tenantName)

  const tenantCrumb: Breadcrumb = {
    label: tenantName ?? `Tenant #${tenantId}`,
    to: tenantId ? `/tenants/${tenantId}` : undefined,
  }

  // Catalog list
  if (pathname === "/catalogs")
    return [tenantCrumb, { label: "Catalogs" }]

  // Catalog detail
  if (pathname.startsWith("/catalogs/") && params.catalogName)
    return [tenantCrumb, { label: "Catalogs", to: "/catalogs" }, { label: params.catalogName }]

  // Table detail
  if (pathname.startsWith("/tables/") && params.catalogName && params.tableName)
    return [
      tenantCrumb,
      { label: "Catalogs", to: "/catalogs" },
      { label: params.catalogName, to: `/catalogs/${params.catalogName}` },
      { label: params.tableName },
    ]

  // Exact match — single breadcrumb, no link
  if (routeTitles[pathname]) return [{ label: routeTitles[pathname] }]

  // Detail pages
  if (pathname.startsWith("/tenants/") && params.tenantId)
    return [{ label: "Tenants", to: "/tenants" }, { label: `#${params.tenantId}` }]

  if (pathname.startsWith("/traces/") && params.traceId)
    return [{ label: "Traces", to: "/traces" }, { label: params.traceId }]

  // Fallback
  const segment = pathname.split("/")[1]
  return [{ label: segment ? segment.charAt(0).toUpperCase() + segment.slice(1) : "Dashboard" }]
}

function BreadcrumbNav({ crumbs }: { crumbs: Breadcrumb[] }) {
  const items: ReactNode[] = []
  crumbs.forEach((crumb, i) => {
    const isLast = i === crumbs.length - 1
    if (i > 0) {
      items.push(
        <ChevronRight key={`sep-${i}`} className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
      )
    }
    if (crumb.to && !isLast) {
      items.push(
        <Link
          key={i}
          to={crumb.to}
          className="text-muted-foreground hover:text-foreground transition-colors"
        >
          {crumb.label}
        </Link>
      )
    } else {
      items.push(<span key={i}>{crumb.label}</span>)
    }
  })
  return <>{items}</>
}

export function Header() {
  const crumbs = useBreadcrumbs()
  const actionsRef = useHeaderActionsRef()

  return (
    <header className="flex h-14 items-center border-b px-4 gap-4">
      <Sheet>
        <SheetTrigger asChild>
          <Button variant="ghost" size="icon" className="md:hidden">
            <Menu className="h-5 w-5" />
          </Button>
        </SheetTrigger>
        <SheetContent side="left" className="w-60 p-0">
          <MobileSidebar />
        </SheetContent>
      </Sheet>

      <ClusterSwitcher />

      <h1 className="text-lg font-semibold tracking-tight hidden md:flex items-center gap-1.5">
        <BreadcrumbNav crumbs={crumbs} />
      </h1>

      <div className="flex-1" />

      <div ref={actionsRef} className="flex items-center gap-2" />
    </header>
  )
}
