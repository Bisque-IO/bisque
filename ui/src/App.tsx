import { lazy, Suspense } from "react"
import { BrowserRouter, Routes, Route, Navigate } from "react-router"
import { TooltipProvider } from "@/components/ui/tooltip"
import { Toaster } from "@/components/ui/sonner"
import { useAuthStore } from "@/stores/auth"
import { Shell } from "@/components/layout/shell"
import { LoginPage } from "@/pages/login"
import { DashboardPage } from "@/pages/dashboard"
import { TenantListPage } from "@/pages/tenants/list"
import { TenantDetailPage } from "@/pages/tenants/detail"
import { CatalogListPage } from "@/pages/catalogs/list"
import { CatalogDetailPage } from "@/pages/catalogs/detail"
import { ApiKeyListPage } from "@/pages/api-keys/list"
import { TableListPage } from "@/pages/tables/list"
import { TableDetailPage } from "@/pages/tables/detail"
import { TracesPage } from "@/pages/observability/traces"
import { TraceDetailPage } from "@/pages/observability/trace-detail"
import { MetricsPage } from "@/pages/observability/metrics"
import { LogsPage } from "@/pages/observability/logs"
import { ObservabilityOverviewPage } from "@/pages/observability/overview"
import { SettingsPage } from "@/pages/settings"
import { ClusterPage } from "@/pages/cluster"
import { OperationsPage } from "@/pages/operations"

// Lazy-load the query page to keep Monaco out of the main bundle
const QueryPage = lazy(() =>
  import("@/pages/tables/query").then((m) => ({ default: m.QueryPage })),
)

function RequireAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token)
  if (!token) return <Navigate to="/login" replace />
  return <>{children}</>
}

export default function App() {
  return (
    <BrowserRouter>
      <TooltipProvider>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            element={
              <RequireAuth>
                <Shell />
              </RequireAuth>
            }
          >
            <Route index element={<DashboardPage />} />
            <Route path="cluster" element={<ClusterPage />} />
            <Route path="operations" element={<OperationsPage />} />
            <Route path="tenants" element={<TenantListPage />} />
            <Route path="tenants/:tenantId" element={<TenantDetailPage />} />
            <Route path="catalogs" element={<CatalogListPage />} />
            <Route path="catalogs/:catalogName" element={<CatalogDetailPage />} />
            <Route path="tables" element={<TableListPage />} />
            <Route path="tables/:catalogName/:tableName" element={<TableDetailPage />} />
            <Route path="query" element={<Suspense fallback={<div className="p-6 text-muted-foreground">Loading editor...</div>}><QueryPage /></Suspense>} />
            <Route path="observability" element={<ObservabilityOverviewPage />} />
            <Route path="traces" element={<TracesPage />} />
            <Route path="traces/:traceId" element={<TraceDetailPage />} />
            <Route path="metrics" element={<MetricsPage />} />
            <Route path="logs" element={<LogsPage />} />
            <Route path="api-keys" element={<ApiKeyListPage />} />
            <Route path="settings" element={<SettingsPage />} />
          </Route>
        </Routes>
        <Toaster />
      </TooltipProvider>
    </BrowserRouter>
  )
}
