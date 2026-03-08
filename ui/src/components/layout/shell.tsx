import { Outlet } from "react-router"
import { Sidebar } from "./sidebar"
import { Header } from "./header"
import { HeaderActionsProvider } from "./header-actions"
import { useWsConnection } from "@/hooks/use-ws"
import { useConnectionStore } from "@/stores/connection"
import { WifiOff, Loader2 } from "lucide-react"

function ConnectionBanner() {
  const state = useConnectionStore((s) => s.state)
  const attempts = useConnectionStore((s) => s.reconnectAttempts)

  if (state === "connected") return null

  return (
    <div className="flex items-center justify-center gap-2 px-3 py-1.5 text-xs font-medium bg-destructive/10 text-destructive border-b border-destructive/20">
      {state === "connecting" ? (
        <>
          <Loader2 className="h-3 w-3 animate-spin" />
          Connecting to server{attempts > 0 ? ` (attempt ${attempts + 1})` : ""}...
        </>
      ) : (
        <>
          <WifiOff className="h-3 w-3" />
          Disconnected from server
          {attempts > 0 ? ` \u2014 reconnecting (attempt ${attempts})...` : ""}
        </>
      )}
    </div>
  )
}

export function Shell() {
  useWsConnection()

  return (
    <HeaderActionsProvider>
      <div className="flex h-screen overflow-hidden">
        <Sidebar />
        <div className="flex flex-1 flex-col overflow-hidden">
          <Header />
          <ConnectionBanner />
          <main className="flex-1 overflow-auto p-6">
            <Outlet />
          </main>
        </div>
      </div>
    </HeaderActionsProvider>
  )
}
