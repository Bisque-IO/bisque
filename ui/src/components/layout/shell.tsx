import { Outlet } from "react-router"
import { Sidebar } from "./sidebar"
import { Header } from "./header"
import { HeaderActionsProvider } from "./header-actions"
import { useWsConnection } from "@/hooks/use-ws"

export function Shell() {
  useWsConnection()

  return (
    <HeaderActionsProvider>
      <div className="flex h-screen overflow-hidden">
        <Sidebar />
        <div className="flex flex-1 flex-col overflow-hidden">
          <Header />
          <main className="flex-1 overflow-auto p-6">
            <Outlet />
          </main>
        </div>
      </div>
    </HeaderActionsProvider>
  )
}
