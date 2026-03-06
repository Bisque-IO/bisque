import { Outlet } from "react-router"
import { Sidebar } from "./sidebar"
import { Header } from "./header"
import { useWsConnection } from "@/hooks/use-ws"
import { wsClient } from "@/lib/ws"
import { useEffect, useState } from "react"

export function Shell() {
  useWsConnection()
  const [wsConnected, setWsConnected] = useState(false)

  useEffect(() => {
    const interval = setInterval(() => {
      setWsConnected(wsClient.connected)
    }, 1000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <Header wsConnected={wsConnected} />
        <main className="flex-1 overflow-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
