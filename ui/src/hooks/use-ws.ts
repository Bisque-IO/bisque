import { useEffect } from "react"
import { wsClient, type ServerWsMessage } from "@/lib/ws"
import { useCatalogsStore } from "@/stores/catalogs"
import { useAuthStore } from "@/stores/auth"
import { useClusterStore } from "@/stores/cluster"

export function useWsConnection() {
  const token = useAuthStore((s) => s.token)
  const activeId = useClusterStore((s) => s.activeId)
  const addEvent = useCatalogsStore((s) => s.addEvent)
  const clearEvents = useCatalogsStore((s) => s.clearEvents)

  useEffect(() => {
    if (!token) return

    clearEvents()
    wsClient.connect()

    const unsub = wsClient.subscribe((msg: ServerWsMessage) => {
      if (msg.type === "Event") {
        addEvent(msg.event)
      }
    })

    return () => {
      unsub()
      wsClient.disconnect()
    }
  }, [token, activeId, addEvent, clearEvents])
}
