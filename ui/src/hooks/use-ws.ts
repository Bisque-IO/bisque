import { useEffect } from "react"
import { wsClient } from "@/lib/ws"
import { useCatalogsStore } from "@/stores/catalogs"
import { useOperationsStore } from "@/stores/operations"
import { useAuthStore } from "@/stores/auth"
import { useClusterStore } from "@/stores/cluster"
import type { ServerMessage } from "@/lib/ws-protocol"

export function useWsConnection() {
  const token = useAuthStore((s) => s.token)
  const activeId = useClusterStore((s) => s.activeId)

  useEffect(() => {
    if (!token) return

    useCatalogsStore.getState().clearEvents()

    // Safety net: if not already connected (e.g. page refresh with stored token),
    // initiate connection. loginAndConnect() handles the initial login flow.
    wsClient.connect()

    // L4: Wrap message handler in try-catch to prevent unhandled exceptions
    // from breaking the WebSocket subscription.
    const unsub = wsClient.subscribe((msg: ServerMessage) => {
      try {
        switch (msg.type) {
          case "CatalogEvent":
            useCatalogsStore.getState().addEvent(
              msg as unknown as { type: string; catalog: string; table?: string; [key: string]: unknown },
            )
            break
          case "OperationsSnapshot":
            useOperationsStore.getState().setOperations(msg.operations)
            break
          case "OperationUpdate":
            useOperationsStore.getState().upsertOperation(msg.operation)
            break
          case "SnapshotRequired":
            console.warn(`[bisque-ws] SnapshotRequired for catalog="${msg.catalog}". Clearing events.`)
            useCatalogsStore.getState().clearEvents()
            break
        }
      } catch (err) {
        console.error("[bisque-ws] Error handling push message:", err)
      }
    })

    return () => {
      unsub()
    }
  }, [token, activeId])
}
