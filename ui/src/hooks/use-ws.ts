import { useEffect } from "react"
import { wsClient } from "@/lib/ws"
import { useCatalogsStore } from "@/stores/catalogs"
import { useOperationsStore } from "@/stores/operations"
import { useAuthStore } from "@/stores/auth"
import { useClusterStore } from "@/stores/cluster"
import type { ServerMessage } from "@/lib/ws-protocol"

// H12: Ref-count the singleton connection so multiple hook consumers
// don't disconnect each other during cleanup.
let connectionRefCount = 0

export function useWsConnection() {
  const token = useAuthStore((s) => s.token)
  const activeId = useClusterStore((s) => s.activeId)
  const addEvent = useCatalogsStore((s) => s.addEvent)
  const clearEvents = useCatalogsStore((s) => s.clearEvents)
  const setOperations = useOperationsStore((s) => s.setOperations)
  const upsertOperation = useOperationsStore((s) => s.upsertOperation)

  useEffect(() => {
    if (!token) return

    clearEvents()
    connectionRefCount++
    if (connectionRefCount === 1) {
      wsClient.connect()
    }

    // L4: Wrap message handler in try-catch to prevent unhandled exceptions
    // from breaking the WebSocket subscription.
    const unsub = wsClient.subscribe((msg: ServerMessage) => {
      try {
        switch (msg.type) {
          case "CatalogEvent":
            addEvent(msg as unknown as { type: string; catalog: string; table?: string; [key: string]: unknown })
            break
          case "OperationsSnapshot":
            setOperations(msg.operations)
            break
          case "OperationUpdate":
            upsertOperation(msg.operation)
            break
          case "SnapshotRequired":
            // Client fell behind on catalog events — clear and let push refill
            console.warn(`[bisque-ws] SnapshotRequired for catalog="${msg.catalog}". Clearing events.`)
            clearEvents()
            break
        }
      } catch (err) {
        console.error("[bisque-ws] Error handling push message:", err)
      }
    })

    return () => {
      unsub()
      connectionRefCount--
      if (connectionRefCount === 0) {
        wsClient.disconnect()
      }
    }
  }, [token, activeId, addEvent, clearEvents, setOperations, upsertOperation])
}
