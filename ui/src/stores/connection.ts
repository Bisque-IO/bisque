import { create } from "zustand"

export type ConnectionState = "connected" | "connecting" | "disconnected"

interface ConnectionStore {
  state: ConnectionState
  /** Number of reconnect attempts since last successful connection */
  reconnectAttempts: number
  setState: (state: ConnectionState) => void
  incrementReconnectAttempts: () => void
  resetReconnectAttempts: () => void
}

export const useConnectionStore = create<ConnectionStore>((set) => ({
  state: "disconnected",
  reconnectAttempts: 0,
  setState: (state) => set({ state }),
  incrementReconnectAttempts: () =>
    set((s) => ({ reconnectAttempts: s.reconnectAttempts + 1 })),
  resetReconnectAttempts: () => set({ reconnectAttempts: 0 }),
}))
