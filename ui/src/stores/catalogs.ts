import { create } from "zustand"
import type { CatalogEvent } from "@/lib/ws"

interface CatalogsState {
  events: CatalogEvent[]
  addEvent: (event: CatalogEvent) => void
  clearEvents: () => void
}

export const useCatalogsStore = create<CatalogsState>((set) => ({
  events: [],
  addEvent: (event) =>
    set((state) => ({
      events: [event, ...state.events].slice(0, 200),
    })),
  clearEvents: () => set({ events: [] }),
}))
