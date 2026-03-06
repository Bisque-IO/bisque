import { create } from "zustand"
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type CatalogEvent = Record<string, any>

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
