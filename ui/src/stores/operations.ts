import { create } from "zustand"
import type { Operation } from "@/lib/api"

interface OperationsState {
  operations: Operation[]
  /** Internal index for O(1) upsert lookups. */
  _index: Map<string, number>
  setOperations: (ops: Operation[]) => void
  upsertOperation: (op: Operation) => void
}

function buildIndex(ops: Operation[]): Map<string, number> {
  const m = new Map<string, number>()
  for (let i = 0; i < ops.length; i++) {
    m.set(ops[i].id, i)
  }
  return m
}

export const useOperationsStore = create<OperationsState>((set) => ({
  operations: [],
  _index: new Map(),
  setOperations: (ops) => set({ operations: ops, _index: buildIndex(ops) }),
  upsertOperation: (op) =>
    set((state) => {
      const existingIdx = state._index.get(op.id)
      if (existingIdx !== undefined) {
        const next = [...state.operations]
        next[existingIdx] = op
        return { operations: next, _index: state._index }
      }
      const next = [op, ...state.operations]
      // Rebuild index since prepending shifts all indices
      return { operations: next, _index: buildIndex(next) }
    }),
}))
