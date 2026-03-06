import { createContext, useContext, useState, useCallback, type ReactNode } from "react"
import { createPortal } from "react-dom"

const HeaderActionsContext = createContext<{
  setTarget: (el: HTMLDivElement | null) => void
  target: HTMLDivElement | null
}>({ setTarget: () => {}, target: null })

export function HeaderActionsProvider({ children }: { children: ReactNode }) {
  const [target, setTarget] = useState<HTMLDivElement | null>(null)
  return (
    <HeaderActionsContext.Provider value={{ target, setTarget }}>
      {children}
    </HeaderActionsContext.Provider>
  )
}

/** Renders in the header bar. Place this ref on a div in the header. */
export function useHeaderActionsRef() {
  const { setTarget } = useContext(HeaderActionsContext)
  return useCallback((el: HTMLDivElement | null) => setTarget(el), [setTarget])
}

/** Portal children into the header actions slot */
export function HeaderActions({ children }: { children: ReactNode }) {
  const { target } = useContext(HeaderActionsContext)
  if (!target) return null
  return createPortal(children, target)
}
