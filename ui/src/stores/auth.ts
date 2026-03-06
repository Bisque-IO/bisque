import { create } from "zustand"
import type { Account } from "@/lib/api"
import { useClusterStore } from "./cluster"

interface AuthState {
  token: string | null
  userId: number | null
  username: string | null
  accountId: number | null
  accountName: string | null
  accounts: Account[]
  tenantId: number | null
  tenantName: string | null
  login: (params: {
    token: string
    userId: number
    username: string
    accounts: Account[]
    accountId?: number
    tenantId?: number
    tenantName?: string
  }) => void
  selectAccount: (accountId: number, accountName: string) => void
  selectTenant: (tenantId: number, tenantName: string) => void
  logout: () => void
  reloadFromStorage: () => void
}

const FIELDS = [
  "token",
  "user_id",
  "username",
  "account_id",
  "account_name",
  "tenant_id",
  "tenant_name",
] as const

function key(field: string): string {
  const clusterId = useClusterStore.getState().activeId
  return `bisque_${clusterId}_${field}`
}

function getStored(field: string): string | null {
  return localStorage.getItem(key(field))
}

function getStoredNumber(field: string): number | null {
  const v = getStored(field)
  return v ? Number(v) : null
}

function setStored(field: string, value: string) {
  localStorage.setItem(key(field), value)
}

function removeStored(field: string) {
  localStorage.removeItem(key(field))
}

// One-time migration: copy legacy bisque_* keys to bisque_local_*
function migrateLegacyKeys() {
  const legacyFields = ["token", "user_id", "username", "account_id", "account_name", "tenant_id", "tenant_name"]
  const hasLegacy = localStorage.getItem("bisque_token") !== null
  const hasNew = localStorage.getItem("bisque_local_token") !== null
  if (hasLegacy && !hasNew) {
    for (const field of legacyFields) {
      const value = localStorage.getItem(`bisque_${field}`)
      if (value !== null) {
        localStorage.setItem(`bisque_local_${field}`, value)
        localStorage.removeItem(`bisque_${field}`)
      }
    }
  }
}

migrateLegacyKeys()

function readStateFromStorage() {
  return {
    token: getStored("token"),
    userId: getStoredNumber("user_id"),
    username: getStored("username"),
    accountId: getStoredNumber("account_id"),
    accountName: getStored("account_name"),
    accounts: [] as Account[],
    tenantId: getStoredNumber("tenant_id"),
    tenantName: getStored("tenant_name"),
  }
}

export const useAuthStore = create<AuthState>((set) => ({
  ...readStateFromStorage(),

  login: ({ token, userId, username, accounts, accountId, tenantId, tenantName }) => {
    setStored("token", token)
    setStored("user_id", String(userId))
    setStored("username", username)

    const selectedAccountId = accountId ?? accounts[0]?.id ?? null
    const selectedAccountName = accounts.find((a) => a.id === selectedAccountId)?.name ?? null

    if (selectedAccountId != null) {
      setStored("account_id", String(selectedAccountId))
    }
    if (selectedAccountName) {
      setStored("account_name", selectedAccountName)
    }
    if (tenantId != null) {
      setStored("tenant_id", String(tenantId))
    }
    if (tenantName) {
      setStored("tenant_name", tenantName)
    }

    set({
      token,
      userId,
      username,
      accounts,
      accountId: selectedAccountId,
      accountName: selectedAccountName,
      tenantId: tenantId ?? null,
      tenantName: tenantName ?? null,
    })
  },

  selectAccount: (accountId, accountName) => {
    setStored("account_id", String(accountId))
    setStored("account_name", accountName)
    removeStored("tenant_id")
    removeStored("tenant_name")
    set({ accountId, accountName, tenantId: null, tenantName: null })
  },

  selectTenant: (tenantId, tenantName) => {
    setStored("tenant_id", String(tenantId))
    setStored("tenant_name", tenantName)
    set({ tenantId, tenantName })
  },

  logout: () => {
    for (const field of FIELDS) {
      removeStored(field)
    }
    set({
      token: null,
      userId: null,
      username: null,
      accountId: null,
      accountName: null,
      accounts: [],
      tenantId: null,
      tenantName: null,
    })
  },

  reloadFromStorage: () => {
    set(readStateFromStorage())
  },
}))

// When active cluster changes, reload auth state from that cluster's storage
let prevClusterId = useClusterStore.getState().activeId
useClusterStore.subscribe((state) => {
  if (state.activeId !== prevClusterId) {
    prevClusterId = state.activeId
    useAuthStore.getState().reloadFromStorage()
  }
})
