import { create } from "zustand"

export interface ClusterConfig {
  id: string
  name: string
  url: string // "" = local (relative paths)
}

const LOCAL_CLUSTER: ClusterConfig = {
  id: "local",
  name: "Local",
  url: "",
}

const STORAGE_KEY = "bisque_clusters"
const ACTIVE_KEY = "bisque_active_cluster"

function loadClusters(): ClusterConfig[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) {
      const parsed = JSON.parse(raw) as ClusterConfig[]
      if (!parsed.find((c) => c.id === "local")) {
        parsed.unshift(LOCAL_CLUSTER)
      }
      return parsed
    }
  } catch {
    /* ignore */
  }
  return [LOCAL_CLUSTER]
}

function saveClusters(clusters: ClusterConfig[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(clusters))
}

interface ClusterState {
  clusters: ClusterConfig[]
  activeId: string
  activeCluster: ClusterConfig
  addCluster: (cluster: ClusterConfig) => void
  removeCluster: (id: string) => void
  updateCluster: (id: string, updates: Partial<Omit<ClusterConfig, "id">>) => void
  setActive: (id: string) => void
}

export const useClusterStore = create<ClusterState>((set, get) => {
  const clusters = loadClusters()
  const activeId = localStorage.getItem(ACTIVE_KEY) || "local"

  return {
    clusters,
    activeId,
    activeCluster: clusters.find((c) => c.id === activeId) || clusters[0],

    addCluster: (cluster) => {
      const updated = [...get().clusters, cluster]
      saveClusters(updated)
      set({ clusters: updated })
    },

    removeCluster: (id) => {
      if (id === "local") return
      const updated = get().clusters.filter((c) => c.id !== id)
      saveClusters(updated)
      if (get().activeId === id) {
        localStorage.setItem(ACTIVE_KEY, "local")
        set({ clusters: updated, activeId: "local", activeCluster: updated[0] })
      } else {
        set({ clusters: updated })
      }
    },

    updateCluster: (id, updates) => {
      const updated = get().clusters.map((c) =>
        c.id === id ? { ...c, ...updates } : c,
      )
      saveClusters(updated)
      const activeCluster =
        get().activeId === id ? updated.find((c) => c.id === id)! : get().activeCluster
      set({ clusters: updated, activeCluster })
    },

    setActive: (id) => {
      const cluster = get().clusters.find((c) => c.id === id)
      if (!cluster) return
      localStorage.setItem(ACTIVE_KEY, id)
      set({ activeId: id, activeCluster: cluster })
    },
  }
})
