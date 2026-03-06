import { decode } from "@msgpack/msgpack"
import { useAuthStore } from "@/stores/auth"
import { useClusterStore } from "@/stores/cluster"

export type ServerWsMessage =
  | { type: "Session"; session_id: number }
  | { type: "Event"; event: CatalogEvent }
  | { type: "SnapshotRequired"; catalog: string }

export type CatalogEvent =
  | { type: "TableCreated"; catalog: string; table: string; schema_ipc: number[] }
  | { type: "TableDropped"; catalog: string; table: string }
  | { type: "ActiveVersionBumped"; catalog: string; table: string; version: number }
  | { type: "SegmentSealed"; catalog: string; table: string; active_version: number; sealed_version: number }
  | { type: "SegmentPromoted"; catalog: string; table: string; s3_manifest_version: number }

export type ClientWsMessage =
  | { type: "Pin"; catalog: string; table: string; tier: string; version: number }
  | { type: "Unpin"; catalog: string; table: string; tier: string; version: number }
  | { type: "Heartbeat" }
  | { type: "Subscribe"; catalogs: string[] }

type WsEventHandler = (message: ServerWsMessage) => void

export class BisqueWsClient {
  private ws: WebSocket | null = null
  private handlers: Set<WsEventHandler> = new Set()
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectDelay = 1000
  private maxReconnectDelay = 30000
  private _connected = false

  get connected() {
    return this._connected
  }

  connect() {
    const token = useAuthStore.getState().token
    if (!token) return

    const cluster = useClusterStore.getState().activeCluster
    let url: string

    if (cluster.url) {
      const parsed = new URL(cluster.url)
      const protocol = parsed.protocol === "https:" ? "wss:" : "ws:"
      url = `${protocol}//${parsed.host}/_bisque/ws?token=${encodeURIComponent(token)}`
    } else {
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:"
      url = `${protocol}//${window.location.host}/_bisque/ws?token=${encodeURIComponent(token)}`
    }

    this.ws = new WebSocket(url)
    this.ws.binaryType = "arraybuffer"

    this.ws.onopen = () => {
      this._connected = true
      this.reconnectDelay = 1000
    }

    this.ws.onmessage = (event) => {
      try {
        const data = event.data instanceof ArrayBuffer
          ? decode(new Uint8Array(event.data)) as ServerWsMessage
          : JSON.parse(event.data as string) as ServerWsMessage
        for (const handler of this.handlers) {
          handler(data)
        }
      } catch {
        // ignore decode errors
      }
    }

    this.ws.onclose = () => {
      this._connected = false
      this.scheduleReconnect()
    }

    this.ws.onerror = () => {
      this.ws?.close()
    }
  }

  disconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.ws?.close()
    this.ws = null
    this._connected = false
  }

  subscribe(handler: WsEventHandler): () => void {
    this.handlers.add(handler)
    return () => this.handlers.delete(handler)
  }

  private scheduleReconnect() {
    if (this.reconnectTimer) return
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay)
      this.connect()
    }, this.reconnectDelay)
  }
}

import { MOCK } from "./api"
import { mockWsClient } from "./mock-ws"

const realWsClient = new BisqueWsClient()

export const wsClient = MOCK ? mockWsClient : realWsClient
