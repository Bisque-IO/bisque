import path from "path"
import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    proxy: {
      "/_bisque": "http://localhost:3200",
      "/api": "http://localhost:3200",
      "/loki": "http://localhost:3200",
      "/v1": "http://localhost:3200",
    },
  },
})
