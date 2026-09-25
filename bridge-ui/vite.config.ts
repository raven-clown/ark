import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In development, /api goes to a local engine (ARK_ENGINE_URL or :8080).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': { target: process.env.ARK_ENGINE_URL ?? 'http://127.0.0.1:8080', changeOrigin: true },
    },
  },
  build: { outDir: 'dist', chunkSizeWarningLimit: 900 },
})
