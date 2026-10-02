/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// SPEC-07 §4: dev-server proxy so the frontend can call the API gateway
// without CORS issues: /api and /ws both forward to the API gateway on :8080.
export default defineConfig({
  plugins: [react()],
  // sockjs-client expects Node's `global`; alias it to the browser global.
  define: { global: 'globalThis' },
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/ws': {
        target: 'http://localhost:8080',
        ws: true,
      },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    globals: true,
  },
})
