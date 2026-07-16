import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: './',
  build: {
    outDir: '../lib/kafka_batch/web/public',
    emptyOutDir: true,
    sourcemap: false,
  },
})
