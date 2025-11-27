import { defineConfig } from 'vite'
import wasm from 'vite-plugin-wasm'
import { resolve } from 'path'
import { copyFileSync, mkdirSync, existsSync } from 'fs'

export default defineConfig({
  plugins: [
    wasm(),
    {
      name: 'resolve-env-import',
      resolveId(id) {
        // Handle the 'env' bare import from wasm-pack generated code
        if (id === 'env') {
          return '\0env-virtual'
        }
      },
      load(id) {
        if (id === '\0env-virtual') {
          // Provide empty exports for the 'env' module
          return 'export default {};'
        }
      },
    },
    {
      name: 'copy-benchmark-data',
      closeBundle() {
        // Copy benchmark data to dist after build
        // Try public directory first (for local development), then repo root (for CI)
        const benchmarkSources = [
          resolve(__dirname, 'public/benchmarks/benchmark_results.json'),
          resolve(__dirname, '../benchmarks/benchmark_results.json'),
        ]
        const benchmarkDest = resolve(__dirname, 'dist/benchmarks/benchmark_results.json')

        try {
          // Create benchmarks directory if it doesn't exist
          const benchmarksDir = resolve(__dirname, 'dist/benchmarks')
          if (!existsSync(benchmarksDir)) {
            mkdirSync(benchmarksDir, { recursive: true })
          }

          // Try each source location
          let copied = false
          for (const benchmarkSrc of benchmarkSources) {
            if (existsSync(benchmarkSrc)) {
              copyFileSync(benchmarkSrc, benchmarkDest)
              console.log('✓ Copied benchmark_results.json from:', benchmarkSrc)
              copied = true
              break
            }
          }

          if (!copied) {
            console.warn('⚠️  benchmark_results.json not found in:', benchmarkSources.join(', '))
          }
        } catch (err) {
          console.error('Failed to copy benchmark data:', err)
        }

        // Copy dashboard data for performance page
        const dashboardSources = [
          resolve(__dirname, 'public/data/dashboard.json'),
          resolve(__dirname, '../data/dashboard.json'),
        ]
        const dashboardDest = resolve(__dirname, 'dist/data/dashboard.json')

        try {
          // Create data directory if it doesn't exist
          const dataDir = resolve(__dirname, 'dist/data')
          if (!existsSync(dataDir)) {
            mkdirSync(dataDir, { recursive: true })
          }

          // Try each source location
          let copied = false
          for (const dashboardSrc of dashboardSources) {
            if (existsSync(dashboardSrc)) {
              copyFileSync(dashboardSrc, dashboardDest)
              console.log('✓ Copied dashboard.json from:', dashboardSrc)
              copied = true
              break
            }
          }

          if (!copied) {
            console.warn('⚠️  dashboard.json not found in:', dashboardSources.join(', '))
          }
        } catch (err) {
          console.error('Failed to copy dashboard data:', err)
        }
      },
    },
  ],
  base: '/vibesql/', // GitHub Pages path
  build: {
    target: 'esnext', // Modern browsers only
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        conformance: resolve(__dirname, 'conformance.html'),
        benchmarks: resolve(__dirname, 'benchmarks.html'),
        performance: resolve(__dirname, 'performance.html'),
      },
    },
    // Increase chunk size warning limit for Monaco
    chunkSizeWarningLimit: 1000,
  },
  define: {
    __BUILD_TIMESTAMP__: JSON.stringify(new Date().toISOString()),
  },
})
