import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig({
  // sockjs-client@1.6.1 内部使用了 Node.js 的 `global` 变量（见 browser-crypto.js）。
  // Vite 默认不做 Node 内置 polyfill，这里把 `global` 指向浏览器的 `globalThis`，
  // 以避免运行时 `Uncaught ReferenceError: global is not defined` 报错。
  define: {
    global: 'globalThis',
  },
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    host: true,
    proxy: {
      // REST API requests are proxied to the gateway service
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      // WebSocket (SockJS + STOMP) proxied to the gateway service.
      // The gateway forwards /ws/** to notification-service.
      '/ws': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        ws: true,
      },
    },
  },
});