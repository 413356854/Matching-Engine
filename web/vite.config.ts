import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const target = env.VITE_PROXY_TARGET || 'http://localhost:8080';

  return {
    plugins: [react()],
    server: {
      host: '0.0.0.0',
      port: 5173,
      proxy: {
        '/api': {
          target,
          changeOrigin: true,
        },
        // 盘口推送 WebSocket：/ws/orderbook → 后端同一端口
        '/ws': {
          target,
          changeOrigin: true,
          ws: true,
        },
      },
    },
  };
});
