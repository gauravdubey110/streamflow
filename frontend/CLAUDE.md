# Frontend — conventions

Husky + lint-staged run ESLint/Prettier on staged frontend files at commit time
(`frontend/.husky/pre-commit`, `frontend/.lintstagedrc.json`).

### Architecture

- Vite + React 19 + TypeScript, Tailwind for styling, Zustand for state (`store/streamStore.ts`,
  `store/alertStore.ts` — plain key-by-id maps updated via upsert actions), Recharts for charts.
- Real-time data enters through `services/stompClient.ts` (STOMP-over-SockJS client factory,
  `VITE_WS_URL` env var, 5 s reconnect) consumed by `hooks/useWebSocket.ts` and per-feature hooks
  (`useStreamMetrics`, `useAlerts`, `useCircuitBreakerState`, `useStreamHistory`). REST calls go
  through `services/api.ts` (axios).
- Components are organized by feature under `components/{stream,alerts,history,controls,common,layout}`.
- Tests (`src/test/*.test.tsx`) use Vitest + React Testing Library; `src/test/setup.ts` is the
  global test setup.
