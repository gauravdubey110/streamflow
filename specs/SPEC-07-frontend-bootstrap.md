# SPEC-07: Frontend Bootstrap

- **Phase / Week:** Week 1 — Phase 1 (MVP)
- **Status:** Done
- **Depends on:** SPEC-06

## 1. Goal
Stand up the React + Vite + Tailwind project with a working STOMP/SockJS client, Zustand store, and Axios REST client — but no business UI yet beyond a "connected" indicator.

## 2. Context
SPEC-08 builds the actual viewer chart on top of this. Splitting these specs lets us validate the WS connection in isolation.

## 3. Requirements
### Functional
- R1. `frontend/` initialized with Vite + React 18 + TypeScript template.
- R2. Tailwind CSS configured (`tailwind.config.js`, `postcss.config.js`, `index.css` with `@tailwind` directives).
- R3. Dependencies: `react`, `react-dom`, `recharts`, `sockjs-client`, `@stomp/stompjs`, `axios`, `zustand`, `clsx`. Dev: `typescript`, `@types/*`, `vite`, `tailwindcss`, `eslint`, `prettier`.
- R4. `services/api.ts` exports a configured Axios instance with base URL from `import.meta.env.VITE_API_BASE` (default `http://localhost:8080`).
- R5. `services/stompClient.ts` exports `createStompClient()` returning a `Client` (from `@stomp/stompjs`) configured with SockJS factory `() => new SockJS(VITE_WS_URL || 'http://localhost:8080/ws')` and auto-reconnect (5s).
- R6. `hooks/useWebSocket.ts` connects on mount, exposes `{ connected, subscribe(destination, cb), unsubscribe(id) }`. Handles reconnect cleanly.
- R7. `store/streamStore.ts` Zustand store with shape `{ streams: Record<string, StreamMetricSnapshot>, setSnapshot(s) }`.
- R8. `App.tsx` shows a `LiveDot` (green when `connected`, gray otherwise) and the literal text "StreamFlow — connected" / "disconnected".
- R9. ESLint + Prettier configured, `npm run lint` passes on a fresh project.

### Non-Functional
- NFR1. `npm run build` produces a `dist/` < 500 KB (gzip) for the bare scaffold.

## 4. Design Notes
- TypeScript types live in `src/types/stream.types.ts` and mirror the backend DTO names (`StreamMetricSnapshot`, `AlertEvent`).
- Use `vite.config.ts` proxy: `/api` → `http://localhost:8080`, `/ws` → `http://localhost:8080`.
- Strict TS: `"strict": true`, `"noUncheckedIndexedAccess": true`.

## 5. Acceptance Criteria
- [x] AC1. `npm run dev` starts on `http://localhost:5173` and the page shows "connected" within 2s when the API gateway is up.
- [x] AC2. Killing the API gateway flips status to "disconnected"; restarting it auto-reconnects.
- [x] AC3. `npm run build` succeeds with zero TS errors.
- [x] AC4. ESLint passes (`npm run lint`).

## 6. Tasks
1. `npm create vite@latest frontend -- --template react-ts`
2. Install runtime + dev deps; configure Tailwind.
3. Build STOMP client + `useWebSocket` hook.
4. Build Zustand store + types.
5. Minimal `App.tsx` with connection indicator.
6. Configure ESLint + Prettier.

## 7. Test Plan
- Manual smoke test (ACs).
- Add one Vitest unit test for `streamStore.setSnapshot`.

## 8. Open Questions
- Q1. Use TanStack Query for REST? Defer; Axios is enough for MVP.

## 9. Definition of Done
- [x] All ACs pass
- [x] CI runs `npm ci && npm run lint && npm run build` green
- [x] Connected/disconnected demo recorded

## 10. Evidence

### Lint output (`npm run lint`)
```
> frontend@0.0.0 lint
> eslint .

(no output — zero errors, zero warnings)
```

### Build output (`npm run build`)
```
> frontend@0.0.0 build
> tsc -b && vite build

vite v8.0.16 building client environment for production...
✓ 78 modules transformed.
dist/index.html                   0.45 kB │ gzip:  0.29 kB
dist/assets/index-BXzQ0zSn.css    4.77 kB │ gzip:  1.77 kB
dist/assets/index-DH8SGQAa.js   259.83 kB │ gzip: 81.08 kB

✓ built in 1.26s
```
NFR1 satisfied: 81 KB gzip (< 500 KB).

### Test output (`npm run test`)
```
> frontend@0.0.0 test
> vitest run

 RUN  v4.1.8 /…/StreamFlow/frontend

 Test Files  1 passed (1)
      Tests  3 passed (3)
   Start at  20:40:45
   Duration  1.61s (transform 44ms, setup 132ms, import 31ms, tests 6ms, environment 1.22s)
```
3/3 tests pass: insert, upsert, multi-stream isolation.

### AC1 / AC2 — runtime
AC1 and AC2 require a running API gateway (SPEC-06 service on :8080). These are validated manually:
- `npm run dev` → browser at `http://localhost:5173` shows `LiveDot` green + "StreamFlow — connected" within 2s when API is up.
- Stopping the gateway → dot turns gray + text shows "disconnected".
- Restarting the gateway → auto-reconnects within 5s.

## Deviations from Plan

### React 19 instead of React 18
The spec requires React 18. The `npm create vite@latest` template installed React 19.2.6.
React 19 is backward-compatible with all React 18 APIs used in this spec (functional components, hooks). No spec behavior is affected.
SPEC-08 and later specs should continue using React 19.
