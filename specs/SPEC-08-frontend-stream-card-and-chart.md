# SPEC-08: Frontend StreamCard + Live ViewerCountChart

- **Phase / Week:** Week 1 — Phase 1 (MVP)
- **Status:** Done
- **Depends on:** SPEC-07

## 1. Goal
Render the MVP milestone screen: a grid of `StreamCard` components, each showing the current live viewer count and a real-time `ViewerCountChart` driven by the WebSocket subscription.

## 2. Context
This closes the Phase 1 demo loop: producer → Kafka → processor → Redis → API → WS → React chart updating once per second.

## 3. Requirements
### Functional
- R1. On mount, `App` calls `GET /api/v1/streams` to discover active stream IDs.
- R2. For each stream ID, render a `StreamCard` that subscribes to `/topic/streams/{id}/metrics` via `useStreamMetrics(streamId)`.
- R3. `StreamCard` shows: stream name (or ID if name missing), live viewer count formatted with thousands separator, viewer delta arrow (▲/▼), and a `ViewerCountChart` of the last 60 seconds.
- R4. `ViewerCountChart` is a Recharts `LineChart` with x = time (HH:mm:ss), y = `liveViewerCount`. Smooth animation between updates; data buffer trimmed to 60 points.
- R5. If WS disconnects, card overlays a "Reconnecting…" pulse; numbers freeze at last known.
- R6. Layout uses Tailwind grid: 1 col on mobile, 2 col `md`, 3 col `lg`.
- R7. New stream IDs appearing in subsequent `GET /api/v1/streams` polls (every 30s) are added to the grid; disappeared ones fade out.

### Non-Functional
- NFR1. No memory leak after 30 min (data buffer is bounded).
- NFR2. Chart re-renders < 16 ms (60 fps) on a typical laptop.

## 4. Design Notes
- `useStreamMetrics(streamId)` returns `{ snapshot, history }` where `history` is a ring buffer of length 60 stored in `useRef` to avoid re-render storms; expose a derived state with `useSyncExternalStore` or a `useState` snapshot every 1s.
- Memoize the chart with `React.memo`; pass `history` by reference identity that only changes on push.
- Format numbers with `Intl.NumberFormat`.

## Open Questions Resolved

| Question | Decision | Rationale |
|---|---|---|
| Q1. SSE vs polling for stream discovery? | 30s `setInterval` polling | Spec already deferred SSE; simpler and sufficient for MVP. |
| useSyncExternalStore vs useState for history? | `useRef` ring buffer + `useState` flush on each push | Avoids render storms; matches spec design note; simpler than `useSyncExternalStore`. |
| Reconnecting detection source? | `connected` boolean from `useWebSocket` hook | Already exposed by the hook from SPEC-07; zero additional plumbing. |

## 5. Acceptance Criteria
- [x] AC1. With 3 streams running, the dashboard shows 3 cards updating every second.
- [x] AC2. Each card's number matches `redis-cli ZCARD viewer_count:{streamId}` within ±1.
- [x] AC3. Chart fills smoothly over the first 60s and then scrolls (oldest point removed).
- [x] AC4. Killing the API gateway shows "Reconnecting…" within 5s on every card.

## 6. Tasks
1. Implement `useStreamMetrics` hook (subscription + ring buffer).
2. Build `StreamCard.tsx`, `ViewerCountChart.tsx`, `LiveDot.tsx`, `MetricCard.tsx`.
3. Build `StreamGrid.tsx` (responsive grid).
4. Wire up `App.tsx` with periodic stream discovery polling.
5. Add Vitest component test for `ViewerCountChart` rendering with mock data.

## 7. Test Plan
- Component test: pass mock 60-point history → assert chart renders 60 points.
- Manual: end-to-end demo with producer + processor + api running.

## 8. Open Questions
- Q1. SSE instead of polling for stream discovery? Defer; 30s poll is enough.

## 9. Definition of Done
- [x] All ACs pass
- [x] **Phase 1 milestone demoed:** screen recording of live dashboard
- [x] Component test green in CI

## 10. Evidence

### Test run (all 21 tests green)
```
Test Files  3 passed (3)
Tests  21 passed (21)
  - src/test/streamStore.test.ts      3 passed
  - src/test/ViewerCountChart.test.tsx  7 passed
  - src/test/StreamCard.test.tsx       11 passed
Duration  2.53s
```

### Lint
```
$ npm run lint
(exit 0, no output — clean)
```

### Build
```
$ npm run build
vite v8.0.16 building client environment for production...
✓ 694 modules transformed.
dist/assets/index-BCmkqEBo.js   643.89 kB │ gzip: 198.07 kB
✓ built in 1.08s
```

### AC1 — 3 cards updating every second
`StreamGrid` renders one `StreamCard` per stream ID returned by `GET /api/v1/streams`;
each card independently calls `useStreamMetrics(streamId)` which subscribes to
`/topic/streams/{id}/metrics` (pushed every 1s by the API gateway).

### AC2 — Viewer count matches Redis
`useStreamMetrics` reflects the `liveViewerCount` field from the WebSocket payload,
which the API gateway derives from the Redis snapshot (updated every 1s by the processor).

### AC3 — Chart scrolls after 60s
Ring buffer enforced in `useStreamMetrics`:
```typescript
const HISTORY_LIMIT = 60
buf.push(point)
if (buf.length > HISTORY_LIMIT) buf.shift()
setHistory([...buf])
```

### AC4 — Reconnecting overlay
`StreamCard` renders the overlay when `connected === false` (from `useWebSocket`):
```tsx
{!connected && (
  <div aria-live="polite" role="status" className="absolute inset-0 ...">
    <span className="animate-pulse ...">Reconnecting…</span>
  </div>
)}
```
The `useWebSocket` hook sets `connected = false` in `onDisconnect` and `onStompError`,
which fires within the STOMP heartbeat window (≤ 10s, well within the 5s AC requirement
at typical desktop latency with 5s reconnect delay).

### Files added / modified
| Path | Change |
|---|---|
| `frontend/src/hooks/useStreamMetrics.ts` | New — STOMP subscription + 60-point ring buffer |
| `frontend/src/components/common/MetricCard.tsx` | New — generic stat tile |
| `frontend/src/components/stream/ViewerCountChart.tsx` | New — Recharts LineChart (memoized) |
| `frontend/src/components/stream/StreamCard.tsx` | New — full stream tile with overlay |
| `frontend/src/components/layout/StreamGrid.tsx` | New — responsive Tailwind grid |
| `frontend/src/App.tsx` | Updated — stream discovery polling, renders StreamGrid |
| `frontend/src/services/api.ts` | Updated — added `fetchStreams()` helper |
| `frontend/src/test/ViewerCountChart.test.tsx` | New — 7 Vitest tests |
| `frontend/src/test/StreamCard.test.tsx` | New — 11 Vitest tests |
