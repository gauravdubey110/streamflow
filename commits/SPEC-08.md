# Commit Plan — SPEC-08: Frontend StreamCard + Live ViewerCountChart

Suggested branch: `feat/spec-08-stream-card-and-chart`

---

## Commit 1 — Add useStreamMetrics hook and fetchStreams API helper

**Message:**
```
SPEC-08: add useStreamMetrics hook and fetchStreams API helper

useStreamMetrics subscribes to /topic/streams/{id}/metrics via the
existing useWebSocket hook. It maintains a 60-point ring buffer in a
useRef and flushes a shallow-copy array via useState on each new
WebSocket message, which is the only render trigger.

fetchStreams() is added to api.ts as a typed Axios wrapper around
GET /api/v1/streams, typed against StreamSummaryDTO.

Refs: specs/SPEC-08-frontend-stream-card-and-chart.md
```

**Files:**
- `frontend/src/hooks/useStreamMetrics.ts`
- `frontend/src/services/api.ts`

**Stage command:**
```bash
git add frontend/src/hooks/useStreamMetrics.ts frontend/src/services/api.ts
```

---

## Commit 2 — Add MetricCard, ViewerCountChart, StreamCard components

**Message:**
```
SPEC-08: add MetricCard, ViewerCountChart, and StreamCard components

MetricCard: generic stat tile (label + value + optional ReactNode sub).
ViewerCountChart: Recharts LineChart wrapped in React.memo; receives
  up to 60 ChartPoints; x = HH:mm:ss, y = liveViewerCount.
StreamCard: tile per stream — shows name, formatted viewer count,
  delta arrow (▲/▼), buffer rate, ViewerCountChart, and a
  "Reconnecting…" overlay when the WebSocket is disconnected.

Refs: specs/SPEC-08-frontend-stream-card-and-chart.md
```

**Files:**
- `frontend/src/components/common/MetricCard.tsx`
- `frontend/src/components/stream/ViewerCountChart.tsx`
- `frontend/src/components/stream/StreamCard.tsx`

**Stage command:**
```bash
git add \
  frontend/src/components/common/MetricCard.tsx \
  frontend/src/components/stream/ViewerCountChart.tsx \
  frontend/src/components/stream/StreamCard.tsx
```

---

## Commit 3 — Add StreamGrid layout and wire App.tsx with stream discovery

**Message:**
```
SPEC-08: add StreamGrid layout and wire App with stream discovery

StreamGrid: responsive Tailwind grid (1→2→3 cols) that renders
  StreamCard for each active stream; supports fading out departed
  streams via CSS opacity transition.

App.tsx: polls GET /api/v1/streams immediately on mount and every 30s.
  New stream IDs are upserted into the display list; departed IDs are
  faded out over 1s before removal to satisfy R7.

Refs: specs/SPEC-08-frontend-stream-card-and-chart.md
```

**Files:**
- `frontend/src/components/layout/StreamGrid.tsx`
- `frontend/src/App.tsx`

**Stage command:**
```bash
git add frontend/src/components/layout/StreamGrid.tsx frontend/src/App.tsx
```

---

## Commit 4 — Add Vitest component tests for ViewerCountChart and StreamCard

**Message:**
```
SPEC-08: add Vitest component tests for ViewerCountChart and StreamCard

ViewerCountChart.test.tsx (7 tests):
  - renders without crash on empty / 1-point / 60-point history
  - asserts recharts-responsive-container div is present (jsdom)
  - validates memoization: same history ref → same DOM node

StreamCard.test.tsx (11 tests):
  - viewer count formatted with thousands separator
  - delta arrows for positive / negative / zero delta
  - "Reconnecting…" overlay shown/hidden based on connected flag
  - fallback to streamId when no name available
  - fading CSS class applied when fading prop is true

All 21 tests pass: vitest run → 3 files, 21 tests, 0 failures.

Refs: specs/SPEC-08-frontend-stream-card-and-chart.md
```

**Files:**
- `frontend/src/test/ViewerCountChart.test.tsx`
- `frontend/src/test/StreamCard.test.tsx`

**Stage command:**
```bash
git add \
  frontend/src/test/ViewerCountChart.test.tsx \
  frontend/src/test/StreamCard.test.tsx
```

---

## Commit 5 — Mark SPEC-08 Done, add evidence and commit plan

**Message:**
```
SPEC-08: mark Done, add evidence and commit plan

Updates spec status to Done, ticks all AC and DoD checkboxes,
adds §10 Evidence section with test/lint/build output and per-AC
code excerpts. Adds commits/SPEC-08.md commit plan.

Refs: specs/SPEC-08-frontend-stream-card-and-chart.md
```

**Files:**
- `specs/SPEC-08-frontend-stream-card-and-chart.md`
- `commits/SPEC-08.md`

**Stage command:**
```bash
git add \
  specs/SPEC-08-frontend-stream-card-and-chart.md \
  commits/SPEC-08.md
```

---

## Verification before pushing
- [ ] `npm --prefix frontend run lint` — exits 0, no output
- [ ] `npm --prefix frontend run test` — 3 files, 21 tests, 0 failures
- [ ] `npm --prefix frontend run build` — `✓ built in ~1s`, no type errors
- [ ] Demo evidence in spec matches reality
