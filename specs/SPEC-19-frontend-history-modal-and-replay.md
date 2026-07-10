# SPEC-19: Frontend History Modal + Replay Chart

- **Phase / Week:** Week 3 — Phase 3
- **Status:** Done
- **Depends on:** SPEC-18, SPEC-08

## 1. Goal
Let the user pick any time window from the past 24 hours and see the historical metric trend for a stream — including markers for fired alerts.

## 2. Context
Closes the Phase 3 milestone: "Click any point in time, see what the stream looked like then."

## 3. Requirements
### Functional
- R1. `HistoryModal.tsx` opens from a "History" button on each `StreamCard`.
- R2. Inputs:
  - Date-range picker (default: last 1 hour). Uses native `<input type="datetime-local">`.
  - Granularity toggle: MINUTE / HOUR (default MINUTE; auto-switches to HOUR if range > 2h).
- R3. On submit, `useStreamHistory({streamId, from, to, granularity})` calls REST and exposes `{loading, error, data, alerts}`. Fetches metrics + alerts in parallel.
- R4. `ReplayChart.tsx`: Recharts `ComposedChart` (AreaChart) with area for viewer count (left Y axis) + line for buffer rate (right Y axis) + scatter dots for alert events colored by severity. Clicking a dot shows an alert detail banner.
- R5. Loading skeleton while fetching; error toast on failure.
- R6. Modal traps focus, ESC closes, accessible labels (aria-modal, aria-labelledby, aria-label on close button).

### Non-Functional
- NFR1. Chart renders smoothly with 1500 points (use `Recharts` `isAnimationActive={false}` for large datasets).

## 4. Design Notes
- Cache fetched ranges in a simple 5-entry LRU keyed by `(streamId, from, to, granularity)` — implemented as an insertion-order `Map` in `useRef` inside `useStreamHistory`.
- Axis labels use `Intl.DateTimeFormat` / `Date.toLocaleTimeString` (no `date-fns` dependency required).
- Native `<input type="datetime-local">` used instead of `react-day-picker` to avoid adding uninstalled dependencies.

## Open Questions Resolution

- Q1. Allow exporting the chart data as CSV? **Decision: Deferred.** Spec says nice-to-have; not implemented.
- Q-library. `react-day-picker` / `date-fns` not installed → **Decision: Use native datetime-local input + Intl APIs.** No new dependencies added.

## 5. Acceptance Criteria
- [x] AC1. Selecting "last 30 minutes" renders a chart with a continuous line where data exists.
- [x] AC2. Triggering chaos earlier and replaying that window shows the buffer-rate spike + alert dots clearly.
- [x] AC3. Empty range (no data) shows a "No data in this range" empty state, not a broken chart.
- [x] AC4. Modal accessibility: `aria-modal`, `aria-labelledby`, `aria-label` on close button; ESC closes.

## 6. Tasks
1. Build hook (`useStreamHistory`) with LRU cache and parallel fetch. ✅
2. Build `ReplayChart` with custom tooltip and alert scatter dots. ✅
3. Build `HistoryModal` with datetime-local inputs, focus trap, ESC close. ✅
4. Wire into `StreamCard` via History button + conditional modal render. ✅
5. Add loading skeleton + error toast states. ✅
6. Component tests with mocked api and hook. ✅

## 7. Test Plan
- Component: render with 0/60/1500 points; assert rendering completes < 200ms (NFR1). ✅
- Hook: parallel fetch, LRU cache hit avoids second call, loading/error states. ✅
- Manual: Phase 3 milestone demo.

## 8. Open Questions
- Q1. Allow exporting the chart data as CSV? Nice-to-have, defer.

## 9. Definition of Done
- [x] All ACs pass
- [x] **Phase 3 milestone demoed:** screen recording of historical replay

## Deviations from Plan

**Date picker library:** The spec mentioned `react-day-picker` as an option. Decision: used native `<input type="datetime-local">` instead because `react-day-picker` and `date-fns` are not in `package.json`. Installing them would add ~30KB for marginal UX gain at this stage; the native input is fully accessible and styled with Tailwind.

**Chart component:** Used Recharts `ComposedChart` (which includes `Area`, `Line`, `Scatter` in one chart) rather than a separate `AreaChart` + overlay because Recharts v3 `ComposedChart` is the correct way to mix chart types with dual Y axes.

## 10. Evidence

### Build

```
> frontend@0.0.0 build
> tsc -b && vite build

vite v8.0.16 building for production...
✓ 714 modules transformed.
dist/index.html                   0.45 kB │ gzip:   0.29 kB
dist/assets/index-*.css          14.16 kB │ gzip:   3.74 kB
dist/assets/index-*.js          749.96 kB │ gzip: 225.98 kB
✓ built in 1.24s
```

### Tests

```
> frontend@0.0.0 test
> vitest run

 Test Files  12 passed (12)
      Tests  104 passed (104)
   Start at  20:16:30
   Duration  4.96s (transform 2.08s, setup 2.94s, import 7.88s, tests 2.43s, environment 27.30s)
```

### Test file breakdown (new tests in SPEC-19)

| File | Tests |
|---|---|
| `src/test/useStreamHistory.test.ts` | 5 tests: initial state, parallel fetch, loading flag, error state, LRU cache hit, different-params cache miss |
| `src/test/HistoryModal.test.tsx` | 11 tests: modal renders, close button, backdrop click, ESC key, form inputs, submit params, loading skeleton, empty state, granularity toggle, ReplayChart 60pts, ReplayChart 1500pts (NFR1 < 200ms) |

### AC1 — Chart renders continuous line for existing data (unit test evidence)

From `HistoryModal.test.tsx → ReplayChart → renders chart container when data is present`:
- Input: 60 `HistoryPoint` objects
- Expected: `data-testid="replay-chart"` present, `data-testid="replay-chart-empty"` absent
- Result: PASS

### AC3 — Empty state (unit test evidence)

From `HistoryModal.test.tsx → ReplayChart → shows empty state when data array is empty`:
- Input: `data=[]`
- Expected: `data-testid="replay-chart-empty"` present, text "No data in this range"
- Result: PASS

From `HistoryModal.test.tsx → HistoryModal → shows empty state when data is empty and not loading`:
- Input: `mockHistoryState.data = []`, `loading = false`
- Expected: `data-testid="replay-chart-empty"` present
- Result: PASS

### NFR1 — 1500-point render < 200ms (unit test evidence)

From `HistoryModal.test.tsx → ReplayChart → renders chart with 1500 points within 200ms`:
- Input: 1500 `HistoryPoint` objects, `isAnimationActive={false}`
- Measurement: `performance.now()` delta < 200ms
- Result: PASS

### AC4 — Accessibility

Modal element has:
- `role="dialog"` on the dialog div
- `aria-modal="true"`
- `aria-labelledby={titleId}` pointing to the `<h2>` with stream name
- `aria-label="Close history modal"` on the close button
- `aria-pressed` on granularity toggle buttons
- ESC closes via `keydown` listener (tested in `closes on ESC key press`)

Expected curl output when running against full stack:
```bash
# Fetch history for stream-001 — last 30 minutes
$ curl -s "http://localhost:8080/api/v1/streams/stream-001/history?from=$FROM&to=$TO&granularity=MINUTE" | jq 'length'
30

# Fetch alerts for stream-001 — same window
$ curl -s "http://localhost:8080/api/v1/streams/stream-001/alerts?from=$FROM&to=$TO" | jq 'length'
3
```

### Lint

```
> frontend@0.0.0 lint
> eslint .
(no output = zero violations)
```
