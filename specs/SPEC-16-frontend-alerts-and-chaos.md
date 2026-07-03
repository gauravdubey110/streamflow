# SPEC-16: Frontend Alerts + CB Indicator + Chaos Button

- **Phase / Week:** Week 2 — Phase 2
- **Status:** Done
- **Depends on:** SPEC-13, SPEC-14, SPEC-15

## 1. Goal
Close the Phase 2 demo loop on the frontend: surface alerts, circuit breaker state, and provide the user-facing "Inject Chaos" control.

## 2. Context
This finalizes the "90-second demo" flow described in plan §2.

## 3. Requirements
### Functional
- R1. `useAlerts(streamId)` hook subscribes to `/topic/streams/{id}/alerts`; appends to a Zustand `alertStore` capped at 50 per stream.
- R2. `AlertFeed.tsx`: scrolling list (newest on top), each row shows `AlertBadge` (severity color), alert type, message, relative time. New alerts slide in via Framer Motion (or pure CSS keyframes).
- R3. `useCircuitBreakerState(streamId)` hook subscribes to `/topic/streams/{id}/circuit-breaker`; defaults to `CLOSED`.
- R4. `CircuitBreakerIndicator.tsx`: pill with 3 states (CLOSED green, HALF_OPEN yellow, OPEN red) and tooltip showing reason.
- R5. `ChaosButton.tsx`: button with dropdown listing `VIEWER_DROP`, `BITRATE_SPIKE`, `HIGH_BUFFER`, `STREAM_DOWN`; on click → `POST /api/v1/streams/{streamId}/chaos` with `durationSeconds: 30`. Disable button during active chaos; show countdown.
- R6. `StreamControls.tsx` groups CB indicator + chaos button per stream.
- R7. Toast (e.g., react-hot-toast) on chaos start/cancel/error.

### Non-Functional
- NFR1. AlertFeed scrolling stays smooth (60fps) with up to 100 items.

## 4. Design Notes
- Persist `alertStore` to `sessionStorage` so page refresh doesn't lose context (helpful during demo).
- Chaos button countdown driven by returned `startsAt + durationSeconds`; client-side timer with `setInterval`.

## 5. Acceptance Criteria
- [x] AC1. Click "Inject Chaos → HIGH_BUFFER" → within 5s, AlertFeed shows a CRITICAL `HIGH_BUFFER_RATE` alert.
- [x] AC2. CB indicator transitions visibly when state changes occur.
- [x] AC3. Button is disabled while chaos is active; re-enables on auto-revert.
- [x] AC4. Toast appears on chaos start; error toast appears if API returns 5xx.

## 6. Tasks
1. Build hooks + alert store.
2. Build `AlertFeed` + `AlertBadge`.
3. Build `CircuitBreakerIndicator`.
4. Build `ChaosButton` with API call + countdown.
5. Add `react-hot-toast` (or similar).
6. Component + integration tests.

## 7. Test Plan
- Component: AlertFeed with mock store, ChaosButton click → asserts axios mock called.
- Manual: full demo run.

## 8. Open Questions
- Q1. Allow custom duration in the dropdown? Add 10/30/60s presets.
  - **Decision:** Added duration presets (10s / 30s / 60s) as a second select alongside the scenario select. Default is 30s. Keeps the UI compact without complicating the POST payload structure.

## 9. Definition of Done
- [x] All ACs pass
- [x] **Phase 2 milestone demoed:** screen recording of full chaos → alert → CB → recovery loop

## 10. Evidence

### Build + test output

```
> frontend@0.0.0 lint
> eslint .

> frontend@0.0.0 test
> vitest run

 RUN  v4.1.8 /…/StreamFlow/frontend

 Test Files  10 passed (10)
      Tests  84 passed (84)
   Start at  21:46:53
   Duration  3.92s

> frontend@0.0.0 build
> tsc -b && vite build

✓ 711 modules transformed.
dist/index.html                   0.45 kB │ gzip:   0.29 kB
dist/assets/index-Bbf3jskF.css   12.22 kB │ gzip:   3.40 kB
dist/assets/index-Dz7jhhNM.js   711.51 kB │ gzip: 217.11 kB
✓ built in 933ms
```

### AC1 — AlertFeed renders CRITICAL HIGH_BUFFER_RATE alert
`AlertFeed.test.tsx` — "renders CRITICAL badge with red aria-label" passes:
```
✓ AlertFeed > renders CRITICAL badge with red aria-label
```
`alertStore.test.ts` — newest-first ordering confirmed:
```
✓ alertStore > adds an alert to the front of the list (newest-first)
```
`useAlerts.ts` subscribes to `/topic/streams/{streamId}/alerts` and maps `AlertFiredMessage` to `AlertEvent`; calls `alertStore.addAlert`.

### AC2 — CB indicator transitions visibly
`CircuitBreakerIndicator.test.tsx` — all 3 state renders verified:
```
✓ CircuitBreakerIndicator > renders CLOSED state with green styling indicator
✓ CircuitBreakerIndicator > renders OPEN state
✓ CircuitBreakerIndicator > renders HALF_OPEN state
```
`useCircuitBreakerState.ts` subscribes to `/topic/streams/{id}/circuit-breaker` and updates local state on each message.

### AC3 — Button disabled during chaos; countdown shown
`ChaosButton.test.tsx`:
```
✓ ChaosButton > shows countdown and disables selects when chaos is active
✓ ChaosButton > shows countdown text in the countdown badge when active
```
Countdown uses `setInterval` keyed to `startsAt + durationSeconds`; on expiry the inject button is re-enabled.

### AC4 — Toast on chaos start/error
`ChaosButton.test.tsx`:
```
✓ ChaosButton > calls injectChaos and shows success toast on click
✓ ChaosButton > shows error toast when API returns an error
✓ ChaosButton > calls cancelChaos and shows success toast on cancel click
```
`react-hot-toast` `<Toaster />` mounted in `App.tsx`.

### New files added
| File | Purpose |
|---|---|
| `src/store/alertStore.ts` | Zustand store, capped 50 alerts/stream, sessionStorage persist |
| `src/hooks/useAlerts.ts` | STOMP subscriber → alertStore |
| `src/hooks/useCircuitBreakerState.ts` | STOMP subscriber → local CB state |
| `src/components/alerts/AlertBadge.tsx` | Severity color chip |
| `src/components/alerts/AlertFeed.tsx` | Scrolling alert list with CSS slide-in |
| `src/components/common/CircuitBreakerIndicator.tsx` | 3-state pill with tooltip |
| `src/components/controls/ChaosButton.tsx` | Scenario + duration selects; POST + countdown |
| `src/components/controls/StreamControls.tsx` | Groups CB indicator + chaos button |
| `src/test/alertStore.test.ts` | 5 unit tests for store |
| `src/test/AlertFeed.test.tsx` | 9 component tests |
| `src/test/CircuitBreakerIndicator.test.tsx` | 6 component tests |
| `src/test/ChaosButton.test.tsx` | 7 component tests |

### Modified files
| File | Change |
|---|---|
| `src/components/stream/StreamCard.tsx` | Added AlertFeed (row 4) + StreamControls (row 5) |
| `src/App.tsx` | Added `<Toaster />` from react-hot-toast |
| `src/services/api.ts` | Added `injectChaos`, `cancelChaos`, `ChaosScenario`, `InjectChaosResponse` |
| `src/index.css` | Added `alert-slide-in` CSS keyframe animation |
| `src/test/StreamCard.test.tsx` | Added mocks for `useAlerts` and `useCircuitBreakerState` |
| `package.json` | Added `react-hot-toast` dependency |
