# Commit Plan — SPEC-16: Frontend Alerts + CB Indicator + Chaos Button

Suggested branch: `feat/spec-16-frontend-alerts-and-chaos`

---

## Commit 1 — Add alertStore and alert hooks

**Message:**
```
SPEC-16: add alertStore, useAlerts, useCircuitBreakerState hooks

Zustand alertStore persists alerts to sessionStorage (capped at 50
per stream, newest-first, deduplicated by alertId).

useAlerts subscribes to /topic/streams/{id}/alerts and maps
AlertFiredMessage → AlertEvent via useWebSocket.

useCircuitBreakerState subscribes to /topic/streams/{id}/circuit-breaker
and tracks current CB state, defaulting to CLOSED.

Refs: specs/SPEC-16-frontend-alerts-and-chaos.md
```

**Files:**
- `frontend/src/store/alertStore.ts`
- `frontend/src/hooks/useAlerts.ts`
- `frontend/src/hooks/useCircuitBreakerState.ts`

**Stage command:**
```bash
git add frontend/src/store/alertStore.ts \
        frontend/src/hooks/useAlerts.ts \
        frontend/src/hooks/useCircuitBreakerState.ts
```

---

## Commit 2 — Add AlertBadge, AlertFeed, CircuitBreakerIndicator components

**Message:**
```
SPEC-16: add AlertBadge, AlertFeed, CircuitBreakerIndicator components

AlertBadge: severity color chip (CRITICAL=red, WARNING=amber, INFO=blue).
AlertFeed: scrolling list newest-first, capped at 100 displayed rows,
CSS slide-in keyframe animation, relative timestamp formatting.
CircuitBreakerIndicator: pill with 3 states (green/yellow/red) and
title tooltip showing the reason string.
index.css: add alert-slide-in keyframe.

Refs: specs/SPEC-16-frontend-alerts-and-chaos.md
```

**Files:**
- `frontend/src/components/alerts/AlertBadge.tsx`
- `frontend/src/components/alerts/AlertFeed.tsx`
- `frontend/src/components/common/CircuitBreakerIndicator.tsx`
- `frontend/src/index.css`

**Stage command:**
```bash
git add frontend/src/components/alerts/AlertBadge.tsx \
        frontend/src/components/alerts/AlertFeed.tsx \
        frontend/src/components/common/CircuitBreakerIndicator.tsx \
        frontend/src/index.css
```

---

## Commit 3 — Add ChaosButton, StreamControls; update api.ts

**Message:**
```
SPEC-16: add ChaosButton and StreamControls; extend api.ts

ChaosButton: scenario select (4 options) + duration select (10/30/60s
presets), POSTs to /api/v1/streams/{id}/chaos, shows client-side
countdown during active chaos, Cancel button calls DELETE endpoint,
react-hot-toast notifications on start/cancel/error.

StreamControls: groups CircuitBreakerIndicator + ChaosButton per stream.

api.ts: add injectChaos(), cancelChaos(), ChaosScenario type,
InjectChaosResponse interface.

Refs: specs/SPEC-16-frontend-alerts-and-chaos.md
```

**Files:**
- `frontend/src/components/controls/ChaosButton.tsx`
- `frontend/src/components/controls/StreamControls.tsx`
- `frontend/src/services/api.ts`
- `frontend/package.json`
- `frontend/package-lock.json`

**Stage command:**
```bash
git add frontend/src/components/controls/ChaosButton.tsx \
        frontend/src/components/controls/StreamControls.tsx \
        frontend/src/services/api.ts \
        frontend/package.json \
        frontend/package-lock.json
```

---

## Commit 4 — Integrate AlertFeed + StreamControls into StreamCard; add Toaster to App

**Message:**
```
SPEC-16: integrate AlertFeed and StreamControls into StreamCard layout

StreamCard: add Row 4 (AlertFeed) and Row 5 (StreamControls) below the
existing health/quality row. Imports useAlerts hook.

App.tsx: mount react-hot-toast <Toaster /> with dark theme options.

Refs: specs/SPEC-16-frontend-alerts-and-chaos.md
```

**Files:**
- `frontend/src/components/stream/StreamCard.tsx`
- `frontend/src/App.tsx`

**Stage command:**
```bash
git add frontend/src/components/stream/StreamCard.tsx \
        frontend/src/App.tsx
```

---

## Commit 5 — Add SPEC-16 tests

**Message:**
```
SPEC-16: add component and store tests (84 tests passing)

alertStore.test.ts: 5 unit tests — newest-first ordering, cap at 50,
dedup by alertId, clearAlerts, per-stream isolation.

AlertFeed.test.tsx: 9 tests — empty state, badge colors, alert type
formatting, display cap at 100, aria-label.

CircuitBreakerIndicator.test.tsx: 6 tests — all 3 states, tooltip,
aria-label.

ChaosButton.test.tsx: 7 tests — renders, API call on inject, countdown
appears, cancel calls DELETE, error toast on API failure.

StreamCard.test.tsx: added mocks for useAlerts and useCircuitBreakerState
so existing 17 tests continue to pass without side effects.

Refs: specs/SPEC-16-frontend-alerts-and-chaos.md
```

**Files:**
- `frontend/src/test/alertStore.test.ts`
- `frontend/src/test/AlertFeed.test.tsx`
- `frontend/src/test/CircuitBreakerIndicator.test.tsx`
- `frontend/src/test/ChaosButton.test.tsx`
- `frontend/src/test/StreamCard.test.tsx`

**Stage command:**
```bash
git add frontend/src/test/alertStore.test.ts \
        frontend/src/test/AlertFeed.test.tsx \
        frontend/src/test/CircuitBreakerIndicator.test.tsx \
        frontend/src/test/ChaosButton.test.tsx \
        frontend/src/test/StreamCard.test.tsx
```

---

## Commit 6 — Mark SPEC-16 Done with evidence

**Message:**
```
SPEC-16: mark spec Done, add evidence section

All 84 tests pass; lint clean; TypeScript build green.

Refs: specs/SPEC-16-frontend-alerts-and-chaos.md
```

**Files:**
- `specs/SPEC-16-frontend-alerts-and-chaos.md`
- `commits/SPEC-16.md`

**Stage command:**
```bash
git add specs/SPEC-16-frontend-alerts-and-chaos.md \
        commits/SPEC-16.md
```

---

## Verification before pushing

- [ ] `npm --prefix frontend run lint` — no errors
- [ ] `npm --prefix frontend test` — 84/84 tests pass
- [ ] `npm --prefix frontend run build` — TypeScript clean, Vite build green
- [ ] Demo evidence in spec matches reality (test output pasted verbatim)
