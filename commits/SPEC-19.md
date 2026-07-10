# Commit Plan — SPEC-19: Frontend History Modal + Replay Chart

Suggested branch: `feat/spec-19-frontend-history-modal`

---

## Commit 1 — Add HistoryPoint type and history/alerts API functions

**Message:**
```
SPEC-19: add HistoryPoint type and REST fetch functions for history/alerts

Adds HistoryGranularity and HistoryPoint to stream.types.ts (mirrors
StreamMetricSnapshotDTO subset used for replay).  Adds fetchStreamHistory
and fetchStreamAlerts to api.ts, calling the two SPEC-18 endpoints.

Refs: specs/SPEC-19-frontend-history-modal-and-replay.md
```

**Files:**
- `frontend/src/types/stream.types.ts`
- `frontend/src/services/api.ts`

**Stage command:**
```bash
git add frontend/src/types/stream.types.ts frontend/src/services/api.ts
```

---

## Commit 2 — Add useStreamHistory hook with LRU cache

**Message:**
```
SPEC-19: add useStreamHistory hook with parallel fetch and LRU cache

Imperative fetch hook that calls fetchStreamHistory + fetchStreamAlerts in
parallel via Promise.all.  A 5-entry insertion-order Map (stored in useRef)
acts as an LRU cache keyed by (streamId,from,to,granularity) to avoid
duplicate network calls on rapid range tweaks.

Refs: specs/SPEC-19-frontend-history-modal-and-replay.md
```

**Files:**
- `frontend/src/hooks/useStreamHistory.ts`

**Stage command:**
```bash
git add frontend/src/hooks/useStreamHistory.ts
```

---

## Commit 3 — Add ReplayChart component

**Message:**
```
SPEC-19: add ReplayChart with viewer-count area, buffer-rate line, alert dots

Recharts ComposedChart with:
- Area (left Y) for liveViewerCount with gradient fill
- Line (right Y) for bufferRatePct
- Scatter dots for alert events coloured by severity (CRITICAL=red,
  WARNING=amber, INFO=blue); clicking a dot shows an alert detail banner
- isAnimationActive={false} for datasets > 200 points (NFR1)
- Empty-state "No data in this range" when data=[]

Refs: specs/SPEC-19-frontend-history-modal-and-replay.md
```

**Files:**
- `frontend/src/components/history/ReplayChart.tsx`

**Stage command:**
```bash
git add frontend/src/components/history/ReplayChart.tsx
```

---

## Commit 4 — Add HistoryModal component

**Message:**
```
SPEC-19: add HistoryModal with datetime-local inputs and focus trap

HistoryModal features:
- Two datetime-local inputs (from/to), defaulting to last 1 hour
- Granularity toggle MINUTE/HOUR; auto-switches to HOUR when range > 2h
- On submit: calls useStreamHistory; shows loading skeleton or ReplayChart
- Error toast via react-hot-toast on API failure
- Focus trap via keydown listener; ESC closes; aria-modal, aria-labelledby

Uses native <input type="datetime-local"> (no react-day-picker dependency).

Refs: specs/SPEC-19-frontend-history-modal-and-replay.md
```

**Files:**
- `frontend/src/components/history/HistoryModal.tsx`

**Stage command:**
```bash
git add frontend/src/components/history/HistoryModal.tsx
```

---

## Commit 5 — Wire History button into StreamCard

**Message:**
```
SPEC-19: add History button to StreamCard, open HistoryModal on click

Adds a "History" button below StreamControls in each StreamCard.
State (historyOpen) is local to the card.  HistoryModal is rendered
conditionally inside the card's JSX; closing sets historyOpen=false.

Refs: specs/SPEC-19-frontend-history-modal-and-replay.md
```

**Files:**
- `frontend/src/components/stream/StreamCard.tsx`

**Stage command:**
```bash
git add frontend/src/components/stream/StreamCard.tsx
```

---

## Commit 6 — Add tests for useStreamHistory and HistoryModal/ReplayChart

**Message:**
```
SPEC-19: add tests for useStreamHistory hook and HistoryModal/ReplayChart

useStreamHistory.test.ts (5 cases):
- initial state, parallel fetch, loading flag, error state,
  LRU cache hit skips network, different-params cache miss

HistoryModal.test.tsx (11 cases):
- modal renders with title, close button, backdrop click, ESC key,
  form inputs, submit params, loading skeleton, empty state,
  granularity toggle, ReplayChart 60-point render,
  ReplayChart 1500-point render < 200ms (NFR1)

All 104 tests pass; lint clean; tsc strict.

Refs: specs/SPEC-19-frontend-history-modal-and-replay.md
```

**Files:**
- `frontend/src/test/useStreamHistory.test.ts`
- `frontend/src/test/HistoryModal.test.tsx`

**Stage command:**
```bash
git add frontend/src/test/useStreamHistory.test.ts frontend/src/test/HistoryModal.test.tsx
```

---

## Commit 7 — Mark SPEC-19 Done and write commit plan

**Message:**
```
SPEC-19: mark spec Done, add evidence section and commit plan

Updates spec status to Done, ticks all ACs and DoD checkboxes,
adds Evidence section with build output, test results, and
accessibility notes.

Refs: specs/SPEC-19-frontend-history-modal-and-replay.md
```

**Files:**
- `specs/SPEC-19-frontend-history-modal-and-replay.md`
- `commits/SPEC-19.md`

**Stage command:**
```bash
git add specs/SPEC-19-frontend-history-modal-and-replay.md commits/SPEC-19.md
```

---

## Verification before pushing

- [ ] `npm --prefix frontend run lint` — zero violations
- [ ] `npm --prefix frontend run test` — 104 tests, 12 files, all pass
- [ ] `npm --prefix frontend run build` — `tsc -b` clean, Vite build success
- [ ] Demo evidence in spec matches reality
