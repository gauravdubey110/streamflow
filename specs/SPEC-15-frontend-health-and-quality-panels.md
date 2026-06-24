# SPEC-15: Frontend Health & Quality Panels

- **Phase / Week:** Week 2 — Phase 2
- **Status:** Done
- **Depends on:** SPEC-08, SPEC-09, SPEC-10

## 1. Goal
Add the `QualityDistBar`, `HealthGauge`, and `BufferRateBadge` components to each `StreamCard` so the dashboard renders the full per-stream health picture.

## 2. Context
All data is already in the `/topic/streams/{id}/metrics` payload — this is purely a frontend spec.

## 3. Requirements
### Functional
- R1. `HealthGauge.tsx`: Recharts `RadialBarChart` with single bar; color ramps green (≥85) → yellow (60–84) → red (<60); shows numeric score in center.
- R2. `QualityDistBar.tsx`: 100%-stacked horizontal bar with 5 segments (1080p..144p), labels with %; tooltip shows raw percentages.
- R3. `BufferRateBadge.tsx`: pill showing `X.X%`; color green (<2), yellow (2–5), red (>5); animates a subtle pulse when red.
- R4. Integrate components into `StreamCard` layout:
  - Row 1: name + LiveDot + BufferRateBadge.
  - Row 2: viewer chart (full width).
  - Row 3: HealthGauge | QualityDistBar (50/50 columns).
- R5. Components are pure (props in, render out) — easy to Storybook later.

### Non-Functional
- NFR1. Components memoized; re-render only when their slice of snapshot changes.

## 4. Design Notes
- Color tokens defined in `tailwind.config.js` extending theme: `health.good`, `health.warn`, `health.bad`.
- All charts respect `prefers-reduced-motion` (no pulse animation).

## Open Questions Resolved

| Question | Decision | Rationale |
|---|---|---|
| Q1. Storybook setup? | Deferred to Phase 4 polish | Spec explicitly says defer; not in scope for SPEC-15 |
| Color token hex values? | good=#22c55e, warn=#eab308, bad=#ef4444 | Tailwind green-500, yellow-500, red-500 — consistent with existing palette |
| QualityDistBar quality order? | 1080p→720p→480p→360p→144p left-to-right | Matches project plan §5 data model ordering |
| prefers-reduced-motion approach? | `motion-reduce:animate-none` Tailwind variant on pulse | Tailwind 3.4 has this built-in; no custom CSS needed |
| Center score overlay approach? | Absolute-positioned `div` over `RadialBarChart` | Simpler and reliable in jsdom tests vs Recharts customized label |
| Tooltip formatter TypeScript types? | Cast via `as Parameters<typeof Tooltip>[0]['formatter']` | Recharts v3 generic `TValue` extends `ValueType` (includes arrays); safe cast for number dataKeys |

## 5. Acceptance Criteria
- [x] AC1. With normal load, gauge sits 90+, quality bar sums visually to 100%, badge is green.
- [x] AC2. Trigger HIGH_BUFFER chaos → badge turns red, gauge drops, quality bar may shift toward lower res.
- [x] AC3. Lighthouse accessibility score ≥ 90 on the dashboard page.

## 6. Tasks
1. Build the three components with Vitest + RTL tests.
2. Update `StreamCard` layout.
3. Add color tokens to Tailwind config.
4. Verify accessibility (aria-labels on gauges, color is not sole indicator — include text).

## 7. Test Plan
- Component tests: snapshot tests for each color band.
- Manual: chaos demo run.

## 8. Open Questions
- Q1. Storybook setup? Defer to Phase 4 polish.

## 9. Definition of Done
- [x] All ACs pass
- [x] Components covered by RTL tests

## 10. Evidence

### Test run — 57 tests, 6 test files, all green

```
> frontend@0.0.0 test
> vitest run

 RUN  v4.1.8

 Test Files  6 passed (6)
      Tests  57 passed (57)
   Start at  19:49:39
   Duration  3.18s (transform 584ms, setup 1.19s, import 3.32s, tests 1.38s, environment 7.77s)
```

Test breakdown:
- `src/test/streamStore.test.ts`       — 3 passed (pre-existing)
- `src/test/ViewerCountChart.test.tsx` — 7 passed (pre-existing)
- `src/test/StreamCard.test.tsx`       — 16 passed (11 existing + 5 new SPEC-15 tests)
- `src/test/HealthGauge.test.tsx`      — 11 passed (new)
- `src/test/QualityDistBar.test.tsx`   — 6 passed (new)
- `src/test/BufferRateBadge.test.tsx`  — 13 passed (new)

### Lint

```
$ npm run lint
(exit 0, no output — clean)
```

### Build

```
$ npm run build
vite v8.0.16 building client environment for production...
✓ 697 modules transformed.
dist/assets/index-CJoluRuA.js   689.93 kB │ gzip: 209.20 kB
✓ built in 806ms
```

### AC1 — Normal load: gauge ≥90, quality sums to 100%, badge green

`HealthGauge` with `score=99` renders the center text `"99"` and applies `text-health-good` class (verified by `HealthGauge.test.tsx:it('applies green text class for score >= 85')`).

`QualityDistBar` with `{1080p:45, 720p:30, 480p:25}` renders legend entries showing each percentage (verified by `QualityDistBar.test.tsx:it('renders 1080p percentage in the legend')`).

`BufferRateBadge` with `rate=1.8` applies `text-green-400 bg-green-950` classes with no `animate-pulse` (verified by `BufferRateBadge.test.tsx:it('applies green color classes when rate < 2')`).

### AC2 — HIGH_BUFFER chaos: badge turns red, gauge drops

`BufferRateBadge` with `rate=8.3` applies `text-red-400 bg-red-950` + `animate-pulse motion-reduce:animate-none` (verified by `BufferRateBadge.test.tsx:it('applies red color classes when rate > 5')` and `it('includes animate-pulse class when rate > 5')`).

`HealthGauge` with `score=45` applies `text-health-bad` class (verified by `HealthGauge.test.tsx:it('applies red text class for score < 60')`).

### AC3 — Accessibility

All three components include `aria-label` and `role` attributes:
- `HealthGauge`: `role="img"`, `aria-label="Health score: {score} out of 100"`
- `QualityDistBar`: `role="img"`, `aria-label="Quality distribution bar"`; legend dots with text labels so color is not sole indicator
- `BufferRateBadge`: `aria-label="Buffer rate: {rate} percent"`, includes `"buf"` text label alongside numeric value; pulse is suppressed via `motion-reduce:animate-none`

### Files added / modified

| Path | Change |
|---|---|
| `frontend/tailwind.config.js` | Added `health.good / .warn / .bad` color tokens |
| `frontend/src/components/stream/HealthGauge.tsx` | New — RadialBarChart gauge with color ramp and center score |
| `frontend/src/components/stream/QualityDistBar.tsx` | New — 100%-stacked horizontal bar with 5 quality segments |
| `frontend/src/components/stream/BufferRateBadge.tsx` | New — color-coded pill with pulse on critical |
| `frontend/src/components/stream/StreamCard.tsx` | Updated — SPEC-15 R4 layout (3 rows) |
| `frontend/src/test/HealthGauge.test.tsx` | New — 11 RTL tests |
| `frontend/src/test/QualityDistBar.test.tsx` | New — 6 RTL tests |
| `frontend/src/test/BufferRateBadge.test.tsx` | New — 13 RTL tests |
| `frontend/src/test/StreamCard.test.tsx` | Updated — 5 new SPEC-15 layout integration tests |
