# Commit Plan — SPEC-15: Frontend Health & Quality Panels

Suggested branch: `feat/spec-15-health-quality-panels`

---

## Commit 1 — Add health color tokens to Tailwind config

**Message:**
```
SPEC-15: add health color tokens to tailwind.config.js

Add health.good / health.warn / health.bad color tokens to the Tailwind
theme extension. These map to green-500, yellow-500, and red-500 hex values
and are referenced by HealthGauge and BufferRateBadge for consistent
color semantics across the dashboard.

Refs: specs/SPEC-15-frontend-health-and-quality-panels.md
```

**Files:**
- `frontend/tailwind.config.js`

**Stage command:**
```bash
git add frontend/tailwind.config.js
```

---

## Commit 2 — Add HealthGauge component

**Message:**
```
SPEC-15: add HealthGauge component with color ramp

Recharts RadialBarChart showing health score 0-100 with a color ramp:
green (>= 85), yellow (60-84), red (< 60). Numeric score overlaid in
the center via absolute-positioned div so color is not the sole indicator.
Memoized with React.memo. aria-label and role="img" for accessibility.

Refs: specs/SPEC-15-frontend-health-and-quality-panels.md
```

**Files:**
- `frontend/src/components/stream/HealthGauge.tsx`

**Stage command:**
```bash
git add frontend/src/components/stream/HealthGauge.tsx
```

---

## Commit 3 — Add QualityDistBar component

**Message:**
```
SPEC-15: add QualityDistBar 100%-stacked horizontal bar

Recharts BarChart with 5 stacked Bar segments (1080p, 720p, 480p, 360p,
144p). LabelList shows tier label inside segments >= 8% wide; tooltip
shows raw percentage. Legend dots below the bar ensure color is not the
sole indicator. Memoized with React.memo.

Refs: specs/SPEC-15-frontend-health-and-quality-panels.md
```

**Files:**
- `frontend/src/components/stream/QualityDistBar.tsx`

**Stage command:**
```bash
git add frontend/src/components/stream/QualityDistBar.tsx
```

---

## Commit 4 — Add BufferRateBadge component

**Message:**
```
SPEC-15: add BufferRateBadge pill with pulse on critical

Color-coded pill: green (< 2%), yellow (2-5%), red (> 5%). Animates
animate-pulse when red; suppressed via motion-reduce:animate-none for
users who prefer reduced motion. Includes "buf" text label alongside
the numeric value. aria-label for accessibility. Memoized.

Refs: specs/SPEC-15-frontend-health-and-quality-panels.md
```

**Files:**
- `frontend/src/components/stream/BufferRateBadge.tsx`

**Stage command:**
```bash
git add frontend/src/components/stream/BufferRateBadge.tsx
```

---

## Commit 5 — Update StreamCard layout (SPEC-15 R4)

**Message:**
```
SPEC-15: update StreamCard layout with 3-row design

Row 1: name + LiveDot + BufferRateBadge (header)
Row 2: ViewerCountChart (full width)
Row 3: HealthGauge | QualityDistBar (50/50 columns)

Buffer rate is now shown as a badge in the header rather than a plain
MetricCard. Badge is only rendered when snapshot is non-null.

Refs: specs/SPEC-15-frontend-health-and-quality-panels.md
```

**Files:**
- `frontend/src/components/stream/StreamCard.tsx`

**Stage command:**
```bash
git add frontend/src/components/stream/StreamCard.tsx
```

---

## Commit 6 — Add RTL tests for new components + update StreamCard tests

**Message:**
```
SPEC-15: add RTL tests for HealthGauge, QualityDistBar, BufferRateBadge

HealthGauge (11 tests): color band boundaries (85, 60, 59), clamping,
aria-label, center score text.
QualityDistBar (6 tests): aria-label, legend entries, empty distribution.
BufferRateBadge (13 tests): color bands, pulse on red, boundary values.
StreamCard.test.tsx (5 new tests): integration tests for SPEC-15 layout.
Total: 57 tests passing (up from 21).

Refs: specs/SPEC-15-frontend-health-and-quality-panels.md
```

**Files:**
- `frontend/src/test/HealthGauge.test.tsx`
- `frontend/src/test/QualityDistBar.test.tsx`
- `frontend/src/test/BufferRateBadge.test.tsx`
- `frontend/src/test/StreamCard.test.tsx`

**Stage command:**
```bash
git add frontend/src/test/HealthGauge.test.tsx \
        frontend/src/test/QualityDistBar.test.tsx \
        frontend/src/test/BufferRateBadge.test.tsx \
        frontend/src/test/StreamCard.test.tsx
```

---

## Commit 7 — Mark SPEC-15 Done

**Message:**
```
SPEC-15: mark spec Done, add evidence section

Update Status to Done, tick all ACs and DoD checkboxes, add §10 Evidence
with test output, lint output, build output, and per-AC verification.

Refs: specs/SPEC-15-frontend-health-and-quality-panels.md
```

**Files:**
- `specs/SPEC-15-frontend-health-and-quality-panels.md`
- `commits/SPEC-15.md`

**Stage command:**
```bash
git add specs/SPEC-15-frontend-health-and-quality-panels.md commits/SPEC-15.md
```

---

## Verification before pushing

- [ ] `npm --prefix frontend run lint` — exit 0, no output
- [ ] `npm --prefix frontend run test` — 57 tests passing (6 test files)
- [ ] `npm --prefix frontend run build` — ✓ built in ~800ms
- [ ] Demo evidence in spec §10 matches reality
