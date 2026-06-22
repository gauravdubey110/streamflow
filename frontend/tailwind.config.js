/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      /**
       * Health color tokens — used by HealthGauge, BufferRateBadge.
       * Spec ref: SPEC-15 §4 Design Notes.
       */
      colors: {
        health: {
          /** Score ≥ 85 — healthy (green-500) */
          good: '#22c55e',
          /** Score 60–84 — degraded (yellow-500) */
          warn: '#eab308',
          /** Score < 60 — critical (red-500) */
          bad: '#ef4444',
        },
      },
    },
  },
  plugins: [],
}

