package com.streamflow.processor.aggregator;

import org.springframework.stereotype.Component;

/**
 * Computes a composite stream-health score in the range [0, 100].
 *
 * <p>SPEC-09 R3 — algorithm (applied in order, each penalty capped individually):
 * <ol>
 *   <li>Start at 100.</li>
 *   <li>Subtract {@code bufferRatePct × 5}, capped at −40.
 *       A 0 % buffer rate removes nothing; an 8 % rate removes 40 points.</li>
 *   <li>Subtract {@code frameDropRate × 1 000}, capped at −30.
 *       A 0.03 frame-drop rate removes 30 points (cap).</li>
 *   <li>Subtract {@code max(0, encoderLatencyMs − 150) / 10}, capped at −20.
 *       Latencies below 150 ms are free; 350 ms removes 20 points (cap).</li>
 *   <li>Subtract {@code max(0, 3 000 − bitrateKbps) / 100}, capped at −10.
 *       Bitrates ≥ 3 000 kbps are free; 2 000 kbps removes 10 points (cap).</li>
 *   <li>Floor result at 0; round to one decimal place.</li>
 * </ol>
 *
 * <p>This class is intentionally kept pure (no Spring dependencies beyond {@code @Component})
 * and is heavily unit-tested in {@code HealthScoreCalculatorTest}.
 */
@Component
public class HealthScoreCalculator {

    // ── Penalty caps (SPEC-09 R3) ─────────────────────────────────────────────

    /** Maximum penalty for buffer rate (−40 points). */
    static final double BUFFER_PENALTY_CAP        = 40.0;

    /** Maximum penalty for frame-drop rate (−30 points). */
    static final double FRAME_DROP_PENALTY_CAP    = 30.0;

    /** Maximum penalty for encoder latency (−20 points). */
    static final double LATENCY_PENALTY_CAP       = 20.0;

    /** Maximum penalty for low bitrate (−10 points). */
    static final double BITRATE_PENALTY_CAP       = 10.0;

    /** Encoder latency (ms) below which no penalty is applied. */
    static final int LATENCY_FREE_THRESHOLD_MS    = 150;

    /** Bitrate (kbps) at or above which no penalty is applied. */
    static final int BITRATE_FREE_THRESHOLD_KBPS  = 3_000;

    /**
     * Computes the health score.
     *
     * @param bufferRatePct     percentage of events that are buffer-start events (0 – 100)
     * @param frameDropRate     fraction of frames dropped (0.0 – 1.0)
     * @param encoderLatencyMs  encoder latency in milliseconds (≥ 0)
     * @param bitrateKbps       current stream bitrate in kbps (≥ 0)
     * @return health score in [0.0, 100.0] rounded to one decimal place
     */
    public double compute(double bufferRatePct,
                          double frameDropRate,
                          int encoderLatencyMs,
                          int bitrateKbps) {

        double score = 100.0;

        // Penalty 1: buffer rate
        double bufferPenalty = Math.min(bufferRatePct * 5.0, BUFFER_PENALTY_CAP);
        score -= bufferPenalty;

        // Penalty 2: frame-drop rate
        double frameDropPenalty = Math.min(frameDropRate * 1_000.0, FRAME_DROP_PENALTY_CAP);
        score -= frameDropPenalty;

        // Penalty 3: encoder latency exceeding 150 ms
        double latencyExcess = Math.max(0, encoderLatencyMs - LATENCY_FREE_THRESHOLD_MS);
        double latencyPenalty = Math.min(latencyExcess / 10.0, LATENCY_PENALTY_CAP);
        score -= latencyPenalty;

        // Penalty 4: bitrate below 3 000 kbps
        double bitrateDeficit = Math.max(0, BITRATE_FREE_THRESHOLD_KBPS - bitrateKbps);
        double bitratePenalty = Math.min(bitrateDeficit / 100.0, BITRATE_PENALTY_CAP);
        score -= bitratePenalty;

        // Floor at 0 and round to 1 decimal place
        score = Math.max(0.0, score);
        return Math.round(score * 10.0) / 10.0;
    }
}
