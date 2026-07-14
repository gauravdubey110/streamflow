package com.streamflow.processor.aggregator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HealthScoreCalculator}.
 *
 * <p>SPEC-09 Test Plan — 6 unit cases:
 *
 * <ol>
 *   <li>Perfect conditions → 100.0
 *   <li>Mild buffer rate (4 %) → score between 70 and 100
 *   <li>High encoder latency (350 ms) → latency penalty capped at 20
 *   <li>Low bitrate (2 000 kbps) → bitrate penalty capped at 10
 *   <li>Combined degradation → score &lt; 60 (SPEC-09 Test Plan requirement)
 *   <li>Extreme values → floor at 0.0
 * </ol>
 */
class HealthScoreCalculatorTest {

  private HealthScoreCalculator calculator;

  @BeforeEach
  void setUp() {
    calculator = new HealthScoreCalculator();
  }

  // ── Case 1: Perfect conditions ────────────────────────────────────────────

  /**
   * Zero buffer rate, no frame drops, latency ≤ 150 ms, bitrate ≥ 3 000 kbps → all penalties are
   * zero → score = 100.0.
   */
  @Test
  void case1_perfectConditions_returnsHundred() {
    double score = calculator.compute(0.0, 0.0, 120, 4_500);

    assertThat(score).as("Perfect conditions should yield 100.0").isEqualTo(100.0);
  }

  // ── Case 2: Mild buffer rate ──────────────────────────────────────────────

  /** Buffer rate = 4 % → penalty = 4 × 5 = 20 → score = 80.0. Score must be in range (70, 100). */
  @Test
  void case2_mildBufferRate_scoreReducedButAbove70() {
    double score = calculator.compute(4.0, 0.0, 120, 4_500);

    assertThat(score)
        .as("Mild 4% buffer rate should produce score in (70, 100)")
        .isGreaterThan(70.0)
        .isLessThan(100.0);

    // Verify exact math: penalty = 4*5 = 20 → score = 80.0
    assertThat(score).isEqualTo(80.0);
  }

  // ── Case 3: High encoder latency ─────────────────────────────────────────

  /**
   * Latency = 350 ms → excess = 200 ms → penalty = 200/10 = 20 (cap). Score = 100 − 20 = 80.0. Any
   * latency higher than 350 ms should still yield exactly 80.0 (cap applies).
   */
  @Test
  void case3_highEncoderLatency_penaltyCappedAt20() {
    // At exactly the cap boundary: 150 + 200 = 350 ms → penalty = 20
    double scoreAtCap = calculator.compute(0.0, 0.0, 350, 4_500);
    assertThat(scoreAtCap)
        .as("Latency 350 ms should give exactly 80.0 (cap of 20 applied)")
        .isEqualTo(80.0);

    // Beyond the cap: 500 ms → excess = 350, raw penalty = 35, capped at 20
    double scoreAboveCap = calculator.compute(0.0, 0.0, 500, 4_500);
    assertThat(scoreAboveCap)
        .as("Latency 500 ms must still give 80.0 (cap prevents further deduction)")
        .isEqualTo(80.0);

    // Sub-threshold: 149 ms → no penalty
    double scoreBelow = calculator.compute(0.0, 0.0, 149, 4_500);
    assertThat(scoreBelow)
        .as("Latency 149 ms (below threshold) should give 100.0")
        .isEqualTo(100.0);
  }

  // ── Case 4: Low bitrate ──────────────────────────────────────────────────

  /**
   * Bitrate = 2 000 kbps → deficit = 1 000 → penalty = 1 000/100 = 10 (cap). Score = 100 − 10 =
   * 90.0.
   */
  @Test
  void case4_lowBitrate_penaltyCappedAt10() {
    // At exactly the cap boundary: 3000 − 2000 = 1000 → penalty = 10
    double score = calculator.compute(0.0, 0.0, 120, 2_000);
    assertThat(score).as("Bitrate 2000 kbps should give 90.0 (cap of 10 applied)").isEqualTo(90.0);

    // Below cap: deficit < 1000 → partial penalty
    double scorePartial = calculator.compute(0.0, 0.0, 120, 2_500);
    assertThat(scorePartial).as("Bitrate 2500 kbps should give 95.0 (penalty = 5)").isEqualTo(95.0);

    // Above free threshold: no penalty
    double scoreAtThreshold = calculator.compute(0.0, 0.0, 120, 3_000);
    assertThat(scoreAtThreshold)
        .as("Bitrate 3000 kbps (at threshold) should give 100.0")
        .isEqualTo(100.0);
  }

  // ── Case 5: Combined degradation → score < 60 ────────────────────────────

  /**
   * SPEC-09 Test Plan requirement: push a degraded event and assert score < 60.
   *
   * <p>Chosen inputs: buffer=8%, frameDrop=0.02, latency=250 ms, bitrate=2000 kbps.
   *
   * <ul>
   *   <li>Buffer penalty: 8 × 5 = 40 (capped at 40)
   *   <li>Frame-drop penalty: 0.02 × 1000 = 20 (no cap)
   *   <li>Latency penalty: (250−150)/10 = 10 (no cap)
   *   <li>Bitrate penalty: (3000−2000)/100 = 10 (capped at 10)
   *   <li>Total deduction = 80 → score = 100 − 80 = 20.0
   * </ul>
   */
  @Test
  void case5_combinedDegradation_scoreBelow60() {
    double score = calculator.compute(8.0, 0.02, 250, 2_000);

    assertThat(score).as("Combined degradation should push score below 60").isLessThan(60.0);

    // Verify exact math (no rounding needed here since result is a whole number)
    assertThat(score).as("Combined degradation: 100 − 40 − 20 − 10 − 10 = 20.0").isEqualTo(20.0);
  }

  // ── Case 6: Extreme values → floor at 0 ──────────────────────────────────

  /**
   * All penalties at or beyond their individual caps → total deduction = 100. Score must be floored
   * at 0.0, not negative.
   */
  @Test
  void case6_extremeValues_floorAtZero() {
    // bufferRatePct=20 → penalty 100, capped at 40
    // frameDropRate=1.0 → penalty 1000, capped at 30
    // latency=9999 → penalty >> 20, capped at 20
    // bitrate=0 → deficit=3000, penalty=30, capped at 10
    // Total capped penalty = 40+30+20+10 = 100 → raw score = 0.0
    double score = calculator.compute(20.0, 1.0, 9_999, 0);

    assertThat(score)
        .as("All penalties at max cap should yield exactly 0.0 (not negative)")
        .isEqualTo(0.0);
  }

  // ── Frame-drop cap verification ───────────────────────────────────────────

  /**
   * Frame-drop rate = 0.03 → raw penalty = 30 (exactly at cap) → score = 70.0. Frame-drop rate =
   * 0.05 → raw penalty = 50, capped at 30 → score still 70.0.
   */
  @Test
  void frameDrop_penaltyCappedAt30() {
    double atCap = calculator.compute(0.0, 0.03, 120, 4_500);
    assertThat(atCap)
        .as("frameDrop 0.03 should give exactly 70.0 (30 point penalty)")
        .isEqualTo(70.0);

    double aboveCap = calculator.compute(0.0, 0.05, 120, 4_500);
    assertThat(aboveCap).as("frameDrop 0.05 (above cap) should still give 70.0").isEqualTo(70.0);
  }

  // ── Rounding to one decimal ───────────────────────────────────────────────

  /**
   * Verify that the result is rounded to exactly one decimal place. Buffer = 1.1 % → penalty = 5.5
   * → score = 94.5 (one decimal place).
   */
  @Test
  void result_isRoundedToOneDecimalPlace() {
    double score = calculator.compute(1.1, 0.0, 120, 4_500);

    // penalty = 1.1 * 5 = 5.5 → score = 94.5
    assertThat(score)
        .as("Result should be rounded to one decimal place")
        .isCloseTo(94.5, within(0.001));
  }
}
