package com.streamflow.api.exception;

/**
 * Thrown when a history query's time range exceeds the configured maximum (default 24 hours,
 * controlled by {@code streamflow.history.max-range-hours}).
 *
 * <p>SPEC-18 R3: mapped to HTTP 400 with a problem-details JSON body by {@link
 * GlobalExceptionHandler#handleHistoryRange(HistoryRangeException)}.
 */
public class HistoryRangeException extends RuntimeException {

  private final long fromMs;
  private final long toMs;
  private final int maxHours;

  /**
   * @param fromMs requested start of range (epoch-milliseconds)
   * @param toMs requested end of range (epoch-milliseconds)
   * @param maxHours configured maximum range in hours
   */
  public HistoryRangeException(long fromMs, long toMs, int maxHours) {
    super(
        String.format(
            "Requested range [%d, %d] spans %.1f hours which exceeds the maximum of %d hours.",
            fromMs, toMs, (toMs - fromMs) / 3_600_000.0, maxHours));
    this.fromMs = fromMs;
    this.toMs = toMs;
    this.maxHours = maxHours;
  }

  public long getFromMs() {
    return fromMs;
  }

  public long getToMs() {
    return toMs;
  }

  public int getMaxHours() {
    return maxHours;
  }
}
