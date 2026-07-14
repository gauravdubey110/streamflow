package com.streamflow.api.repository;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for computing Cassandra partition-key bucket strings.
 *
 * <p>SPEC-18 R4: the {@code metric_snapshots} table partitions by {@code stream_id} only (no date
 * bucket), so snapshot queries only need the stream ID and the minute_bucket range. The {@code
 * viewer_events} and {@code alerts} tables partition by {@code (stream_id, date_bucket)}, so
 * queries must enumerate every bucket that could contain data in the requested time range.
 *
 * <p>All bucket strings are UTC.
 */
public final class BucketHelper {

  /** Hourly bucket format used for {@code viewer_events}: {@code yyyy-MM-dd-HH}. */
  public static final DateTimeFormatter HOUR_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneOffset.UTC);

  /** Daily bucket format used for {@code alerts}: {@code yyyy-MM-dd}. */
  public static final DateTimeFormatter DAY_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

  private BucketHelper() {}

  /**
   * Returns the list of {@code yyyy-MM-dd-HH} (hourly) bucket strings that span the closed interval
   * {@code [from, to]}.
   *
   * <p>Example: {@code from = 2024-06-03T13:45Z}, {@code to = 2024-06-03T15:10Z} → {@code
   * ["2024-06-03-13", "2024-06-03-14", "2024-06-03-15"]}.
   *
   * @param from start of range (inclusive), epoch-milliseconds
   * @param to end of range (inclusive), epoch-milliseconds
   * @return ordered list of hourly bucket strings; always contains at least one element
   */
  public static List<String> hourBuckets(long from, long to) {
    Instant start = Instant.ofEpochMilli(from).truncatedTo(ChronoUnit.HOURS);
    Instant end = Instant.ofEpochMilli(to).truncatedTo(ChronoUnit.HOURS);

    List<String> buckets = new ArrayList<>();
    Instant current = start;
    while (!current.isAfter(end)) {
      buckets.add(HOUR_FMT.format(current));
      current = current.plus(1, ChronoUnit.HOURS);
    }
    return buckets;
  }

  /**
   * Returns the list of {@code yyyy-MM-dd} (daily) bucket strings that span the closed interval
   * {@code [from, to]}.
   *
   * <p>Example: {@code from = 2024-06-03T22:00Z}, {@code to = 2024-06-04T02:00Z} → {@code
   * ["2024-06-03", "2024-06-04"]}.
   *
   * @param from start of range (inclusive), epoch-milliseconds
   * @param to end of range (inclusive), epoch-milliseconds
   * @return ordered list of daily bucket strings; always contains at least one element
   */
  public static List<String> dayBuckets(long from, long to) {
    Instant start = Instant.ofEpochMilli(from).truncatedTo(ChronoUnit.DAYS);
    Instant end = Instant.ofEpochMilli(to).truncatedTo(ChronoUnit.DAYS);

    List<String> buckets = new ArrayList<>();
    Instant current = start;
    while (!current.isAfter(end)) {
      buckets.add(DAY_FMT.format(current));
      current = current.plus(1, ChronoUnit.DAYS);
    }
    return buckets;
  }

  /**
   * Formats an epoch-millis value as an hourly bucket string.
   *
   * @param epochMillis the timestamp in milliseconds since epoch
   * @return hourly bucket string (e.g. {@code "2024-06-03-14"})
   */
  public static String hourBucket(long epochMillis) {
    return HOUR_FMT.format(Instant.ofEpochMilli(epochMillis));
  }

  /**
   * Formats an epoch-millis value as a daily bucket string.
   *
   * @param epochMillis the timestamp in milliseconds since epoch
   * @return daily bucket string (e.g. {@code "2024-06-03"})
   */
  public static String dayBucket(long epochMillis) {
    return DAY_FMT.format(Instant.ofEpochMilli(epochMillis));
  }
}
