package com.streamflow.processor.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the bucket-derivation helper methods in the Cassandra repositories.
 *
 * <p>SPEC-17: verifies the static helper methods that format the date/hour bucket strings used as
 * partition keys in Cassandra tables.
 *
 * <p>No infrastructure required — pure logic tests.
 */
class BucketHelperTest {

  // ── CassandraViewerEventRepository.hourBucket ─────────────────────────────

  @Test
  void hourBucket_formats_yyyyMMddHH() {
    // 2024-06-03 14:30:45 UTC → "2024-06-03-14"
    Instant ts = Instant.parse("2024-06-03T14:30:45Z");
    assertThat(CassandraViewerEventRepository.hourBucket(ts.toEpochMilli()))
        .isEqualTo("2024-06-03-14");
  }

  @Test
  void hourBucket_midnightUTC_isHour00() {
    // 2024-01-01 00:00:00 UTC → "2024-01-01-00"
    Instant ts = Instant.parse("2024-01-01T00:00:00Z");
    assertThat(CassandraViewerEventRepository.hourBucket(ts.toEpochMilli()))
        .isEqualTo("2024-01-01-00");
  }

  @Test
  void hourBucket_23h59UTC_isHour23() {
    // 2024-12-31 23:59:59 UTC → "2024-12-31-23"
    Instant ts = Instant.parse("2024-12-31T23:59:59Z");
    assertThat(CassandraViewerEventRepository.hourBucket(ts.toEpochMilli()))
        .isEqualTo("2024-12-31-23");
  }

  // ── CassandraAlertRepository.dayBucket ────────────────────────────────────

  @Test
  void dayBucket_formats_yyyyMMdd() {
    // 2024-06-03 22:45:00 UTC → "2024-06-03"
    Instant ts = Instant.parse("2024-06-03T22:45:00Z");
    assertThat(CassandraAlertRepository.dayBucket(ts.toEpochMilli())).isEqualTo("2024-06-03");
  }

  @Test
  void dayBucket_endOfDayUTC_isSameDay() {
    // 2024-06-03 23:59:59 UTC → "2024-06-03" (still same day)
    Instant ts = Instant.parse("2024-06-03T23:59:59Z");
    assertThat(CassandraAlertRepository.dayBucket(ts.toEpochMilli())).isEqualTo("2024-06-03");
  }

  @Test
  void dayBucket_nextDayMidnightUTC_isNextDay() {
    // 2024-06-04 00:00:00 UTC → "2024-06-04"
    Instant ts = Instant.parse("2024-06-04T00:00:00Z");
    assertThat(CassandraAlertRepository.dayBucket(ts.toEpochMilli())).isEqualTo("2024-06-04");
  }
}
