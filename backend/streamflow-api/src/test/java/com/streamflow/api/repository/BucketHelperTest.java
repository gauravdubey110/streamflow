package com.streamflow.api.repository;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BucketHelper}.
 *
 * <p>SPEC-18 Test Plan: verifies the bucket enumeration logic that is central to
 * the time-range spanning queries.
 */
class BucketHelperTest {

    // ── hourBuckets ────────────────────────────────────────────────────────────

    @Test
    void hourBuckets_singleHour_returnsOneEntry() {
        // 14:00 to 14:30 — all within the same hour → ["2024-06-03-14"]
        long from = Instant.parse("2024-06-03T14:00:00Z").toEpochMilli();
        long to   = Instant.parse("2024-06-03T14:30:00Z").toEpochMilli();
        List<String> buckets = BucketHelper.hourBuckets(from, to);
        assertThat(buckets).containsExactly("2024-06-03-14");
    }

    @Test
    void hourBuckets_spansThreeHours_returnsThreeEntries() {
        // 13:45 to 15:10 UTC → hours 13, 14, 15
        long from = Instant.parse("2024-06-03T13:45:00Z").toEpochMilli();
        long to   = Instant.parse("2024-06-03T15:10:00Z").toEpochMilli();
        List<String> buckets = BucketHelper.hourBuckets(from, to);
        assertThat(buckets).containsExactly("2024-06-03-13", "2024-06-03-14", "2024-06-03-15");
    }

    @Test
    void hourBuckets_spansMidnight_includesBothDays() {
        // 23:30 to 00:15 next day → ["2024-06-03-23", "2024-06-04-00"]
        long from = Instant.parse("2024-06-03T23:30:00Z").toEpochMilli();
        long to   = Instant.parse("2024-06-04T00:15:00Z").toEpochMilli();
        List<String> buckets = BucketHelper.hourBuckets(from, to);
        assertThat(buckets).containsExactly("2024-06-03-23", "2024-06-04-00");
    }

    @Test
    void hourBuckets_fromEqualsTo_returnsSingleBucket() {
        long ts = Instant.parse("2024-06-03T14:30:00Z").toEpochMilli();
        List<String> buckets = BucketHelper.hourBuckets(ts, ts);
        assertThat(buckets).hasSize(1);
    }

    // ── dayBuckets ─────────────────────────────────────────────────────────────

    @Test
    void dayBuckets_singleDay_returnsOneEntry() {
        long from = Instant.parse("2024-06-03T08:00:00Z").toEpochMilli();
        long to   = Instant.parse("2024-06-03T22:00:00Z").toEpochMilli();
        List<String> buckets = BucketHelper.dayBuckets(from, to);
        assertThat(buckets).containsExactly("2024-06-03");
    }

    @Test
    void dayBuckets_spansThreeDays_returnsThreeEntries() {
        long from = Instant.parse("2024-06-01T22:00:00Z").toEpochMilli();
        long to   = Instant.parse("2024-06-03T02:00:00Z").toEpochMilli();
        List<String> buckets = BucketHelper.dayBuckets(from, to);
        assertThat(buckets).containsExactly("2024-06-01", "2024-06-02", "2024-06-03");
    }

    @Test
    void dayBuckets_crossesMidnight_returnsTwoDays() {
        long from = Instant.parse("2024-06-03T22:00:00Z").toEpochMilli();
        long to   = Instant.parse("2024-06-04T02:00:00Z").toEpochMilli();
        List<String> buckets = BucketHelper.dayBuckets(from, to);
        assertThat(buckets).containsExactly("2024-06-03", "2024-06-04");
    }

    // ── hourBucket / dayBucket single-value helpers ────────────────────────────

    @Test
    void hourBucket_formatsCorrectly() {
        long ts = Instant.parse("2024-06-03T14:30:45Z").toEpochMilli();
        assertThat(BucketHelper.hourBucket(ts)).isEqualTo("2024-06-03-14");
    }

    @Test
    void dayBucket_formatsCorrectly() {
        long ts = Instant.parse("2024-06-03T22:45:00Z").toEpochMilli();
        assertThat(BucketHelper.dayBucket(ts)).isEqualTo("2024-06-03");
    }
}
