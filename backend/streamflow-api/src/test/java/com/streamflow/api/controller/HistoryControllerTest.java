package com.streamflow.api.controller;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HistoryController} helper methods.
 *
 * <p>SPEC-18 Test Plan: verifies ETag computation properties without requiring a Spring context or
 * Cassandra connection.
 */
class HistoryControllerTest {

  @Test
  void computeEtag_startsWithWeakPrefix() {
    String etag = HistoryController.computeEtag("stream-001", 1000L, 2000L, "MINUTE", 30);
    assertThat(etag).startsWith("W/\"").endsWith("\"");
  }

  @Test
  void computeEtag_sameInputsProduceSameEtag() {
    String e1 = HistoryController.computeEtag("stream-001", 1000L, 2000L, "MINUTE", 30);
    String e2 = HistoryController.computeEtag("stream-001", 1000L, 2000L, "MINUTE", 30);
    assertThat(e1).isEqualTo(e2);
  }

  @Test
  void computeEtag_differentResultCountProducesDifferentEtag() {
    String e1 = HistoryController.computeEtag("stream-001", 1000L, 2000L, "MINUTE", 30);
    String e2 = HistoryController.computeEtag("stream-001", 1000L, 2000L, "MINUTE", 31);
    assertThat(e1).isNotEqualTo(e2);
  }

  @Test
  void computeEtag_differentStreamProducesDifferentEtag() {
    String e1 = HistoryController.computeEtag("stream-001", 1000L, 2000L, "MINUTE", 30);
    String e2 = HistoryController.computeEtag("stream-002", 1000L, 2000L, "MINUTE", 30);
    assertThat(e1).isNotEqualTo(e2);
  }

  @Test
  void computeEtag_hasExpectedLength() {
    // W/"<16 hex chars>" = 3 + 16 + 1 = 20 chars
    String etag = HistoryController.computeEtag("stream-001", 1000L, 2000L, "MINUTE", 30);
    // W/"0123456789abcdef" = 20 chars
    assertThat(etag).hasSize(20);
  }
}
