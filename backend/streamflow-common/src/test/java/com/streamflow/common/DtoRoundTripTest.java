package com.streamflow.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.CbStateEventDTO;
import com.streamflow.common.dto.StreamHealthEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** SPEC-02: AC1 + AC3 — round-trip serialization tests for all DTOs and VideoQuality enum. */
class DtoRoundTripTest {

  private static ObjectMapper mapper;

  @BeforeAll
  static void setup() {
    mapper = new ObjectMapper();
  }

  // ── ViewerEventDTO ────────────────────────────────────────────────────────

  @Test
  @DisplayName("ViewerEventDTO round-trips with all fields present")
  void viewerEventDto_roundTrip_withAllFields() throws Exception {
    var original =
        new ViewerEventDTO(
            "evt-uuid-1",
            "stream-001",
            "viewer-uuid-1",
            EventType.JOIN,
            VideoQuality.Q_1080P,
            1200L,
            1717350000000L,
            "IN-MH");

    byte[] json = mapper.writeValueAsBytes(original);
    var restored = mapper.readValue(json, ViewerEventDTO.class);

    assertEquals(original, restored);
  }

  @Test
  @DisplayName("ViewerEventDTO omits null optional fields from JSON")
  void viewerEventDto_roundTrip_withNullOptionalFields() throws Exception {
    var original =
        new ViewerEventDTO(
            "evt-uuid-2",
            "stream-001",
            "viewer-uuid-2",
            EventType.DROP,
            VideoQuality.Q_720P,
            null, // bufferDurationMs — optional, must be omitted
            1717350001000L,
            null // region — optional, must be omitted
            );

    String json = mapper.writeValueAsString(original);

    // Verify the JSON does not contain null-value keys
    assert !json.contains("bufferDurationMs") : "bufferDurationMs should be absent when null";
    assert !json.contains("region") : "region should be absent when null";

    var restored = mapper.readValue(json, ViewerEventDTO.class);
    assertEquals(original, restored);
    assertNull(restored.bufferDurationMs());
    assertNull(restored.region());
  }

  // ── StreamHealthEventDTO ─────────────────────────────────────────────────

  @Test
  @DisplayName("StreamHealthEventDTO round-trips correctly")
  void streamHealthEventDto_roundTrip() throws Exception {
    var original =
        new StreamHealthEventDTO(
            "health-uuid-1", "stream-001", 4500, 0.02, 120, "edge-mumbai-01", 1717350000000L);

    byte[] json = mapper.writeValueAsBytes(original);
    var restored = mapper.readValue(json, StreamHealthEventDTO.class);

    assertEquals(original, restored);
  }

  // ── AlertEventDTO ────────────────────────────────────────────────────────

  @Test
  @DisplayName("AlertEventDTO round-trips correctly")
  void alertEventDto_roundTrip() throws Exception {
    var original =
        new AlertEventDTO(
            "alert-uuid-1",
            "stream-001",
            AlertSeverity.CRITICAL,
            AlertType.HIGH_BUFFER_RATE,
            5.0,
            8.3,
            "Buffer rate 8.3% exceeds threshold 5.0% on stream-001",
            1717350000000L);

    byte[] json = mapper.writeValueAsBytes(original);
    var restored = mapper.readValue(json, AlertEventDTO.class);

    assertEquals(original, restored);
  }

  // ── StreamMetricSnapshotDTO ──────────────────────────────────────────────

  @Test
  @DisplayName("StreamMetricSnapshotDTO round-trips correctly")
  void streamMetricSnapshotDto_roundTrip() throws Exception {
    var qualityDist =
        Map.of(
            "1080p", 45.2,
            "720p", 24.1,
            "480p", 18.6,
            "360p", 8.3,
            "144p", 3.8);

    var original =
        new StreamMetricSnapshotDTO(
            "stream-001",
            "Tech Talk Live",
            847230L,
            1230L,
            1.8,
            42,
            qualityDist,
            99.2,
            "CLOSED",
            0,
            1717350000000L);

    byte[] json = mapper.writeValueAsBytes(original);
    var restored = mapper.readValue(json, StreamMetricSnapshotDTO.class);

    assertEquals(original, restored);
  }

  // ── VideoQuality enum wire-format (AC3) ──────────────────────────────────

  @Test
  @DisplayName("AC3: VideoQuality.fromWire(\"720p\") returns Q_720P")
  void videoQuality_fromWire_returns_correct_constant() {
    assertEquals(VideoQuality.Q_720P, VideoQuality.fromWire("720p"));
    assertEquals(VideoQuality.Q_1080P, VideoQuality.fromWire("1080p"));
    assertEquals(VideoQuality.Q_480P, VideoQuality.fromWire("480p"));
    assertEquals(VideoQuality.Q_360P, VideoQuality.fromWire("360p"));
    assertEquals(VideoQuality.Q_144P, VideoQuality.fromWire("144p"));
  }

  @Test
  @DisplayName("AC3: objectMapper.writeValueAsString(Q_720P) equals \"720p\"")
  void videoQuality_jackson_serializes_to_wire_string() throws Exception {
    assertEquals("\"720p\"", mapper.writeValueAsString(VideoQuality.Q_720P));
    assertEquals("\"1080p\"", mapper.writeValueAsString(VideoQuality.Q_1080P));
    assertEquals("\"480p\"", mapper.writeValueAsString(VideoQuality.Q_480P));
    assertEquals("\"360p\"", mapper.writeValueAsString(VideoQuality.Q_360P));
    assertEquals("\"144p\"", mapper.writeValueAsString(VideoQuality.Q_144P));
  }

  @Test
  @DisplayName("AC3: VideoQuality deserializes from JSON wire string correctly")
  void videoQuality_jackson_deserializes_from_wire_string() throws Exception {
    assertEquals(VideoQuality.Q_720P, mapper.readValue("\"720p\"", VideoQuality.class));
    assertEquals(VideoQuality.Q_1080P, mapper.readValue("\"1080p\"", VideoQuality.class));
  }

  @Test
  @DisplayName("EventType enum round-trips as its name()")
  void eventType_serializes_as_name() throws Exception {
    assertEquals("\"JOIN\"", mapper.writeValueAsString(EventType.JOIN));
    assertEquals(EventType.QUALITY_SWITCH, mapper.readValue("\"QUALITY_SWITCH\"", EventType.class));
  }

  @Test
  @DisplayName("AlertSeverity enum round-trips as its name()")
  void alertSeverity_serializes_as_name() throws Exception {
    assertEquals("\"CRITICAL\"", mapper.writeValueAsString(AlertSeverity.CRITICAL));
    assertEquals(AlertSeverity.WARNING, mapper.readValue("\"WARNING\"", AlertSeverity.class));
  }

  @Test
  @DisplayName("AlertType enum round-trips as its name()")
  void alertType_serializes_as_name() throws Exception {
    assertEquals("\"HIGH_BUFFER_RATE\"", mapper.writeValueAsString(AlertType.HIGH_BUFFER_RATE));
    assertEquals(AlertType.VIEWER_DROP, mapper.readValue("\"VIEWER_DROP\"", AlertType.class));
  }

  // ── CbStateEventDTO (SPEC-14) ─────────────────────────────────────────────

  @Test
  @DisplayName("SPEC-14: CbStateEventDTO round-trips correctly")
  void cbStateEventDto_roundTrip() throws Exception {
    var original =
        new CbStateEventDTO(
            "all", "CLOSED", "OPEN", "Failure rate 60% exceeded threshold 50%", 1717350000000L);

    byte[] json = mapper.writeValueAsBytes(original);
    var restored = mapper.readValue(json, CbStateEventDTO.class);

    assertEquals(original, restored);
  }
}
