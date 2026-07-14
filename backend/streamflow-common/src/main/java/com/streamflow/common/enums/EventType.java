package com.streamflow.common.enums;

/**
 * Viewer interaction event types published to the {@code viewer-events} Kafka topic. Enum names are
 * the JSON wire values (no custom @JsonValue needed).
 */
public enum EventType {
  JOIN,
  DROP,
  QUALITY_SWITCH,
  BUFFER_START,
  BUFFER_END,
  ERROR
}
