package com.streamflow.api.websocket;

import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;

/**
 * STOMP WebSocket push payload for alert notifications (SPEC-14 R4).
 *
 * <p>Broadcast to {@code /topic/streams/{streamId}/alerts} whenever a new
 * {@link com.streamflow.common.dto.AlertEventDTO} is consumed from the
 * {@code alerts} Kafka topic.
 *
 * <p>Wire format matches the Project Plan §10 {@code ALERT_FIRED} schema:
 * <pre>{@code
 * {
 *   "type":      "ALERT_FIRED",
 *   "alertId":   "uuid",
 *   "streamId":  "stream-001",
 *   "severity":  "CRITICAL",
 *   "alertType": "HIGH_BUFFER_RATE",
 *   "message":   "Buffer rate 8.3% exceeds threshold 5.0%",
 *   "ts":        1717350000000
 * }
 * }</pre>
 */
public record AlertWsMessage(
        /** Always {@code "ALERT_FIRED"} — discriminator field (SPEC-14 R4). */
        String type,

        /** UUID of the alert event. */
        String alertId,

        /** The stream that triggered the alert. */
        String streamId,

        /** Alert severity level. */
        AlertSeverity severity,

        /** The alert rule that fired. */
        AlertType alertType,

        /** Human-readable alert description. */
        String message,

        /** Epoch-ms when the alert was evaluated. */
        long ts
) {
    /** Discriminator value used in every {@link AlertWsMessage}. */
    public static final String TYPE = "ALERT_FIRED";
}
