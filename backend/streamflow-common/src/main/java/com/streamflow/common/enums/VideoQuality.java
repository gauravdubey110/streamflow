package com.streamflow.common.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Video quality tiers used in viewer events and quality-distribution aggregations.
 *
 * <p>The wire representation is the human-readable resolution string (e.g. {@code "1080p"}).
 * Jackson uses {@link #getWireValue()} for serialization and {@link #fromWire(String)} for
 * deserialization so that JSON payloads are legible without needing to know enum ordinals.
 */
public enum VideoQuality {
    Q_1080P("1080p"),
    Q_720P("720p"),
    Q_480P("480p"),
    Q_360P("360p"),
    Q_144P("144p");

    private final String wireValue;

    VideoQuality(String wireValue) {
        this.wireValue = wireValue;
    }

    /**
     * Returns the JSON wire string (e.g. {@code "720p"}).
     * Jackson calls this during serialization because of {@code @JsonValue}.
     */
    @JsonValue
    public String getWireValue() {
        return wireValue;
    }

    /**
     * Factory used by Jackson during deserialization and by application code that
     * receives raw quality strings from Kafka messages.
     *
     * @param value the wire string, e.g. {@code "720p"}
     * @return the matching enum constant
     * @throws IllegalArgumentException if the string does not correspond to a known quality
     */
    @JsonCreator
    public static VideoQuality fromWire(String value) {
        for (VideoQuality q : values()) {
            if (q.wireValue.equals(value)) {
                return q;
            }
        }
        throw new IllegalArgumentException("Unknown VideoQuality wire value: " + value);
    }
}
