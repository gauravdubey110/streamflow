package com.streamflow.producer.strategy;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Normal (steady-state) load strategy.
 *
 * <p>SPEC-03 R4 / SPEC-10 R1 event distribution (updated):
 * <ul>
 *   <li>65 % JOIN
 *   <li>25 % DROP
 *   <li>5  % QUALITY_SWITCH — triggers quality-distribution increment
 *   <li>5  % BUFFER_START  — triggers buffer-rate counter; carries {@code bufferDurationMs}
 * </ul>
 *
 * <p>Quality distribution is uniformly drawn from all five tiers for simplicity at this
 * stage; the quality-distribution aggregator in SPEC-10 will track the real distribution.
 *
 * <p>Region is drawn from a weighted set reflecting a realistic IN/US mix.
 *
 * <p>SPEC-10 R1: BUFFER_START events carry a random {@code bufferDurationMs} in the
 * range [500, 3000] ms to simulate realistic buffering durations.
 */
@Component
public class NormalLoadStrategy implements EventGenerationStrategy {

    private static final List<EventType> WEIGHTED_TYPES;
    private static final VideoQuality[] QUALITIES = VideoQuality.values();
    private static final String[] REGIONS = {"IN-MH", "IN-DL", "IN-KA", "US-CA", "US-NY"};

    /** Minimum simulated buffer duration in milliseconds (SPEC-10 R1). */
    static final long BUFFER_DURATION_MIN_MS = 500L;

    /** Maximum simulated buffer duration in milliseconds (SPEC-10 R1). */
    static final long BUFFER_DURATION_MAX_MS = 3_000L;

    static {
        // Build weighted list: 65 JOIN, 25 DROP, 5 QUALITY_SWITCH, 5 BUFFER_START per 100 slots
        // SPEC-10 R1: BUFFER_START at ~5% baseline (1% was too low for observable buffer rate)
        WEIGHTED_TYPES = new java.util.ArrayList<>(100);
        for (int i = 0; i < 65; i++) WEIGHTED_TYPES.add(EventType.JOIN);
        for (int i = 0; i < 25; i++) WEIGHTED_TYPES.add(EventType.DROP);
        for (int i = 0; i < 5; i++)  WEIGHTED_TYPES.add(EventType.QUALITY_SWITCH);
        for (int i = 0; i < 5; i++)  WEIGHTED_TYPES.add(EventType.BUFFER_START);
    }

    @Override
    public ViewerEventDTO generate(String streamId, String streamName) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        EventType eventType = WEIGHTED_TYPES.get(rng.nextInt(WEIGHTED_TYPES.size()));
        VideoQuality quality = QUALITIES[rng.nextInt(QUALITIES.length)];
        String region = REGIONS[rng.nextInt(REGIONS.length)];

        // SPEC-10 R1: BUFFER_START carries bufferDurationMs; other types do not
        Long bufferDurationMs = null;
        if (eventType == EventType.BUFFER_START) {
            bufferDurationMs = rng.nextLong(BUFFER_DURATION_MIN_MS, BUFFER_DURATION_MAX_MS + 1);
        }

        return new ViewerEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                UUID.randomUUID().toString(),
                eventType,
                quality,
                bufferDurationMs,
                System.currentTimeMillis(),
                region
        );
    }
}
