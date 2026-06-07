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
 * <p>SPEC-03 R4 event distribution:
 * <ul>
 *   <li>70 % JOIN
 *   <li>25 % DROP
 *   <li>5  % QUALITY_SWITCH (placeholder; BUFFER_START/END/ERROR added in later specs)
 * </ul>
 *
 * <p>Quality distribution is uniformly drawn from all five tiers for simplicity at this
 * stage; the quality-distribution aggregator in SPEC-10 will track the real distribution.
 *
 * <p>Region is drawn from a weighted set reflecting a realistic IN/US mix.
 */
@Component
public class NormalLoadStrategy implements EventGenerationStrategy {

    private static final List<EventType> WEIGHTED_TYPES;
    private static final VideoQuality[] QUALITIES = VideoQuality.values();
    private static final String[] REGIONS = {"IN-MH", "IN-DL", "IN-KA", "US-CA", "US-NY"};

    static {
        // Build weighted list: 70 JOIN, 25 DROP, 5 QUALITY_SWITCH per 100 slots
        WEIGHTED_TYPES = new java.util.ArrayList<>(100);
        for (int i = 0; i < 70; i++) WEIGHTED_TYPES.add(EventType.JOIN);
        for (int i = 0; i < 25; i++) WEIGHTED_TYPES.add(EventType.DROP);
        for (int i = 0; i < 5; i++)  WEIGHTED_TYPES.add(EventType.QUALITY_SWITCH);
    }

    @Override
    public ViewerEventDTO generate(String streamId, String streamName) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        EventType eventType = WEIGHTED_TYPES.get(rng.nextInt(WEIGHTED_TYPES.size()));
        VideoQuality quality = QUALITIES[rng.nextInt(QUALITIES.length)];
        String region = REGIONS[rng.nextInt(REGIONS.length)];

        return new ViewerEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                UUID.randomUUID().toString(),
                eventType,
                quality,
                null,          // bufferDurationMs — not applicable for JOIN/DROP/QUALITY_SWITCH
                System.currentTimeMillis(),
                region
        );
    }
}
