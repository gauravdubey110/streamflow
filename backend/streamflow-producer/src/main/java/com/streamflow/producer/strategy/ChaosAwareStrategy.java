package com.streamflow.producer.strategy;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import com.streamflow.producer.chaos.ChaosInjector;
import com.streamflow.producer.chaos.ChaosScenario;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Decorator around {@link NormalLoadStrategy} that applies chaos modifiers when a chaos
 * session is active for the target stream.
 *
 * <p>SPEC-13 R2 / Design Notes: this is the {@link Primary} {@link EventGenerationStrategy}
 * bean so Spring injects it into {@link com.streamflow.producer.simulator.StreamSimulator}
 * and {@link com.streamflow.producer.simulator.ViewerEventProducer}.
 *
 * <h3>Scenario behaviour</h3>
 * <ul>
 *   <li>{@link ChaosScenario#VIEWER_DROP} — doubles the probability of a DROP event (from
 *       ~25 % to ~50 %). The generated event type is re-rolled with the chaos distribution.</li>
 *   <li>{@link ChaosScenario#HIGH_BUFFER} — emits {@code BUFFER_START} at 12 % probability
 *       (vs. ~5 % baseline) with a 500–3 000 ms buffer duration.</li>
 *   <li>{@link ChaosScenario#BITRATE_SPIKE} — delegates to the normal strategy; the health
 *       event modulation is handled by
 *       {@link com.streamflow.producer.simulator.StreamHealthProducer}.</li>
 *   <li>{@link ChaosScenario#STREAM_DOWN} — returns {@code null}; the caller
 *       ({@link com.streamflow.producer.simulator.ViewerEventProducer}) must skip
 *       publishing when {@code null} is returned.</li>
 * </ul>
 */
@Slf4j
@Primary
@Component
@RequiredArgsConstructor
public class ChaosAwareStrategy implements EventGenerationStrategy {

    /** Buffer duration min for chaos HIGH_BUFFER scenario (ms). */
    static final long CHAOS_BUFFER_MIN_MS = 500L;

    /** Buffer duration max for chaos HIGH_BUFFER scenario (ms). */
    static final long CHAOS_BUFFER_MAX_MS = 3_000L;

    private final NormalLoadStrategy delegate;
    private final ChaosInjector chaosInjector;

    /**
     * Generates a viewer event, applying any active chaos modifier for the stream.
     *
     * @param streamId   the Kafka message key
     * @param streamName human-readable stream name (for log context)
     * @return a {@link ViewerEventDTO}, or {@code null} when {@link ChaosScenario#STREAM_DOWN}
     *         is active (callers must skip publishing nulls)
     */
    @Override
    public ViewerEventDTO generate(String streamId, String streamName) {
        Optional<ChaosScenario> active = chaosInjector.activeScenario(streamId);
        if (active.isEmpty()) {
            return delegate.generate(streamId, streamName);
        }

        ChaosScenario scenario = active.get();
        return switch (scenario) {
            case VIEWER_DROP -> generateWithViewerDrop(streamId, streamName);
            case HIGH_BUFFER -> generateWithHighBuffer(streamId, streamName);
            case BITRATE_SPIKE -> delegate.generate(streamId, streamName); // health events handle this
            case STREAM_DOWN -> null; // caller skips publish
        };
    }

    // ── scenario implementations ─────────────────────────────────────────────

    /**
     * Doubles the DROP event ratio: ~50 % DROP, ~38 % JOIN, ~6 % QUALITY_SWITCH, ~6 % BUFFER_START.
     */
    private ViewerEventDTO generateWithViewerDrop(String streamId, String streamName) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int roll = rng.nextInt(100);
        // 0-49 → DROP (50%), 50-87 → JOIN (38%), 88-93 → QUALITY_SWITCH (6%), 94-99 → BUFFER_START (6%)
        EventType eventType;
        if (roll < 50) {
            eventType = EventType.DROP;
        } else if (roll < 88) {
            eventType = EventType.JOIN;
        } else if (roll < 94) {
            eventType = EventType.QUALITY_SWITCH;
        } else {
            eventType = EventType.BUFFER_START;
        }

        Long bufferDurationMs = null;
        if (eventType == EventType.BUFFER_START) {
            bufferDurationMs = rng.nextLong(CHAOS_BUFFER_MIN_MS, CHAOS_BUFFER_MAX_MS + 1);
        }

        VideoQuality quality = VideoQuality.values()[rng.nextInt(VideoQuality.values().length)];
        String region = randomRegion(rng);

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

    /**
     * Emits BUFFER_START at 12 % rate; other event types distributed normally from the remaining 88 %.
     */
    private ViewerEventDTO generateWithHighBuffer(String streamId, String streamName) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int roll = rng.nextInt(100);

        // 0-11 → BUFFER_START (12%), 12-87 → JOIN (~76% of remainder ≈ 66.9%), 88-95 → DROP, 96-99 → QUALITY_SWITCH
        EventType eventType;
        Long bufferDurationMs = null;

        if (roll < 12) {
            eventType = EventType.BUFFER_START;
            bufferDurationMs = rng.nextLong(CHAOS_BUFFER_MIN_MS, CHAOS_BUFFER_MAX_MS + 1);
        } else if (roll < 76) {
            eventType = EventType.JOIN;
        } else if (roll < 95) {
            eventType = EventType.DROP;
        } else {
            eventType = EventType.QUALITY_SWITCH;
        }

        VideoQuality quality = VideoQuality.values()[rng.nextInt(VideoQuality.values().length)];
        String region = randomRegion(rng);

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

    private static String randomRegion(ThreadLocalRandom rng) {
        String[] regions = {"IN-MH", "IN-DL", "IN-KA", "US-CA", "US-NY"};
        return regions[rng.nextInt(regions.length)];
    }
}
