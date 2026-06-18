package com.streamflow.producer.chaos;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages active chaos sessions for all simulated streams.
 *
 * <p>SPEC-13 R1/Design Notes:
 * <ul>
 *   <li>Active chaos is tracked per {@code streamId} in a {@link ConcurrentHashMap}.</li>
 *   <li>{@link #start(ChaosScenario, String, int)} schedules a {@link ScheduledFuture} to auto-revert
 *       after {@code durationSeconds}.</li>
 *   <li>{@link #cancel(String)} cancels the future and removes the state immediately.</li>
 *   <li>Starting a new chaos session on a stream that already has active chaos cancels the
 *       previous session first.</li>
 * </ul>
 *
 * <p>Thread-safety: {@link ConcurrentHashMap} guards concurrent reads (strategy lookup) and writes
 * (start/cancel). The {@link ScheduledExecutorService} is daemon-threaded so it does not block JVM
 * shutdown.
 */
@Slf4j
@Component
public class ChaosInjector {

    /**
     * Maps {@code streamId} to the currently active {@link ChaosState} for that stream.
     * A stream with no entry has no active chaos.
     */
    private final ConcurrentHashMap<String, ChaosState> activeByStream = new ConcurrentHashMap<>();

    /**
     * Maps {@code chaosId} to the currently active {@link ChaosState}.
     * Enables O(1) lookup by chaosId in {@link #cancel(String)}.
     */
    private final ConcurrentHashMap<String, ChaosState> activeById = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "chaos-revert-scheduler");
        t.setDaemon(true);
        return t;
    });

    /**
     * Starts a chaos session for the given stream.
     *
     * <p>SPEC-13 R1: returns the generated {@code chaosId}. If the stream already has
     * active chaos, the previous session is cancelled first.
     *
     * @param scenario        degradation type to apply
     * @param streamId        target stream
     * @param durationSeconds how long chaos lasts (1–300); after this time the session auto-reverts
     * @return the unique chaos identifier
     */
    public String start(ChaosScenario scenario, String streamId, int durationSeconds) {
        // Cancel any pre-existing chaos on this stream
        ChaosState existing = activeByStream.get(streamId);
        if (existing != null) {
            log.info("Replacing existing chaos={} on stream={}", existing.chaosId(), streamId);
            cancelInternal(existing);
        }

        String chaosId = UUID.randomUUID().toString();
        long expiresAtMs = System.currentTimeMillis() + (long) durationSeconds * 1_000;

        // Schedule auto-revert
        ScheduledFuture<?> future = scheduler.schedule(
                () -> {
                    log.info("Auto-reverting chaos={} on stream={} after {}s",
                            chaosId, streamId, durationSeconds);
                    removeById(chaosId);
                },
                durationSeconds,
                TimeUnit.SECONDS
        );

        ChaosState state = new ChaosState(chaosId, streamId, scenario, expiresAtMs, future);
        activeByStream.put(streamId, state);
        activeById.put(chaosId, state);

        log.info("Chaos started: id={} stream={} scenario={} durationSeconds={}",
                chaosId, streamId, scenario, durationSeconds);
        return chaosId;
    }

    /**
     * Cancels an active chaos session by its ID.
     *
     * @param chaosId the identifier returned by {@link #start}
     * @return {@code true} if a session was found and cancelled; {@code false} if not found
     */
    public boolean cancel(String chaosId) {
        ChaosState state = activeById.get(chaosId);
        if (state == null) {
            log.warn("cancel: no active chaos found with id={}", chaosId);
            return false;
        }
        cancelInternal(state);
        log.info("Chaos cancelled: id={} stream={}", chaosId, state.streamId());
        return true;
    }

    /**
     * Returns the active {@link ChaosScenario} for the given stream, or empty if none.
     *
     * <p>Called on every event generation cycle by {@link com.streamflow.producer.strategy.ChaosAwareStrategy}.
     *
     * @param streamId the stream to query
     * @return the active scenario, or {@link Optional#empty()}
     */
    public Optional<ChaosScenario> activeScenario(String streamId) {
        ChaosState state = activeByStream.get(streamId);
        return Optional.ofNullable(state).map(ChaosState::scenario);
    }

    /**
     * Returns {@code true} if there is an active chaos session for the given stream.
     *
     * @param streamId stream identifier
     */
    public boolean isActive(String streamId) {
        return activeByStream.containsKey(streamId);
    }

    // ── private helpers ──────────────────────────────────────────────────────

    private void cancelInternal(ChaosState state) {
        state.autoRevertFuture().cancel(false);
        removeById(state.chaosId());
    }

    private void removeById(String chaosId) {
        ChaosState state = activeById.remove(chaosId);
        if (state != null) {
            activeByStream.remove(state.streamId(), state);
        }
    }
}
