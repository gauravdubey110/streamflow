package com.streamflow.producer.chaos;

import java.util.concurrent.ScheduledFuture;

/**
 * Holds the runtime state of a single active chaos injection.
 *
 * <p>SPEC-13 Design Notes: tracked in {@code ConcurrentHashMap<chaosId, ChaosState>} inside
 * {@link ChaosInjector}; the {@link #autoRevertFuture} is used to cancel the scheduler on
 * early cancellation via {@link ChaosInjector#cancel(String)}.
 *
 * @param chaosId         Unique identifier for this chaos session (UUID).
 * @param streamId        The stream to which chaos is applied.
 * @param scenario        The type of degradation being injected.
 * @param expiresAtMs     Wall-clock epoch-millis when the chaos auto-reverts.
 * @param autoRevertFuture Scheduled task that calls {@code ChaosInjector.cancel} after duration.
 */
public record ChaosState(
        String chaosId,
        String streamId,
        ChaosScenario scenario,
        long expiresAtMs,
        ScheduledFuture<?> autoRevertFuture
) {}
