package com.streamflow.processor.scheduler;

import com.streamflow.processor.aggregator.ViewerCountAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled task that evicts stale viewer entries from Redis sorted sets.
 *
 * <p>SPEC-04 R3: runs every 10 seconds and removes members whose score (join timestamp) is older
 * than 5 minutes from every known stream's {@code viewer_count:{streamId}} key.
 *
 * <p>This handles the case where a viewer disconnects without sending a {@code DROP} event —
 * without eviction the sorted set would grow unboundedly.
 *
 * <p>Scheduling is enabled at the application level via {@link
 * org.springframework.scheduling.annotation.EnableScheduling} on {@link
 * com.streamflow.processor.StreamProcessorApplication}.
 */
@Slf4j
@Component
public class StaleViewerEvictionTask {

  private final ViewerCountAggregator viewerCountAggregator;

  public StaleViewerEvictionTask(ViewerCountAggregator viewerCountAggregator) {
    this.viewerCountAggregator = viewerCountAggregator;
  }

  /**
   * Evict sorted-set members older than 5 minutes for all known streams.
   *
   * <p>Runs every 10 000 ms (SPEC-04 R3). Fixed-delay ensures the next run starts 10 s after the
   * previous one finishes, preventing overlap when there are many streams.
   */
  @Scheduled(fixedDelay = 10_000)
  public void evictStaleViewers() {
    var streamIds = viewerCountAggregator.getKnownStreamIds();
    if (streamIds.isEmpty()) {
      return;
    }
    log.debug("Running stale-viewer eviction for {} stream(s)", streamIds.size());
    streamIds.forEach(viewerCountAggregator::evictStaleEntries);
  }
}
