package com.streamflow.api.chaos;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

/**
 * HTTP client that forwards chaos commands to the producer's internal REST API.
 *
 * <p>SPEC-13 R5: uses {@link RestTemplate} (not Feign) with a configurable base URL ({@code
 * streamflow.producer.base-url}) so it can be pointed at any producer instance (local or Docker).
 *
 * <p>Throws {@link IllegalStateException} if the producer is unreachable; callers should map this
 * to an appropriate HTTP 502/503 response.
 */
@Slf4j
@Component
public class ProducerChaosClient {

  private final RestTemplate restTemplate;
  private final String producerBaseUrl;

  public ProducerChaosClient(
      RestTemplate chaosRestTemplate,
      @Value("${streamflow.producer.base-url:http://localhost:8081}") String producerBaseUrl) {
    this.restTemplate = chaosRestTemplate;
    this.producerBaseUrl = producerBaseUrl;
  }

  /**
   * Forwards a chaos start request to the producer.
   *
   * @param streamId the target stream
   * @param scenario chaos scenario
   * @param durationSeconds chaos duration
   * @return the chaos response containing the chaosId and startsAt timestamp
   * @throws IllegalStateException if the producer returns an unexpected error
   */
  public ChaosResponse startChaos(String streamId, ChaosScenarioDTO scenario, int durationSeconds) {
    String url = producerBaseUrl + "/internal/chaos";

    // Build the request body that the producer's InternalChaosController expects
    Map<String, Object> body =
        Map.of(
            "streamId", streamId,
            "scenario", scenario.name(),
            "durationSeconds", durationSeconds);

    log.info(
        "Forwarding chaos start to producer: url={} streamId={} scenario={} duration={}s",
        url,
        streamId,
        scenario,
        durationSeconds);

    try {
      ResponseEntity<ChaosResponse> response =
          restTemplate.postForEntity(url, body, ChaosResponse.class);
      ChaosResponse result = response.getBody();
      log.info("Producer returned chaosId={}", result != null ? result.chaosId() : "null");
      return result;
    } catch (HttpClientErrorException e) {
      log.error(
          "Producer returned error for chaos start: status={} body={}",
          e.getStatusCode(),
          e.getResponseBodyAsString());
      throw new IllegalStateException(
          "Producer rejected chaos start: " + e.getResponseBodyAsString(), e);
    }
  }

  /**
   * Forwards a chaos cancel request to the producer.
   *
   * @param chaosId the session identifier to cancel
   * @return {@code true} if the producer cancelled it; {@code false} if not found (404)
   */
  public boolean cancelChaos(String chaosId) {
    String url = producerBaseUrl + "/internal/chaos/" + chaosId;

    log.info("Forwarding chaos cancel to producer: url={}", url);

    try {
      restTemplate.exchange(url, HttpMethod.DELETE, HttpEntity.EMPTY, Void.class);
      return true;
    } catch (HttpClientErrorException.NotFound e) {
      log.warn("Producer: chaos session not found for chaosId={}", chaosId);
      return false;
    } catch (HttpClientErrorException e) {
      log.error("Producer returned error for chaos cancel: status={}", e.getStatusCode());
      throw new IllegalStateException("Producer rejected chaos cancel: " + e.getMessage(), e);
    }
  }
}
