package com.streamflow.processor.circuitbreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.CbStateEventDTO;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Unit test for {@link CircuitBreakerStatePublisher} (SPEC-14 R2).
 *
 * <p>Verifies that a {@link CircuitBreakerStateEvent} is correctly converted to a {@link
 * CbStateEventDTO} and published to the {@code cb-events} Kafka topic.
 */
@ExtendWith(MockitoExtension.class)
class CircuitBreakerStatePublisherTest {

  @Mock private KafkaTemplate<String, CbStateEventDTO> cbKafkaTemplate;

  @InjectMocks private CircuitBreakerStatePublisher publisher;

  @Test
  @SuppressWarnings("unchecked")
  void onApplicationEvent_publishesToCbEventsTopic() {
    // Arrange: suppress the completable future warning in whenComplete
    when(cbKafkaTemplate.send(any(String.class), any(String.class), any(CbStateEventDTO.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    long ts = System.currentTimeMillis();
    CircuitBreakerStateEvent event =
        new CircuitBreakerStateEvent(this, "CLOSED", "OPEN", "failure rate exceeded", ts);

    // Act
    publisher.onApplicationEvent(event);

    // Assert: correct topic, key, and payload
    ArgumentCaptor<CbStateEventDTO> dtoCaptor = ArgumentCaptor.forClass(CbStateEventDTO.class);
    verify(cbKafkaTemplate)
        .send(
            eq(KafkaTopics.CB_EVENTS),
            eq(CircuitBreakerStatePublisher.GLOBAL_STREAM_ID),
            dtoCaptor.capture());

    CbStateEventDTO dto = dtoCaptor.getValue();
    assertThat(dto.streamId()).isEqualTo(CircuitBreakerStatePublisher.GLOBAL_STREAM_ID);
    assertThat(dto.previousState()).isEqualTo("CLOSED");
    assertThat(dto.currentState()).isEqualTo("OPEN");
    assertThat(dto.reason()).isEqualTo("failure rate exceeded");
    assertThat(dto.ts()).isEqualTo(ts);
  }

  @Test
  @SuppressWarnings("unchecked")
  void onApplicationEvent_halfOpenTransition_publishesCorrectly() {
    when(cbKafkaTemplate.send(any(String.class), any(String.class), any(CbStateEventDTO.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    CircuitBreakerStateEvent event =
        new CircuitBreakerStateEvent(
            this, "OPEN", "HALF_OPEN", "wait elapsed", System.currentTimeMillis());

    publisher.onApplicationEvent(event);

    ArgumentCaptor<CbStateEventDTO> captor = ArgumentCaptor.forClass(CbStateEventDTO.class);
    verify(cbKafkaTemplate).send(eq(KafkaTopics.CB_EVENTS), any(), captor.capture());

    assertThat(captor.getValue().currentState()).isEqualTo("HALF_OPEN");
    assertThat(captor.getValue().previousState()).isEqualTo("OPEN");
  }

  @Test
  void globalStreamIdConstant_isAll() {
    assertThat(CircuitBreakerStatePublisher.GLOBAL_STREAM_ID).isEqualTo("all");
  }
}
