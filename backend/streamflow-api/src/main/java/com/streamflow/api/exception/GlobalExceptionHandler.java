package com.streamflow.api.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

import java.net.URI;
import java.time.Instant;

/**
 * Global exception handler that maps domain exceptions to RFC-7807 problem JSON.
 *
 * <p>SPEC-06 task §5: All REST error responses use {@link ProblemDetail}
 * (Spring 6 / Spring Boot 3 native support) so the wire format is standard
 * application/problem+json.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final URI STREAM_NOT_FOUND_TYPE =
            URI.create("https://streamflow.example/problems/stream-not-found");

    /**
     * Maps {@link StreamNotFoundException} to HTTP 404 with RFC-7807 body.
     *
     * <p>Response body:
     * <pre>{@code
     * {
     *   "type":     "https://streamflow.example/problems/stream-not-found",
     *   "title":    "Stream Not Found",
     *   "status":   404,
     *   "detail":   "No active stream found for id: stream-001",
     *   "instance": "/api/v1/streams/stream-001"
     * }
     * }</pre>
     */
    @ExceptionHandler(StreamNotFoundException.class)
    public ProblemDetail handleStreamNotFound(StreamNotFoundException ex, WebRequest request) {
        log.warn("StreamNotFound: {}", ex.getMessage());

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setType(STREAM_NOT_FOUND_TYPE);
        problem.setTitle("Stream Not Found");
        problem.setProperty("timestamp", Instant.now().toEpochMilli());
        return problem;
    }
}
