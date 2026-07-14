package com.streamflow.api.exception;

import java.net.URI;
import java.time.Instant;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

/**
 * Global exception handler that maps domain exceptions to RFC-7807 problem JSON.
 *
 * <p>SPEC-06 task §5: All REST error responses use {@link ProblemDetail} (Spring 6 / Spring Boot 3
 * native support) so the wire format is standard application/problem+json.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

  private static final URI STREAM_NOT_FOUND_TYPE =
      URI.create("https://streamflow.example/problems/stream-not-found");

  /**
   * Maps Bean Validation failures ({@link MethodArgumentNotValidException}) to HTTP 400.
   *
   * <p>SPEC-13 R6: returns a ProblemDetail listing each field violation.
   */
  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
    String detail =
        ex.getBindingResult().getFieldErrors().stream()
            .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
            .collect(Collectors.joining("; "));

    ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
    problem.setTitle("Validation Failed");
    problem.setProperty("timestamp", Instant.now().toEpochMilli());
    return problem;
  }

  /**
   * Maps unreadable JSON bodies (e.g. unknown enum values) to HTTP 400.
   *
   * <p>SPEC-13 R6: catches invalid {@code scenario} enum values on deserialisation.
   */
  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ProblemDetail handleUnreadableMessage(HttpMessageNotReadableException ex) {
    ProblemDetail problem =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.BAD_REQUEST, "Invalid request body: " + ex.getMessage());
    problem.setTitle("Bad Request");
    problem.setProperty("timestamp", Instant.now().toEpochMilli());
    return problem;
  }

  /**
   * Maps {@link StreamNotFoundException} to HTTP 404 with RFC-7807 body.
   *
   * <p>Response body:
   *
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

  /**
   * Maps {@link HistoryRangeException} to HTTP 400 with RFC-7807 body.
   *
   * <p>SPEC-18 R3: fired when the requested {@code [from, to]} window exceeds the configured
   * maximum (default 24 hours). The response body includes the maximum allowed range and the actual
   * requested span for debugging.
   */
  @ExceptionHandler(HistoryRangeException.class)
  public ProblemDetail handleHistoryRange(HistoryRangeException ex) {
    log.warn("HistoryRangeExceeded: {}", ex.getMessage());

    ProblemDetail problem =
        ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    problem.setType(URI.create("https://streamflow.example/problems/history-range-exceeded"));
    problem.setTitle("History Range Exceeded");
    problem.setProperty("maxRangeHours", ex.getMaxHours());
    problem.setProperty("fromMs", ex.getFromMs());
    problem.setProperty("toMs", ex.getToMs());
    problem.setProperty("timestamp", Instant.now().toEpochMilli());
    return problem;
  }
}
