package com.streamflow.api.exception;

/**
 * Thrown when a requested stream ID has no snapshot in Redis.
 *
 * <p>SPEC-06 R4: Mapped to HTTP 404 by {@link GlobalExceptionHandler}.
 */
public class StreamNotFoundException extends RuntimeException {

  public StreamNotFoundException(String streamId) {
    super("No active stream found for id: " + streamId);
  }
}
