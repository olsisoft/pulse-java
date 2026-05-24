package com.streamflow.pulse.client.exceptions;

/**
 * Base exception for every error raised by the Pulse client.
 *
 * <p>Catch this to handle all client-side errors uniformly:
 *
 * <pre>{@code
 * try {
 *     client.pipelines.get("p1");
 * } catch (PulseClientException e) {
 *     log.error("Pulse call failed: {}", e.getMessage(), e);
 * }
 * }</pre>
 *
 * <p>For more precise handling, catch one of the subclasses:
 * {@link PulseAuthException}, {@link PulseNotFoundException},
 * {@link PulseValidationException}, {@link PulseRateLimitException},
 * or {@link PulseApiException}.
 */
public class PulseClientException extends RuntimeException {

    public PulseClientException(String message) {
        super(message);
    }

    public PulseClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
