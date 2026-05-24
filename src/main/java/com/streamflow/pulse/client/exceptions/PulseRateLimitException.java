package com.streamflow.pulse.client.exceptions;

import java.util.Map;
import java.util.Optional;

/**
 * 429 — per-user or per-IP rate limit hit.
 *
 * <p>Carries the {@link #retryAfterSeconds()} value the server advises waiting
 * before retrying, parsed from either the JSON body ({@code retryAfterSeconds}
 * field) or the {@code Retry-After} HTTP header, whichever is present.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * try {
 *     client.pipelines.list();
 * } catch (PulseRateLimitException e) {
 *     Thread.sleep(e.retryAfterSeconds().orElse(60) * 1000L);
 *     // retry
 * }
 * }</pre>
 */
public class PulseRateLimitException extends PulseApiException {

    private final Integer retryAfterSeconds;

    public PulseRateLimitException(int statusCode, String path, Map<String, Object> body,
                                   Integer retryAfterSeconds) {
        super(statusCode, path, body);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public Optional<Integer> retryAfterSeconds() {
        return Optional.ofNullable(retryAfterSeconds);
    }
}
