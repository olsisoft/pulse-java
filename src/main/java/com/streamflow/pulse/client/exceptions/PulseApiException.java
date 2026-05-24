package com.streamflow.pulse.client.exceptions;

import java.util.Map;

/**
 * A non-2xx response from the Pulse server.
 *
 * <p>Carries the HTTP status code, the parsed error body (if JSON), and the
 * request path so log lines + bug reports are actionable.
 */
public class PulseApiException extends PulseClientException {

    private final int statusCode;
    private final String path;
    private final Map<String, Object> body;

    public PulseApiException(int statusCode, String path, Map<String, Object> body) {
        super(formatMessage(statusCode, path, body));
        this.statusCode = statusCode;
        this.path = path;
        this.body = body;
    }

    public int statusCode() {
        return statusCode;
    }

    public String path() {
        return path;
    }

    /** The parsed error body, or {@code null} if the response carried no JSON body. */
    public Map<String, Object> body() {
        return body;
    }

    private static String formatMessage(int statusCode, String path, Map<String, Object> body) {
        StringBuilder sb = new StringBuilder("HTTP ").append(statusCode).append(" from ").append(path);
        if (body != null) {
            Object err = body.get("error");
            if (err == null) err = body.get("errorMessage");
            if (err == null) err = body.get("message");
            if (err != null) {
                sb.append(" — ").append(err);
            }
        }
        return sb.toString();
    }
}
