package com.streamflow.pulse.client.exceptions;

import java.util.Map;

/** 400 — the request body is malformed. */
public class PulseValidationException extends PulseApiException {
    public PulseValidationException(int statusCode, String path, Map<String, Object> body) {
        super(statusCode, path, body);
    }
}
