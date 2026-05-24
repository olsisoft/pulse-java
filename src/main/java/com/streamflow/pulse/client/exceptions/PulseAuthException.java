package com.streamflow.pulse.client.exceptions;

import java.util.Map;

/** 401 — invalid / missing / expired JWT. */
public class PulseAuthException extends PulseApiException {
    public PulseAuthException(int statusCode, String path, Map<String, Object> body) {
        super(statusCode, path, body);
    }
}
