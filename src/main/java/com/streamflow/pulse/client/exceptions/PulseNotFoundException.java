package com.streamflow.pulse.client.exceptions;

import java.util.Map;

/** 404 — the resource does not exist. */
public class PulseNotFoundException extends PulseApiException {
    public PulseNotFoundException(int statusCode, String path, Map<String, Object> body) {
        super(statusCode, path, body);
    }
}
