package com.streamflow.pulse.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.pulse.client.exceptions.PulseApiException;
import com.streamflow.pulse.client.exceptions.PulseAuthException;
import com.streamflow.pulse.client.exceptions.PulseClientException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * {@code client.events()} — publish to, read from, and live-stream the events
 * flowing through the Pulse engine.
 *
 * <h2>Quick start</h2>
 *
 * <pre>{@code
 * // Producer side — write to a pipeline's input topic; the engine carries
 * // each event through every downstream stage (decode, rules, LLM, sink, …):
 * client.events().publish("legacy-cobol", record);
 *
 * // Inspect a hop while developing:
 * for (Map<String, Object> e : client.events().read("modernized-events", 20))
 *     System.out.println(e.get("value"));
 *
 * // Live SSE stream:
 * CompletableFuture<Void> done = client.events().stream(event -> {
 *     System.out.println(event.get("type"));
 * });
 * done.cancel(true);   // stop early, or done.join() to block
 * }</pre>
 *
 * <p>The {@code stream} consumer callback is invoked on a background thread for
 * each parsed event. Cancel the returned {@link CompletableFuture} to stop the
 * stream; the HTTP connection is closed by the cancel hook.
 */
public final class EventsResource {

    private static final String PATH = "/api/pulse/events/stream";
    private static final String EVENTS_PATH = "/api/pulse/events";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final PulseClient client;

    EventsResource(PulseClient client) {
        this.client = client;
    }

    /**
     * Publishes one event to {@code topic} via {@code POST /api/pulse/events}.
     *
     * <p>This is the producer side of a pipeline: write to a pipeline's
     * {@code inputTopic} and the engine carries the event through every
     * downstream stage — there is no glue code to write between stages.
     *
     * @param topic the destination topic (a pipeline's input topic, typically);
     *              auto-created on first publish if it doesn't exist
     * @param key   optional partition / routing key (may be {@code null}); events
     *              sharing a key stay ordered on the same partition
     * @param value the event payload as a string (JSON or raw text)
     * @return the server acknowledgement ({@code eventId}, {@code topic},
     *         {@code key}, {@code timestamp})
     * @throws IllegalArgumentException if {@code topic} is null/blank
     * @throws PulseAuthException if no token is set
     * @throws PulseApiException on a non-2xx response (e.g. a topic key-policy
     *         violation surfaces as 400)
     */
    public Map<String, Object> publish(String topic, String key, String value) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        Map<String, Object> body = new HashMap<>();
        body.put("topic", topic);
        if (key != null) body.put("key", key);
        body.put("value", value);
        return client.request("POST", EVENTS_PATH, body, true);
    }

    /** Publishes one event with no routing key. See {@link #publish(String, String, String)}. */
    public Map<String, Object> publish(String topic, String value) {
        return publish(topic, null, value);
    }

    /**
     * Reads the most recent events on {@code topic} via
     * {@code GET /api/pulse/events/{topic}?limit=N} — handy for inspecting a
     * pipeline hop while developing.
     *
     * @param topic the topic to read
     * @param limit max number of events to return (newest first); clamped to ≥ 1
     * @return the events list — each a map with {@code id}, {@code topic},
     *         {@code key}, {@code value}, {@code timestamp}; empty if the topic
     *         has no events
     * @throws IllegalArgumentException if {@code topic} is null/blank
     * @throws PulseAuthException if no token is set
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> read(String topic, int limit) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        String path = EVENTS_PATH + "/" + enc(topic) + "?limit=" + Math.max(1, limit);
        Map<String, Object> result = client.request("GET", path, null, true);
        Object events = result.get("events");
        if (events instanceof List<?> list) {
            return (List<Map<String, Object>>) list;
        }
        return java.util.Collections.emptyList();
    }

    /**
     * Subscribes to {@code GET /api/pulse/events/stream}, invoking {@code onEvent}
     * for each parsed event on a background thread.
     *
     * @param onEvent  callback receiving each parsed event payload as a Map.
     *                 Non-JSON payloads surface as {@code {"data": "<raw text>"}}.
     * @return a {@link CompletableFuture} that completes when the server ends
     *         the stream, or fails if the auth check or HTTP transport
     *         errors. Cancel the future to stop the stream early.
     * @throws PulseAuthException immediately (synchronously) if no token is set.
     */
    public CompletableFuture<Void> stream(Consumer<Map<String, Object>> onEvent) {
        String token = client.getToken();
        if (token == null || token.isEmpty()) {
            Map<String, Object> body = Map.of("error", "no token set for SSE stream");
            throw new PulseAuthException(401, PATH, body);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(client.baseUrlInternal() + PATH))
                // No request-level timeout — the connection must stay open
                // for the lifetime of the stream. The caller cancels via the
                // returned future.
                .header("Authorization", "Bearer " + token)
                .header("Accept", "text/event-stream")
                .header("Cache-Control", "no-cache")
                .header("User-Agent", "pulse-client-java/2.7.8")
                .GET()
                .build();

        HttpClient http = client.httpClientInternal();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        CompletableFuture<HttpResponse<java.io.InputStream>> sendFuture =
                http.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());

        CompletableFuture<Void> resultFuture = sendFuture.thenApplyAsync(response -> {
            if (response.statusCode() >= 400) {
                Map<String, Object> errBody = readErrorBody(response, client.mapperInternal());
                throw translateError(response.statusCode(), errBody);
            }

            ObjectMapper mapper = client.mapperInternal();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
                List<String> dataLines = new ArrayList<>();
                String line;
                while ((line = reader.readLine()) != null) {
                    if (cancelled.get()) break;

                    if (line.isEmpty()) {
                        // Event boundary — assemble + dispatch
                        if (!dataLines.isEmpty()) {
                            String payload = String.join("\n", dataLines);
                            dataLines.clear();
                            try {
                                Map<String, Object> event = mapper.readValue(payload, MAP_TYPE);
                                onEvent.accept(event);
                            } catch (IOException parseErr) {
                                // Non-JSON — surface as {data: ...}
                                Map<String, Object> raw = new HashMap<>();
                                raw.put("data", payload);
                                onEvent.accept(raw);
                            }
                        }
                        continue;
                    }
                    if (line.startsWith(":")) continue;          // SSE comment
                    if (line.startsWith("data:")) {
                        String value = line.substring(5);
                        if (value.startsWith(" ")) value = value.substring(1);
                        dataLines.add(value);
                    }
                    // event:/id:/retry: consumed but not surfaced.
                }
            } catch (IOException ioErr) {
                if (!cancelled.get()) {
                    throw new PulseClientException(
                            "pulse: SSE stream read error: " + ioErr.getMessage(), ioErr);
                }
            }
            return null;
        });

        // Wire cancellation: when the returned future is cancelled, also
        // cancel the underlying send so the connection closes.
        resultFuture.whenComplete((v, err) -> {
            if (err instanceof java.util.concurrent.CancellationException) {
                cancelled.set(true);
                sendFuture.cancel(true);
            }
        });

        return resultFuture;
    }

    /**
     * B-113 — the changes that touched a state key between two instants.
     *
     * <p>{@code GET /api/pulse/iq/agents/{affectingState}/state/replay/{key}?from=&to=&limit=}.
     * {@code affectingState} is the agent whose state store to inspect;
     * {@code key} is the state key. {@code from} / {@code to} accept the same
     * specs as {@link IQResource#get(String, String, String)} ({@code now}, a
     * relative offset like {@code -1h}, an ISO-8601 instant, or epoch millis).
     * Pass {@link ReplayOptions#defaults()} for the server defaults
     * ({@code from=-1h}, {@code to=now}, {@code limit=100}).
     *
     * <p>Returns the ordered list of changes (the unwrapped {@code changes}
     * field of the response), each carrying {@code timestamp},
     * {@code changeType} ({@code PUT} / {@code DELETE}), the resulting
     * {@code value}, and {@code eventId} when known:
     *
     * <pre>{@code
     * List<Map<String, Object>> changes = client.events().replay(
     *         "user-sessions", "u42",
     *         EventsResource.ReplayOptions.builder()
     *                 .from("2026-05-24T10:00:00Z").to("2026-05-24T11:00:00Z").build());
     * }</pre>
     *
     * @throws PulseAuthException if no token is set.
     * @throws com.streamflow.pulse.client.exceptions.PulseNotFoundException
     *         when the agent is not queryable.
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> replay(String affectingState, String key, ReplayOptions options) {
        String path = "/api/pulse/iq/agents/" + enc(affectingState)
                + "/state/replay/" + enc(key) + options.toQuery();
        Map<String, Object> result = client.request("GET", path, null, true);
        Object changes = result.get("changes");
        if (changes instanceof List<?> list) {
            return (List<Map<String, Object>>) list;
        }
        return java.util.Collections.emptyList();
    }

    /** Convenience overload — replay with default range (from=-1h, to=now, limit=100). */
    public List<Map<String, Object>> replay(String affectingState, String key) {
        return replay(affectingState, key, ReplayOptions.defaults());
    }

    /** URL-encodes a path segment so values containing {@code /}, spaces, etc.
     *  round-trip safely. Server's URLDecoder reverses this exactly. */
    private static String enc(String segment) {
        return java.net.URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Optional {@code from} / {@code to} / {@code limit} for
     * {@link EventsResource#replay}. All default to the server defaults
     * ({@code from=-1h}, {@code to=now}, {@code limit=100}) and are always
     * sent on the wire. Build via {@link #builder()} or use {@link #defaults()}.
     */
    public static final class ReplayOptions {
        private static final ReplayOptions DEFAULTS = new ReplayOptions("-1h", "now", 100);

        private final String from;
        private final String to;
        private final int limit;

        private ReplayOptions(String from, String to, int limit) {
            this.from = from;
            this.to = to;
            this.limit = limit;
        }

        public static ReplayOptions defaults() { return DEFAULTS; }

        public static Builder builder() { return new Builder(); }

        String toQuery() {
            return "?from=" + java.net.URLEncoder.encode(from, StandardCharsets.UTF_8)
                    + "&to=" + java.net.URLEncoder.encode(to, StandardCharsets.UTF_8)
                    + "&limit=" + limit;
        }

        public static final class Builder {
            private String from = "-1h";
            private String to = "now";
            private int limit = 100;

            public Builder from(String from) { this.from = from; return this; }
            public Builder to(String to) { this.to = to; return this; }
            public Builder limit(int limit) { this.limit = limit; return this; }
            public ReplayOptions build() { return new ReplayOptions(from, to, limit); }
        }
    }

    private static Map<String, Object> readErrorBody(
            HttpResponse<java.io.InputStream> response, ObjectMapper mapper) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[4096];
            int n;
            while ((n = reader.read(buf)) != -1) {
                sb.append(buf, 0, n);
                if (sb.length() > 8192) break; // cap error body size
            }
            if (sb.length() == 0) return null;
            try {
                return mapper.readValue(sb.toString(), MAP_TYPE);
            } catch (IOException nonJson) {
                Map<String, Object> raw = new HashMap<>();
                raw.put("error", sb.toString());
                return raw;
            }
        } catch (IOException ignored) {
            return null;
        }
    }

    private static PulseApiException translateError(int status, Map<String, Object> body) {
        if (status == 401) return new PulseAuthException(status, PATH, body);
        return new PulseApiException(status, PATH, body);
    }
}
