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
 * {@code client.events()} — live SSE stream of events flowing through the
 * Pulse engine.
 *
 * <h2>Quick start</h2>
 *
 * <pre>{@code
 * CompletableFuture<Void> done = client.events().stream(event -> {
 *     System.out.println(event.get("type"));
 * });
 *
 * // ... later, to stop:
 * done.cancel(true);
 *
 * // or block until the server closes the stream:
 * done.join();
 * }</pre>
 *
 * <p>The consumer callback is invoked on a background thread for each parsed
 * event. Cancel the returned {@link CompletableFuture} to stop the stream;
 * the HTTP connection is closed by the cancel hook.
 *
 * <p>For richer back-pressure / multi-subscriber semantics, wire a
 * {@link java.util.concurrent.Flow.Publisher} on top of this consumer (left
 * as a v3.0 follow-up — most operator use-cases just want the consumer-
 * callback shape).
 */
public final class EventsResource {

    private static final String PATH = "/api/pulse/events/stream";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final PulseClient client;

    EventsResource(PulseClient client) {
        this.client = client;
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
                .header("User-Agent", "pulse-client-java/2.6.0")
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
