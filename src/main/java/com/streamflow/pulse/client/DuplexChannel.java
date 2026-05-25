package com.streamflow.pulse.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.pulse.client.exceptions.PulseApiException;
import com.streamflow.pulse.client.exceptions.PulseClientException;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * B-114 — a bidirectional duplex channel to a Pulse decision agent.
 *
 * <p>Opens ONE WebSocket to {@code /api/pulse/agents/{id}/duplex}: events are
 * streamed IN and the agent's correlated outputs come back OUT on the same
 * connection, matched by a correlation id. This collapses the two-connection
 * publish-then-poll pattern into a single round-trip — the path for
 * synchronous-decision microservices (fraud, pricing, A/B assignment).
 *
 * <p>The duplex endpoint runs on the Pulse WebSocket port (REST port + 1 by
 * convention); {@link #deriveWsUrl(String, String, String)} derives it from
 * the client's {@code baseUrl}.
 *
 * <p>Built on {@code java.net.http.WebSocket} (JDK 11+) so no new runtime
 * dependency is introduced. The async / callback-based JDK API is wrapped in a
 * blocking façade: a {@link WebSocket.Listener} accumulates partial text
 * frames, parses each completed message, and enqueues OUTPUT events onto an
 * internal {@link BlockingQueue} that {@link #recv()} drains.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * try (DuplexChannel ch = client.duplex("fraud-detector")) {
 *     String cid = ch.send(Map.of("amount", 5000), "tx-1");
 *     Map<String, Object> out = ch.recv();   // out.get("correlation_id") == "tx-1"
 *     if ("DENY".equals(((Map<?,?>) out.get("payload")).get("decision"))) {
 *         // ...
 *     }
 * }
 * }</pre>
 *
 * <p>The connection (and the server's {@code connected} handshake) is
 * established lazily on the first {@link #send} / {@link #recv} call, so the
 * channel can be constructed and stored before any I/O happens.
 *
 * <p>Instances are NOT designed for concurrent use across threads — drive a
 * single channel from one thread (the typical request/response loop). The
 * underlying frame intake is thread-safe, but the open handshake and close
 * lifecycle assume a single owning caller.
 */
public final class DuplexChannel implements AutoCloseable {

    /** Default {@link #recv()} timeout when none is supplied. */
    private static final Duration DEFAULT_RECV_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final String url;
    private final HttpClient http;
    private final ObjectMapper mapper;

    /** Parsed OUTPUT events, ready for {@link #recv()}. */
    private final BlockingQueue<Map<String, Object>> outputs = new LinkedBlockingQueue<>();
    /** First server-side error frame seen (surfaced from send/recv). */
    private final AtomicReference<PulseApiException> error = new AtomicReference<>();

    private volatile WebSocket ws;
    private volatile boolean closed;

    DuplexChannel(String url, HttpClient http, ObjectMapper mapper) {
        this.url = Objects.requireNonNull(url, "url");
        this.http = Objects.requireNonNull(http, "http");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    /**
     * Builds the duplex WebSocket URL from the client's REST {@code baseUrl}.
     *
     * <p>{@code http→ws} / {@code https→wss}, host unchanged, port → REST port
     * + 1 (the Pulse WebSocket server convention). The JWT, when set, rides as a
     * {@code token} query param (the server reads it from the upgrade request).
     *
     * @param baseUrl the REST base URL (e.g. {@code http://localhost:9090}).
     * @param agentId the target agent id (URL-encoded into the path).
     * @param token   the bearer JWT, or {@code null} for an unauthenticated URL.
     * @return the derived {@code ws://} / {@code wss://} URL.
     */
    public static String deriveWsUrl(String baseUrl, String agentId, String token) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("baseUrl must be a non-empty string");
        }
        if (agentId == null || agentId.isBlank()) {
            throw new IllegalArgumentException("agentId must be a non-empty string");
        }
        URI parts = URI.create(baseUrl);
        String scheme = "https".equalsIgnoreCase(parts.getScheme()) ? "wss" : "ws";
        String host = parts.getHost() != null ? parts.getHost() : "localhost";
        int port = parts.getPort();
        String netloc = port < 0 ? host : host + ":" + (port + 1);
        String path = "/api/pulse/agents/" + encode(agentId) + "/duplex";
        StringBuilder sb = new StringBuilder()
                .append(scheme).append("://").append(netloc).append(path);
        if (token != null && !token.isEmpty()) {
            sb.append("?token=").append(encode(token));
        }
        return sb.toString();
    }

    /**
     * Publishes {@code payload} to the agent's input topic.
     *
     * <p>Returns the correlation id that the matching output will carry —
     * generated as a fresh UUID when {@code correlationId} is {@code null}.
     * Opens the connection (and awaits the server's {@code connected}
     * handshake) on the first call.
     *
     * @param payload       the event payload to send.
     * @param correlationId an explicit correlation id, or {@code null} to
     *                      generate one.
     * @return the correlation id used for this send.
     */
    public String send(Map<String, Object> payload, String correlationId) {
        Objects.requireNonNull(payload, "payload");
        ensureOpen();
        raiseIfError();
        String cid = (correlationId != null && !correlationId.isBlank())
                ? correlationId
                : UUID.randomUUID().toString();
        Map<String, Object> frame = new java.util.LinkedHashMap<>();
        frame.put("type", "send");
        frame.put("correlationId", cid);
        frame.put("payload", payload);
        String text;
        try {
            text = mapper.writeValueAsString(frame);
        } catch (Exception e) {
            throw new PulseClientException("Failed to serialise duplex send frame", e);
        }
        try {
            ws.sendText(text, true).get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulseClientException("Interrupted while sending on duplex channel", e);
        } catch (Exception e) {
            throw new PulseClientException("Failed to send on duplex channel " + url, e);
        }
        return cid;
    }

    /** Convenience overload that generates a correlation id. */
    public String send(Map<String, Object> payload) {
        return send(payload, null);
    }

    /**
     * Returns the next agent OUTPUT event, blocking up to the default 30s
     * timeout. Acknowledgement / pong / connected frames are skipped
     * transparently; a server error frame is raised as a {@link PulseApiException}.
     *
     * <p>The returned map is the agent's output event (its {@code id} /
     * {@code topic} / {@code type} / {@code key} / {@code payload}) plus a
     * {@code correlation_id} field identifying the input that produced it.
     *
     * @return the next output event.
     * @throws PulseClientException if the timeout elapses before any output.
     * @throws PulseApiException    if the server sent an error frame.
     */
    public Map<String, Object> recv() {
        return recv(DEFAULT_RECV_TIMEOUT);
    }

    /**
     * Returns the next agent OUTPUT event, blocking up to {@code timeout}.
     *
     * @param timeout how long to wait for the next output event.
     * @return the next output event.
     * @throws PulseClientException if the timeout elapses before any output.
     * @throws PulseApiException    if the server sent an error frame.
     */
    public Map<String, Object> recv(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        ensureOpen();
        raiseIfError();
        try {
            Map<String, Object> out = outputs.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (out != null) {
                return out;
            }
            // Nothing arrived in time — surface a pending error if one landed.
            raiseIfError();
            throw new PulseClientException(
                    "Timed out after " + timeout.toMillis() + "ms waiting for a duplex output from " + url);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulseClientException("Interrupted while receiving on duplex channel", e);
        }
    }

    /** Closes the WebSocket. Idempotent — safe to call more than once. */
    @Override
    public void close() {
        closed = true;
        WebSocket sock = ws;
        if (sock != null) {
            try {
                sock.sendClose(WebSocket.NORMAL_CLOSURE, "client closing").orTimeout(
                        CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
                // Best-effort close — the connection may already be gone.
            } finally {
                ws = null;
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal: connection lifecycle + frame parsing
    // ------------------------------------------------------------------

    private synchronized void ensureOpen() {
        if (closed) {
            throw new PulseClientException("duplex channel is closed");
        }
        if (ws != null) {
            return;
        }
        // Used to surface the server's first handshake frame eagerly.
        CompletableFuture<Map<String, Object>> handshake = new CompletableFuture<>();
        FrameListener listener = new FrameListener(handshake);
        WebSocket socket;
        try {
            socket = http.newWebSocketBuilder()
                    .connectTimeout(CONNECT_TIMEOUT)
                    .buildAsync(URI.create(url), listener)
                    .get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulseClientException("Interrupted while opening duplex channel", e);
        } catch (Exception e) {
            throw new PulseClientException("Failed to open duplex channel " + url, e);
        }

        // The server sends a 'connected' frame first (or 'error' + close for an
        // unknown agent / disabled duplex). Surface the error eagerly.
        Map<String, Object> first;
        try {
            first = handshake.get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulseClientException("Interrupted awaiting duplex handshake", e);
        } catch (Exception e) {
            throw new PulseClientException("No handshake frame from duplex server " + url, e);
        }
        if ("error".equals(first.get("type"))) {
            try {
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "handshake error").orTimeout(
                        CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
                // best effort
            }
            throw new PulseApiException(400, url, first);
        }
        this.ws = socket;
    }

    private void raiseIfError() {
        PulseApiException e = error.get();
        if (e != null) {
            throw e;
        }
    }

    /**
     * Parses one fully-assembled JSON message and routes it. Package-private
     * and static so the dispatch logic is unit-testable without a live socket.
     *
     * @return the OUTPUT event to enqueue (with {@code correlation_id} attached),
     *         or {@code null} for ack / pong / connected frames that are skipped.
     * @throws PulseApiException if the message is an error frame.
     */
    static Map<String, Object> dispatch(Map<String, Object> msg, String url) {
        Object kind = msg.get("type");
        if ("output".equals(kind)) {
            Object rawEvent = msg.get("event");
            Map<String, Object> event = new java.util.LinkedHashMap<>();
            if (rawEvent instanceof Map<?, ?> m) {
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    event.put(String.valueOf(e.getKey()), e.getValue());
                }
            } else {
                event.put("value", rawEvent);
            }
            event.put("correlation_id", msg.get("correlationId"));
            return event;
        }
        if ("error".equals(kind)) {
            throw new PulseApiException(400, url, msg);
        }
        // ack / pong / connected → transparently skipped
        return null;
    }

    /** Parses raw JSON text into a map. Package-private for unit testing. */
    Map<String, Object> parse(String json) {
        try {
            Map<String, Object> parsed = mapper.readValue(json, MAP_TYPE);
            return parsed != null ? parsed : Map.of();
        } catch (Exception e) {
            throw new PulseClientException("Failed to parse duplex frame: " + json, e);
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Accumulates partial text frames until {@code last == true}, parses the
     * assembled message, and routes it: the first frame completes the handshake
     * future; subsequent OUTPUT frames are enqueued; error frames are recorded
     * so the next send / recv surfaces them.
     */
    private final class FrameListener implements WebSocket.Listener {
        private final CompletableFuture<Map<String, Object>> handshake;
        private final StringBuilder buffer = new StringBuilder();
        private volatile boolean handshakeDone;

        FrameListener(CompletableFuture<Map<String, Object>> handshake) {
            this.handshake = handshake;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String complete = buffer.toString();
                buffer.setLength(0);
                try {
                    Map<String, Object> msg = parse(complete);
                    if (!handshakeDone) {
                        handshakeDone = true;
                        handshake.complete(msg);
                    } else {
                        Map<String, Object> out = dispatch(msg, url);
                        if (out != null) {
                            outputs.offer(out);
                        }
                    }
                } catch (PulseApiException apiErr) {
                    // An error frame after the handshake — record it so the
                    // next blocking call surfaces it, and unblock recv().
                    error.compareAndSet(null, apiErr);
                } catch (RuntimeException parseErr) {
                    if (!handshakeDone) {
                        handshakeDone = true;
                        handshake.completeExceptionally(parseErr);
                    }
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            if (!handshakeDone) {
                handshakeDone = true;
                handshake.completeExceptionally(
                        new PulseClientException("duplex socket closed during handshake: " + reason));
            }
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable err) {
            if (!handshakeDone) {
                handshakeDone = true;
                handshake.completeExceptionally(
                        new PulseClientException("duplex socket error", err));
            }
        }
    }
}
