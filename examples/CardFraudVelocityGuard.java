// Use case 6 — end-to-end card-fraud velocity guard, driven entirely through the
// pulse-java SDK. This is a *runnable* demo: it exercises every public surface of
// the SDK against a Pulse server and prints what each call returned.
//
//   business story
//   --------------
//   A payments platform wants to stop "velocity fraud": a stolen card fired at
//   many merchants in seconds. The rule: more than 5 authorizations on one card
//   inside a 60-second window is fraud. Two jobs:
//     1. a STREAMING job that keeps a live per-card 60s count on the mesh, and
//     2. a SYNCHRONOUS decision on every fresh charge: ALLOW or DENY in one hop.
//
//   what the SDK does here (each step is a real SDK call over a real socket)
//   -----------------------------------------------------------------------
//     version()                          → handshake with the server
//     auth().login(...)                  → get + cache a JWT
//     streams().deploy(StreamBuilder...) → compile the velocity DSL → POST pipeline
//     iq().query(...) / iq().get(...)    → read the live per-card window state (B-106)
//     events().stream(...)               → tail live fraud-alert events over SSE
//     duplex("fraud-decider")            → send a charge, get ALLOW/DENY back (B-114)
//     PulseRateLimitException            → manual retry honouring retryAfterSeconds
//                                          (the SDK has no auto-retry — by design)
//
// To keep the demo self-contained in a sandbox it boots an EmbeddedPulse — a tiny
// in-process stand-in for the Pulse REST + WebSocket API that holds a per-card
// velocity map. Point `PULSE_URL` at a real Pulse deployment and the SAME SDK
// calls work unchanged; delete the EmbeddedPulse block and nothing else changes.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/CardFraudVelocityGuard.java
// Run:     java  -cp target/classes:<jackson-databind>:<jackson-core>:<jackson-annotations>:/tmp/ex \
//                 CardFraudVelocityGuard
import com.streamflow.pulse.client.DuplexChannel;
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.StreamBuilder;
import com.streamflow.pulse.client.StreamBuilder.Aggregators;
import com.streamflow.pulse.client.StreamBuilder.ToTopicOptions;
import com.streamflow.pulse.client.StreamBuilder.WindowOptions;
import com.streamflow.pulse.client.StreamBuilder.Windows;
import com.streamflow.pulse.client.IQResource.QueryOptions;
import com.streamflow.pulse.client.exceptions.PulseRateLimitException;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public final class CardFraudVelocityGuard {

    static final int FRAUD_THRESHOLD = 5;        // > 5 authorizations / 60s = fraud
    static final String DECIDER_AGENT = "fraud-decider";

    public static void main(String[] args) throws Exception {
        // ── boot the stand-in Pulse (skipped automatically when PULSE_URL is set) ──
        String configured = System.getenv("PULSE_URL");
        EmbeddedPulse embedded = null;
        String baseUrl;
        if (configured != null && !configured.isBlank()) {
            baseUrl = configured;
            System.out.println("Using external Pulse at " + baseUrl);
        } else {
            embedded = new EmbeddedPulse();
            embedded.seedAuthorizationStream();   // replay synthetic auths → per-card 60s counts
            embedded.start();
            baseUrl = "http://localhost:" + embedded.httpPort();
            System.out.println("Booted EmbeddedPulse (stand-in) at " + baseUrl
                    + "  [duplex ws on :" + (embedded.httpPort() + 1) + "]");
        }

        try (PulseClient client = PulseClient.builder().baseUrl(baseUrl).build()) {

            // 1 ── version handshake ────────────────────────────────────────────
            section("1. version()  — who am I talking to?");
            Map<String, Object> v = client.version();
            System.out.println("   server version = " + v.get("version") + "  build = " + v.get("build"));

            // 2 ── authenticate (JWT cached on the client) ─────────────────────
            section("2. auth().login()  — get + cache a JWT");
            Map<String, Object> login = client.auth().login("risk-ops", "demo-secret");
            System.out.println("   logged in as '" + login.get("username")
                    + "', token cached = " + (client.getToken() != null));

            // 3 ── declare + deploy the velocity pipeline via the streams DSL ──
            section("3. streams().deploy(StreamBuilder)  — the velocity job, server-side");
            StreamBuilder velocity = new StreamBuilder("card-velocity-60s")
                    .describedAs("Per-card authorization velocity in a 60s tumbling window")
                    .fromTopic("card-authorizations")
                    .filter("amount > 0")
                    .keyBy("cardId")
                    .window(Windows.tumbling("60s"), new WindowOptions().aggregations(Map.of(
                            "txCount", Aggregators.count(),
                            "totalAmount", Aggregators.sum("amount"),
                            "maxAmount", Aggregators.max("amount"))))
                    .filter("txCount > " + FRAUD_THRESHOLD)
                    .toTopic("fraud-alerts", new ToTopicOptions().sinkChannel("dashboard"));
            // Show the compiled JSON the SDK will POST (pure client-side compile).
            System.out.println("   compiled pipeline = " + client.streams().compile(velocity));
            Map<String, Object> deployed = client.streams().deploy(velocity);
            System.out.println("   deployed: id=" + deployed.get("id")
                    + " status=" + deployed.get("status"));

            // 4 ── Interactive Query: read the live window state like a DB (B-106) ─
            //      Wrapped in a manual retry loop: the FIRST call gets a 429 so we
            //      demonstrate honouring PulseRateLimitException.retryAfterSeconds()
            //      (the SDK deliberately does NOT auto-retry).
            section("4. iq().query()  — which cards are over the velocity limit right now?");
            Map<String, Object> overLimit = queryWithRetry(client, DECIDER_AGENT,
                    QueryOptions.builder()
                            .filter(QueryOptions.leaf("txCount", "gt", FRAUD_THRESHOLD))
                            .limit(10)
                            .build());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> entries =
                    (List<Map<String, Object>>) overLimit.getOrDefault("entries", List.of());
            System.out.println("   " + entries.size() + " card(s) over the limit:");
            for (Map<String, Object> e : entries) {
                System.out.println("     - " + e.get("key") + " → " + e.get("value"));
            }

            // 4b ─ point lookup on one card
            section("4b. iq().get()  — point lookup of one card's window");
            Map<String, Object> one = client.iq().get(DECIDER_AGENT, "card-007");
            System.out.println("   card-007 state = " + one.get("value"));

            // 5 ── tail live fraud-alert events over SSE ───────────────────────
            section("5. events().stream()  — tail the live fraud-alert feed (SSE)");
            List<Map<String, Object>> alerts = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> sse = client.events().stream(ev -> {
                alerts.add(ev);
                System.out.println("   ALERT ▸ card=" + ev.get("cardId")
                        + " txCount=" + ev.get("txCount") + " reason=" + ev.get("reason"));
            });
            try {
                sse.get(5, TimeUnit.SECONDS);   // server closes the stream after the backlog
            } catch (Exception timeoutOrDone) {
                sse.cancel(true);
            }
            System.out.println("   received " + alerts.size() + " alert event(s)");

            // 6 ── synchronous ALLOW/DENY on fresh charges via the duplex channel ─
            section("6. duplex(\"" + DECIDER_AGENT + "\")  — synchronous decision per charge (B-114)");
            try (DuplexChannel ch = client.duplex(DECIDER_AGENT)) {
                // card-007 is already hot (12 tx) → next charge should DENY.
                // card-100 is brand new → should ALLOW.
                for (String card : List.of("card-007", "card-100", "card-007")) {
                    String cid = ch.send(Map.of("cardId", card, "amount", 250), "charge-" + card + "-" + System.nanoTime());
                    Map<String, Object> out = ch.recv();
                    System.out.println("   charge on " + card + " → " + out.get("decision")
                            + "  (txCount=" + out.get("txCount") + ", reason=" + out.get("reason")
                            + ", corr=" + (out.get("correlation_id") != null) + ")");
                }
            }

            section("DONE — every SDK surface exercised end-to-end against a live server.");
        } finally {
            if (embedded != null) embedded.stop();
        }
    }

    /**
     * The point of step 4: the SDK surfaces a 429 as a typed
     * {@link PulseRateLimitException} carrying {@code retryAfterSeconds()}, but it
     * does NOT retry for you. This is the canonical caller-side retry loop.
     */
    private static Map<String, Object> queryWithRetry(PulseClient client, String agent, QueryOptions opts)
            throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                return client.iq().query(agent, opts);
            } catch (PulseRateLimitException e) {
                int wait = e.retryAfterSeconds().orElse(1);
                System.out.println("   rate-limited (429) on attempt " + attempt
                        + " — sleeping " + wait + "s then retrying (SDK has no auto-retry)");
                Thread.sleep(wait * 1000L);
            }
        }
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("== " + title);
    }

    // ======================================================================
    // EmbeddedPulse — an in-process stand-in for the Pulse REST + WS API.
    // Holds a per-card 60s velocity map and answers exactly the endpoints the
    // SDK calls. NOT part of the SDK; it exists only so this example runs with
    // no external dependencies. A real Pulse needs zero client-code changes.
    // ======================================================================
    static final class EmbeddedPulse {
        private final ObjectMapper json = new ObjectMapper();
        // cardId → [txCount, totalAmount] for the live 60s window.
        private final Map<String, int[]> velocity = new ConcurrentHashMap<>();
        private final AtomicInteger queryHits = new AtomicInteger();
        private HttpServer http;
        private ServerSocket wsServer;
        private volatile boolean running;
        private int port;

        int httpPort() { return port; }

        /** Replay a synthetic authorization stream so the window map has real content. */
        void seedAuthorizationStream() {
            // (cardId, number-of-authorizations-in-the-window)
            Map<String, Integer> burst = new LinkedHashMap<>();
            burst.put("card-001", 1); burst.put("card-002", 3); burst.put("card-003", 9);
            burst.put("card-004", 2); burst.put("card-005", 4); burst.put("card-006", 1);
            burst.put("card-007", 12); burst.put("card-008", 5);
            burst.forEach((card, n) -> {
                for (int i = 0; i < n; i++) authorize(card, 100 + i);
            });
        }

        /** One authorization → bump the card's window counters. Returns the new count. */
        private int authorize(String card, int amount) {
            int[] cell = velocity.computeIfAbsent(card, k -> new int[2]);
            synchronized (cell) {
                cell[0] += 1;
                cell[1] += amount;
                return cell[0];
            }
        }

        void start() throws IOException {
            this.port = findFreePortPair();
            http = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
            http.createContext("/", this::route);
            http.setExecutor(java.util.concurrent.Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "embedded-pulse-http");
                t.setDaemon(true);
                return t;
            }));
            http.start();
            running = true;
            startDuplexServer(port + 1);
        }

        void stop() {
            running = false;
            if (http != null) http.stop(0);
            try { if (wsServer != null) wsServer.close(); } catch (IOException ignored) { }
        }

        // ---- REST routing ------------------------------------------------
        private void route(HttpExchange ex) throws IOException {
            String path = ex.getRequestURI().getPath();
            try {
                if (path.equals("/api/pulse/version")) {
                    writeJson(ex, 200, Map.of("version", "2.6.1", "build", "embedded-demo", "edition", "pulse"));
                } else if (path.equals("/api/auth/login")) {
                    writeJson(ex, 200, Map.of(
                            "token", "demo-jwt-" + System.nanoTime(),
                            "refreshToken", "demo-refresh",
                            "username", "risk-ops"));
                } else if (path.equals("/api/pulse/pipelines") && isPost(ex)) {
                    Map<String, Object> body = readJson(ex);
                    writeJson(ex, 201, Map.of(
                            "id", "pl-card-velocity-60s",
                            "name", body.getOrDefault("name", "card-velocity-60s"),
                            "status", "RUNNING",
                            "nodeCount", ((List<?>) body.getOrDefault("nodes", List.of())).size()));
                } else if (path.endsWith("/state/query") && isPost(ex)) {
                    // Demonstrate the 429 path exactly once, then serve real data.
                    if (queryHits.incrementAndGet() == 1) {
                        ex.getResponseHeaders().add("Retry-After", "1");
                        writeJson(ex, 429, Map.of(
                                "error", "rate limited", "retryAfterSeconds", 1));
                        return;
                    }
                    writeJson(ex, 200, queryOverThreshold());
                } else if (path.contains("/state/value/")) {
                    String key = path.substring(path.indexOf("/state/value/") + "/state/value/".length());
                    key = java.net.URLDecoder.decode(key, StandardCharsets.UTF_8);
                    int[] cell = velocity.get(key);
                    if (cell == null) {
                        writeJson(ex, 404, Map.of("error", "Key not found", "key", key));
                    } else {
                        writeJson(ex, 200, Map.of("agentId", DECIDER_AGENT, "key", key,
                                "value", windowValue(cell)));
                    }
                } else if (path.equals("/api/pulse/events/stream")) {
                    streamFraudAlerts(ex);
                } else {
                    writeJson(ex, 404, Map.of("error", "no route", "path", path));
                }
            } catch (Exception e) {
                writeJson(ex, 500, Map.of("error", String.valueOf(e.getMessage())));
            }
        }

        private Map<String, Object> queryOverThreshold() {
            List<Map<String, Object>> entries = new ArrayList<>();
            new TreeMap<>(velocity).forEach((card, cell) -> {
                if (cell[0] > FRAUD_THRESHOLD) {
                    entries.add(Map.of("key", card, "value", windowValue(cell)));
                }
            });
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("entries", entries);
            resp.put("count", entries.size());
            resp.put("queryable", true);
            return resp;
        }

        private Map<String, Object> windowValue(int[] cell) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("txCount", cell[0]);
            m.put("totalAmount", cell[1]);
            m.put("overLimit", cell[0] > FRAUD_THRESHOLD);
            return m;
        }

        /** SSE: one alert per card currently over the limit, then close the stream. */
        private void streamFraudAlerts(HttpExchange ex) throws IOException {
            ex.getResponseHeaders().add("Content-Type", "text/event-stream");
            ex.getResponseHeaders().add("Cache-Control", "no-cache");
            ex.sendResponseHeaders(200, 0);
            try (OutputStream os = ex.getResponseBody()) {
                for (Map.Entry<String, int[]> e : new TreeMap<>(velocity).entrySet()) {
                    int[] cell = e.getValue();
                    if (cell[0] <= FRAUD_THRESHOLD) continue;
                    Map<String, Object> alert = new LinkedHashMap<>();
                    alert.put("type", "fraud-alert");
                    alert.put("cardId", e.getKey());
                    alert.put("txCount", cell[0]);
                    alert.put("reason", "velocity>" + FRAUD_THRESHOLD + " in 60s window");
                    os.write(("data: " + json.writeValueAsString(alert) + "\n\n")
                            .getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
                // stream ends → the SDK's CompletableFuture completes
            }
        }

        // ---- duplex WebSocket server (the synchronous-decision agent) -----
        private void startDuplexServer(int wsPort) throws IOException {
            wsServer = new ServerSocket();
            wsServer.bind(new InetSocketAddress("127.0.0.1", wsPort));
            Thread accept = new Thread(() -> {
                while (running) {
                    try {
                        Socket sock = wsServer.accept();
                        Thread h = new Thread(() -> handleDuplex(sock), "embedded-pulse-ws");
                        h.setDaemon(true);
                        h.start();
                    } catch (IOException e) {
                        if (running) System.err.println("ws accept: " + e.getMessage());
                        return;
                    }
                }
            }, "embedded-pulse-ws-accept");
            accept.setDaemon(true);
            accept.start();
        }

        private void handleDuplex(Socket sock) {
            try (sock; InputStream in = sock.getInputStream(); OutputStream out = sock.getOutputStream()) {
                if (!wsHandshake(in, out)) return;
                // Pulse sends a 'connected' frame first; the SDK awaits it as the handshake.
                wsSend(out, Map.of("type", "connected", "agentId", DECIDER_AGENT));
                while (running) {
                    String text = wsReadText(in);
                    if (text == null) return;               // close / eof
                    Map<String, Object> frame = json.readValue(text, Map.class);
                    if (!"send".equals(frame.get("type"))) continue;
                    String cid = String.valueOf(frame.get("correlationId"));
                    @SuppressWarnings("unchecked")
                    Map<String, Object> payload = (Map<String, Object>) frame.get("payload");
                    String card = String.valueOf(payload.get("cardId"));
                    int amount = ((Number) payload.getOrDefault("amount", 0)).intValue();
                    int count = authorize(card, amount);    // this charge counts toward the window
                    boolean deny = count > FRAUD_THRESHOLD;
                    Map<String, Object> decision = new LinkedHashMap<>();
                    decision.put("decision", deny ? "DENY" : "ALLOW");
                    decision.put("cardId", card);
                    decision.put("txCount", count);
                    decision.put("reason", deny
                            ? "velocity>" + FRAUD_THRESHOLD + " in 60s window"
                            : "within velocity limit");
                    wsSend(out, Map.of("type", "output", "correlationId", cid, "event", decision));
                }
            } catch (Exception ignored) {
                // connection closed by client — normal at end of demo
            }
        }

        private boolean wsHandshake(InputStream in, OutputStream out) throws IOException {
            // Read request headers until blank line; capture Sec-WebSocket-Key.
            StringBuilder req = new StringBuilder();
            int prev = -1, c;
            while ((c = in.read()) != -1) {
                req.append((char) c);
                if (prev == '\r' && c == '\n' && req.toString().endsWith("\r\n\r\n")) break;
                prev = c;
            }
            String key = null;
            for (String line : req.toString().split("\r\n")) {
                int i = line.indexOf(':');
                if (i > 0 && line.substring(0, i).trim().equalsIgnoreCase("Sec-WebSocket-Key")) {
                    key = line.substring(i + 1).trim();
                }
            }
            if (key == null) return false;
            String accept;
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                byte[] d = sha1.digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
                        .getBytes(StandardCharsets.US_ASCII));
                accept = Base64.getEncoder().encodeToString(d);
            } catch (Exception e) {
                return false;
            }
            String resp = "HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
            out.write(resp.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            return true;
        }

        /** Reads one masked text frame from the client; null on close/eof. */
        private String wsReadText(InputStream in) throws IOException {
            int b0 = in.read();
            if (b0 == -1) return null;
            int opcode = b0 & 0x0F;
            int b1 = in.read();
            if (b1 == -1) return null;
            boolean masked = (b1 & 0x80) != 0;
            long len = b1 & 0x7F;
            if (len == 126) {
                len = ((long) in.read() << 8) | in.read();
            } else if (len == 127) {
                len = 0;
                for (int i = 0; i < 8; i++) len = (len << 8) | in.read();
            }
            byte[] mask = new byte[4];
            if (masked) readFully(in, mask);
            byte[] payload = new byte[(int) len];
            readFully(in, payload);
            if (masked) for (int i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
            if (opcode == 0x8) return null;                 // close frame
            if (opcode == 0x1) return new String(payload, StandardCharsets.UTF_8);
            return wsReadText(in);                          // skip ping/pong/binary, read next
        }

        /** Writes one unmasked text frame (server→client). */
        private void wsSend(OutputStream out, Map<String, Object> msg) throws IOException {
            byte[] data = json.writeValueAsBytes(msg);
            out.write(0x81);                                // FIN + text
            if (data.length < 126) {
                out.write(data.length);
            } else if (data.length < 65536) {
                out.write(126);
                out.write((data.length >> 8) & 0xFF);
                out.write(data.length & 0xFF);
            } else {
                out.write(127);
                for (int i = 7; i >= 0; i--) out.write((int) ((long) data.length >> (8 * i)) & 0xFF);
            }
            out.write(data);
            out.flush();
        }

        private static void readFully(InputStream in, byte[] buf) throws IOException {
            int off = 0;
            while (off < buf.length) {
                int n = in.read(buf, off, buf.length - off);
                if (n == -1) throw new IOException("unexpected eof");
                off += n;
            }
        }

        private static int findFreePortPair() throws IOException {
            for (int p = 19090; p < 19290; p += 2) {
                try (ServerSocket a = new ServerSocket(); ServerSocket b = new ServerSocket()) {
                    a.bind(new InetSocketAddress("127.0.0.1", p));
                    b.bind(new InetSocketAddress("127.0.0.1", p + 1));
                    return p;
                } catch (IOException retry) {
                    // both ports must be free — try the next pair
                }
            }
            throw new IOException("no free port pair in 19090..19288");
        }

        // ---- small HTTP JSON helpers -------------------------------------
        private boolean isPost(HttpExchange ex) { return "POST".equalsIgnoreCase(ex.getRequestMethod()); }

        @SuppressWarnings("unchecked")
        private Map<String, Object> readJson(HttpExchange ex) throws IOException {
            byte[] body = ex.getRequestBody().readAllBytes();
            if (body.length == 0) return Map.of();
            return json.readValue(body, Map.class);
        }

        private void writeJson(HttpExchange ex, int status, Map<String, Object> body) throws IOException {
            byte[] bytes = json.writeValueAsBytes(body);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
