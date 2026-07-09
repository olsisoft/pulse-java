// Feed the predictive-maintenance pipeline and read any hop — over Pulse's HTTP
// event API.
//
// Works against any cluster-backed Pulse: it publishes to and reads from the bus
// via /api/pulse/events, so no Kafka client and no extra dependency are needed —
// just the JDK + Jackson (already on the SDK classpath).
//
//   feed                 → publish sample BINARY sensor frames to `machine-telemetry`
//   read <topic> [limit] → read <topic> (default telemetry-scored, limit 20) and print
//
// Each frame is the proprietary 30-byte little-endian layout the WASM module
// decodes, base64-wrapped so it survives the JSON event value losslessly:
//   magic "SFM1" | machineId(8) | f32 vibration | f32 bearingTemp | u16 rpm |
//   f32 motorCurrent | u32 timestamp
//
// Topics along the flow:
//   telemetry-scored (after WASM decode + ONNX mlPredict — carries risk.riskScore)
//   maintenance-alerts (after the rule gate — only riskScore > 0.8)
//   workorder-drafts (after the LLM)   machine-health (per-machine window)
//   workorder-created (after MCP, when the CMMS plugin is installed)
//
// Auth: PULSE_TOKEN, or PULSE_USER/PULSE_PASSWORD (login). PULSE_URL default
// http://localhost:9090.
//
// Compile + run with the SAME classpath as Main (SDK + Jackson):
//   javac -cp "$CP" -d /tmp/ex examples/predictive-maintenance-onnx/FeedAndRead.java
//   java  -cp "$CP:/tmp/ex" FeedAndRead feed
//   java  -cp "$CP:/tmp/ex" FeedAndRead read telemetry-scored
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public final class FeedAndRead {

    static final ObjectMapper M = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static String base;
    static String token;

    public static void main(String[] args) throws Exception {
        base = env("PULSE_URL", "http://localhost:9090");
        token = authenticate();

        String cmd = args.length > 0 ? args[0] : "feed";
        switch (cmd) {
            case "feed" -> feed();
            case "read" -> read(args.length > 1 ? args[1] : "telemetry-scored",
                    args.length > 2 ? Integer.parseInt(args[2]) : 20);
            default -> {
                System.err.println("usage: FeedAndRead feed | read <topic> [limit]");
                System.exit(2);
            }
        }
    }

    static String authenticate() throws Exception {
        String t = System.getenv("PULSE_TOKEN");
        if (t != null && !t.isBlank()) return t;
        String user = env("PULSE_USER", "alice");
        String pass = env("PULSE_PASSWORD", "secret");
        String body = M.writeValueAsString(Map.of("username", user, "password", pass));
        HttpResponse<String> r = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + "/api/auth/login"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        return M.readTree(r.body()).path("accessToken").asText();
    }

    static void feed() throws Exception {
        // A small fleet: two healthy machines, one warning, one clearly failing,
        // and MCH-0007 degrading across six readings (a developing bearing fault).
        long ts = System.currentTimeMillis() / 1000L;
        List<String> frames = List.of(
                frame("MCH-0001", 2.1f, 54f, 1500, 12.0f, ts),   // healthy   → risk ~0.05
                frame("MCH-0002", 2.4f, 57f, 1500, 12.5f, ts),   // healthy
                frame("MCH-0003", 6.6f, 74f, 1500, 17.0f, ts),   // warning   → risk ~0.95
                frame("MCH-0009", 11.5f, 96f, 1500, 22.5f, ts),  // failing   → risk ~0.999
                frame("MCH-0007", 3.0f, 60f, 1500, 13.0f, ts),     // 1: still ok
                frame("MCH-0007", 4.5f, 66f, 1500, 14.5f, ts + 1),
                frame("MCH-0007", 6.0f, 73f, 1500, 16.0f, ts + 2),
                frame("MCH-0007", 7.8f, 81f, 1500, 18.0f, ts + 3),
                frame("MCH-0007", 9.6f, 89f, 1500, 20.0f, ts + 4),
                frame("MCH-0007", 11.2f, 96f, 1500, 22.0f, ts + 5)); // 6: failing
        for (String f : frames) {
            String key = f.length() >= 8 ? f.substring(0, 8) : f; // base64 prefix as a routing key
            String body = M.writeValueAsString(Map.of(
                    "topic", "machine-telemetry", "key", key, "value", f));
            post("/api/pulse/events", body);
        }
        System.out.println("Fed " + frames.size() + " binary sensor frames to `machine-telemetry`.");
    }

    static void read(String topic, int limit) throws Exception {
        String path = "/api/pulse/events/" + URLEncoder.encode(topic, StandardCharsets.UTF_8)
                + "?limit=" + limit;
        HttpResponse<String> r = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + path))
                        .header("Authorization", "Bearer " + token).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode events = M.readTree(r.body()).path("events");
        int n = events.isArray() ? events.size() : 0;
        System.out.println("`" + topic + "` — " + n + " event(s):");
        for (JsonNode e : events) {
            // The bus value is a string; for agent output it's a JSON envelope
            // whose `action` carries the operator result. Print the action if
            // present, else the raw value.
            String value = e.path("value").asText("");
            String shown = value;
            try {
                JsonNode v = M.readTree(value);
                JsonNode action = v.path("action");
                shown = action.isMissingNode() ? value : M.writeValueAsString(action);
            } catch (Exception ignore) {
                // value wasn't JSON — show it raw (e.g. the base64 frame on machine-telemetry)
            }
            System.out.println("  [" + e.path("key").asText("") + "] "
                    + (shown.length() > 300 ? shown.substring(0, 300) + "…" : shown));
        }
    }

    static void post(String path, String body) throws Exception {
        HTTP.send(HttpRequest.newBuilder(URI.create(base + path))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + token)
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Build the proprietary 30-byte little-endian sensor frame, base64-encoded.
     * Layout: magic "SFM1" | machineId(8, space-padded) | f32 vibration |
     * f32 bearingTemp | u16 rpm | f32 motorCurrent | u32 timestamp.
     */
    static String frame(String machineId, float vibration, float bearingTemp,
                        int rpm, float motorCurrent, long ts) {
        ByteBuffer buf = ByteBuffer.allocate(30).order(ByteOrder.LITTLE_ENDIAN);
        buf.put("SFM1".getBytes(StandardCharsets.US_ASCII));
        buf.put(pad8(machineId));
        buf.putFloat(vibration);
        buf.putFloat(bearingTemp);
        buf.putShort((short) rpm);
        buf.putFloat(motorCurrent);
        buf.putInt((int) ts);
        return Base64.getEncoder().encodeToString(buf.array());
    }

    /** machineId as exactly 8 ASCII bytes, space-padded / truncated. */
    static byte[] pad8(String s) {
        byte[] out = new byte[8];
        byte[] src = s.getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < 8; i++) out[i] = i < src.length ? src[i] : (byte) ' ';
        return out;
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }
}
