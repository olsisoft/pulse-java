// Feed the COBOL pipeline and read any hop — over Pulse's HTTP event API.
//
// Works against any Pulse (standalone/FREE included): it publishes to and reads
// from the bus via /api/pulse/events, so no Kafka client and no extra dependency
// are needed — just the JDK + Jackson (already on the SDK classpath).
//
//   feed                 → publish sample fixed-width COBOL records to `legacy-cobol`
//   read <topic> [limit] → read <topic> (default modernized-events, limit 20) and print
//
// Topics along the flow:
//   cobol-parsed (after WASM parse)   cobol-ruled (after rules)
//   modernized-events (after the LLM) account-velocity (velocity window)
//   cobol-enriched-mcp (after MCP, when the plugin is installed)
//
// Auth: PULSE_TOKEN, or PULSE_USER/PULSE_PASSWORD (login). PULSE_URL default
// http://localhost:9090.
//
// Compile + run with the SAME classpath as Main (SDK + Jackson):
//   javac -cp "$CP" -d /tmp/ex examples/cobol-mainframe-modernization/FeedAndRead.java
//   java  -cp "$CP:/tmp/ex" FeedAndRead feed
//   java  -cp "$CP:/tmp/ex" FeedAndRead read modernized-events
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
            case "read" -> read(args.length > 1 ? args[1] : "modernized-events",
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
        // A handful of accounts; ACC0000007 fires 6 times (a velocity burst).
        List<String> recs = List.of(
                rec("ACC0000001", "DR", 150075, "LOAN", "USD", "20260609", "Monthly loan repayment branch 22"),
                rec("ACC0000002", "CR", 25000, "SAVE", "EUR", "20260609", "Interest credit Q2"),
                rec("ACC0000003", "DR", 9999900, "WIRE", "USD", "20260609", "Outbound wire to vendor ACME"),
                rec("ACC0000005", "CR", 25000, "RFND", "EUR", "20260609", "Refund for cancelled order 8842"),
                rec("ACC0000007", "DR", 5000, "CARD", "USD", "20260609", "POS purchase 1"),
                rec("ACC0000007", "DR", 5000, "CARD", "USD", "20260609", "POS purchase 2"),
                rec("ACC0000007", "DR", 5000, "CARD", "USD", "20260609", "POS purchase 3"),
                rec("ACC0000007", "DR", 5000, "CARD", "USD", "20260609", "POS purchase 4"),
                rec("ACC0000007", "DR", 5000, "CARD", "USD", "20260609", "POS purchase 5"),
                rec("ACC0000007", "DR", 5000, "CARD", "USD", "20260609", "POS purchase 6"));
        for (String r : recs) {
            String body = M.writeValueAsString(Map.of(
                    "topic", "legacy-cobol", "key", r.substring(0, 10).trim(), "value", r));
            post("/api/pulse/events", body);
        }
        System.out.println("Fed " + recs.size() + " fixed-width COBOL records to `legacy-cobol`.");
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
                // value wasn't JSON — show it raw (e.g. the raw COBOL on legacy-cobol)
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

    /** Build the fixed-width 80-col COBOL record (ASCII) from structured fields. */
    static String rec(String acct, String code, long amountCents, String prod,
                      String ccy, String date, String memo) {
        return pad(acct, 10) + pad(code, 2) + digits(amountCents, 12)
                + pad(prod, 4) + pad(ccy, 3) + pad(date, 8) + pad(memo, 40) + " ";
    }

    static String pad(String s, int n) {
        if (s.length() >= n) return s.substring(0, n);
        StringBuilder b = new StringBuilder(s);
        while (b.length() < n) b.append(' ');
        return b.toString();
    }

    static String digits(long v, int n) {
        String s = Long.toString(Math.max(0, v));
        StringBuilder b = new StringBuilder();
        while (b.length() + s.length() < n) b.append('0');
        b.append(s);
        return b.length() > n ? b.substring(b.length() - n) : b.toString();
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }
}
