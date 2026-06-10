// Feed the COBOL pipeline over StreamFlow's NATIVE binary client.
//
// This is the production-grade ingest path: the mainframe bridge talks to a
// StreamFlow engine node directly over the native protocol (NOT Kafka, NOT
// HTTP) — the same `StreamFlowClient` that benchmarks at millions of events/s.
// It publishes the fixed-width COBOL records to `legacy-cobol`; the Pulse
// pipeline (WASM parse → rules → LLM → H2 sink) then runs exactly as with the
// HTTP feeder.
//
//   java -cp "$NATIVE_CP" NativeFeeder [host] [port]      (defaults localhost 9094)
//
// TOPOLOGY — this needs the cluster ("streamflow" data plane), not standalone:
//   * A StreamFlow engine node with the native protocol on :9094, AND
//   * Pulse pointed at that node:
//       PULSE_DATAPLANE_MODE=remote
//       PULSE_DATAPLANE_REMOTE_BOOTSTRAP=<node-host>:<native-port>
//       PULSE_DATAPLANE_REMOTE_NATIVE=true
//       PULSE_DATAPLANE_REMOTE_NATIVE_PORT=9094
//   so the pipeline consumes from the very topic this feeder produces to.
// See the README "Run on the cluster" section for the full recipe.
//
// TOPIC NAMING — important. Pulse namespaces every pipeline topic by org on the
// mesh: the pipeline's logical `legacy-cobol` is physically
// `pulse.<orgId>.legacy-cobol`. A raw native producer writes the literal topic
// you give it, so to feed a Pulse pipeline you must target that physical name.
// Set STREAMFLOW_TOPIC to it (single-tenant default org is all-zeros-…-001):
//   STREAMFLOW_TOPIC=pulse.00000000-0000-0000-0000-000000000001.legacy-cobol
// Verified live: producing there → WASM parse → JDBC sink → rows in H2.
// Left unset, it produces to bare `legacy-cobol` (handy for a standalone engine
// you consume directly, e.g. with the native CLI consumer).
//
// Unlike Main/FeedAndRead (pulse-java SDK), this uses the engine SDK
// (`com.streamflow:streamflow-sdk`, which pulls streamflow-core). Build a
// classpath with those jars — e.g. from a built checkout:
//   NATIVE_CP="$(find . -name 'streamflow-sdk-*.jar' -o -name 'streamflow-core-*.jar' \
//       | grep -v sources | tr '\n' ':')/tmp/ex"
//   javac -cp "$NATIVE_CP" -d /tmp/ex examples/cobol-mainframe-modernization/NativeFeeder.java
//   java  --enable-native-access=ALL-UNNAMED -cp "$NATIVE_CP" NativeFeeder
//
// Auth: STREAMFLOW_TOKEN (or null for an auth-disabled node). TLS: plaintext
// here; pass an SSLSocketFactory to nativeClient(...) for a TLS node.
import com.streamflow.sdk.StreamFlowClient;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class NativeFeeder {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : env("STREAMFLOW_HOST", "localhost");
        int port = args.length > 1 ? Integer.parseInt(args[1])
                : Integer.parseInt(env("STREAMFLOW_NATIVE_PORT", "9094"));
        String token = System.getenv("STREAMFLOW_TOKEN"); // null ⇒ auth-disabled node
        // Bare `legacy-cobol` feeds a standalone engine you consume directly;
        // set STREAMFLOW_TOPIC=pulse.<orgId>.legacy-cobol to feed a Pulse
        // pipeline in remote mode (see the file header for why).
        String topic = env("STREAMFLOW_TOPIC", "legacy-cobol");

        // partitions = per-key routing hint; account id is the key so a hot
        // account's records land on one partition (ordered velocity per account).
        try (StreamFlowClient client =
                     StreamFlowClient.nativeClient(host, port, 16, token, null)) {

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
                String key = r.substring(0, 10).trim();
                // Blocking get() so a failure surfaces immediately; drop the .get()
                // for fire-and-forget throughput.
                client.producer()
                        .send(topic, key, r.getBytes(StandardCharsets.UTF_8))
                        .get();
            }
            System.out.println("Produced " + recs.size()
                    + " fixed-width COBOL records to `" + topic + "` over the native client ("
                    + host + ":" + port + ").");
            System.out.println("Read the modernized output with FeedAndRead "
                    + "(read modernized-events) or query the H2 table.");
        }
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
