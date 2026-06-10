// COBOL mainframe modernization — a multi-engine Pulse pipeline, deployed with
// the Java SDK only.
//
// The painful problem: a legacy mainframe emits fixed-width COBOL copybook
// records, nobody wants to rewrite the parser, and the data must enter a modern
// real-time flow — ideally without sensitive data leaving the box.
//
// The answer: wrap the COBOL parser as a sandboxed WebAssembly module (zero host
// syscalls), then chain Pulse's agent engines by topic:
//
//   legacy-cobol ─▶ (streaming + WASM)  COBOL copybook → JSON      → cobol-parsed
//                ─▶ (rule-based)        deterministic business gate → cobol-ruled
//                ─▶ (llm)               classify the memo  ◀─ bring your own LLM
//                                                                   → modernized-events
//   side branches off the parsed / ruled stream:
//                ─▶ (mcp)               enrich via your MCP tools   → cobol-enriched-mcp
//                ─▶ (streaming window)  per-account velocity        → account-velocity
//                ─▶ (sink: jdbc)        parsed records → SQL table  → H2 / Postgres / …
//
// This file ONLY declares + deploys the pipeline (login → upload the WASM module
// → create the pipeline). Feed it and read the output with FeedAndRead. The JDBC
// sink needs the cluster data plane (PULSE_DATAPLANE_MODE=embedded|remote); see
// the README "Run on the cluster" section.
//
// Prerequisites:
//   - Java 17+, the pulse-java SDK on the classpath (target/classes) + Jackson.
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - A JWT (PULSE_TOKEN) or PULSE_USER/PULSE_PASSWORD — upload + create are authenticated.
//   - The WASM module: a prebuilt copy ships at
//     wasm-cobol-parser/cobol-copybook-parser.wasm (rebuild from wasm-cobol-parser/);
//     override the path with WASM_PATH.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/cobol-mainframe-modernization/Main.java
// Run:     PULSE_URL=http://localhost:9090 PULSE_USER=alice PULSE_PASSWORD=secret \
//            java -cp target/classes:/tmp/ex Main
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseApiException;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class Main {

    static final String WASM_MODULE = "cobol-copybook-parser";

    public static void main(String[] args) {
        String url = env("PULSE_URL", "http://localhost:9090");
        String wasmPath = env("WASM_PATH",
                "examples/cobol-mainframe-modernization/wasm-cobol-parser/cobol-copybook-parser.wasm");

        PulseClient.Builder b = PulseClient.builder().baseUrl(url);
        if (System.getenv("PULSE_TOKEN") != null) {
            b.token(System.getenv("PULSE_TOKEN"));
        }
        try (PulseClient client = b.build()) {
            if (System.getenv("PULSE_TOKEN") == null && System.getenv("PULSE_USER") != null) {
                client.auth().login(System.getenv("PULSE_USER"), System.getenv("PULSE_PASSWORD"));
            }

            // 1 ─ Register the COBOL parser as a sandboxed WASM module. The SDK
            //     validates it client-side (must export alloc/process/memory and
            //     import nothing) before the upload.
            System.out.println("Uploading WASM module '" + WASM_MODULE + "' from " + wasmPath + " …");
            client.wasm().upload(new PulseClient.WasmResource.UploadOptions()
                    .name(WASM_MODULE)
                    .path(Path.of(wasmPath))
                    .description("Parses fixed-width COBOL copybook records (PIC X / 9(10)V99) into JSON."));
            System.out.println("  ✓ uploaded (or already present)");

            // 2 ─ Build the multi-engine pipeline definition and deploy it.
            Map<String, Object> pipeline = buildPipeline();
            System.out.println("Creating pipeline 'cobol-mainframe-modernization' (5 agents + H2 sink) …");
            Map<String, Object> created = client.pipelines().create(pipeline);
            System.out.println("  ✓ pipeline id: " + created.get("pipelineId"));

            System.out.println();
            System.out.println("Deployed. Feed `legacy-cobol` and read `modernized-events`:");
            System.out.println("  java -cp \"$CP\" FeedAndRead feed");
            System.out.println("  java -cp \"$CP\" FeedAndRead read modernized-events");
        } catch (PulseApiException e) {
            System.err.println("Pulse API error " + e.statusCode() + " on " + e.path() + ": " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * The pipeline definition (the CreatePipelineRequest wire shape: {name,
     * description, nodes:[{type,label,config{...}}]}). Topics chain each node's
     * outputTopic to the next node's inputTopic.
     *
     * <p>Main path (works out of the box): WASM parse → rule-based → LLM →
     * {@code modernized-events}. Two side branches fan off the same stream — MCP
     * enrichment (needs an MCP plugin) and a per-account velocity window
     * (event-time streaming analytics) — so neither blocks the main path.
     */
    static Map<String, Object> buildPipeline() {
        return Map.of(
                "name", "cobol-mainframe-modernization",
                "description",
                "Legacy COBOL fixed-width feed → WASM parse → rules → LLM → modernized-events, "
                        + "with MCP enrichment + velocity-window side branches and a JDBC sink "
                        + "landing the parsed records in a queryable SQL table.",
                "nodes", List.of(

                        // (1) streaming + WASM — the COBOL parser. legacy-cobol → cobol-parsed.
                        agent("COBOL Copybook Parser", Map.of(
                                "engine", "streaming",
                                "inputTopic", "legacy-cobol",
                                "outputTopic", "cobol-parsed",
                                "operators", List.of(Map.of(
                                        "type", "wasm",
                                        "module", WASM_MODULE,
                                        "onFailure", "DROP")))),

                        // (2) rule-based — deterministic business gate. One forwarding rule
                        //     keeps the flow moving; add condition/target rules for real
                        //     routing (e.g. amount > 10000 → "review").
                        agent("Business Rules", Map.of(
                                "engine", "rule-based",
                                "inputTopic", "cobol-parsed",
                                "outputTopic", "cobol-ruled",
                                "rules", List.of(Map.of(
                                        "condition", "true",
                                        "action", "emit",
                                        "label", "forward-all")))),

                        // (3) llm — classify the free-text MEMO. THE PROVIDER IS YOURS: set it
                        //     in Pulse → Settings → AI — local Ollama (nothing leaves the box;
                        //     ideal for sensitive mainframe data) or cloud Claude/OpenAI —
                        //     hot-swappable, no redeploy. No provider → echo mode (the flow
                        //     still runs). This node's output is the modernized event.
                        agent("Memo Classifier (LLM)", Map.of(
                                "engine", "llm",
                                "inputTopic", "cobol-ruled",
                                "outputTopic", "modernized-events",
                                "systemPrompt",
                                "You classify a back-office transaction memo. Return JSON "
                                        + "{\"category\":\"<refund|chargeback|fee|transfer|other>\","
                                        + "\"riskNote\":\"<one short line>\"} for the `memo` field.")),

                        // (4) mcp — enrich via your MCP tools (CRM, sanctions list, …). A side
                        //     branch off `cobol-ruled`: needs the matching MCP plugin in Pulse;
                        //     without it the node sits `blocked` and does NOT affect the main
                        //     path. Install from Settings → MCP Plugins to activate.
                        agent("Customer & Sanctions Enrichment (MCP)", Map.of(
                                "engine", "mcp",
                                "inputTopic", "cobol-ruled",
                                "outputTopic", "cobol-enriched-mcp",
                                "mcpTools", List.of("crm.lookup", "sanctions.check"))),

                        // (5) streaming window — per-account velocity off the clean parsed
                        //     stream. Event-time tumbling window (emits as a continuous stream
                        //     advances the watermark). Demo: tumbling(60s). Prod: add a final
                        //     filter "txCount > 5" for a real velocity guard.
                        agent("Account Velocity (streaming)", Map.of(
                                "engine", "streaming",
                                "inputTopic", "cobol-parsed",
                                "outputTopic", "account-velocity",
                                "operators", List.of(
                                        Map.of("type", "keyBy", "field", "accountId"),
                                        Map.of("type", "window",
                                                "spec", "tumbling(60s)",
                                                "aggregations", Map.of(
                                                        "txCount", "count()",
                                                        "totalAmount", "sum(amount)"))))),

                        // (6) sink — land the business-ruled records in a real SQL store.
                        //     This is the modernization payoff: a 60-year-old fixed-width
                        //     COBOL feed becomes queryable rows in a relational table, with
                        //     NO bespoke JDBC code — a connector node mounted by the
                        //     enterprise bridge (subType "jdbc"). The table is auto-created
                        //     from the first event's payload keys (accountId, amount, …).
                        //     H2 here for a zero-setup demo; point jdbc.url at Postgres /
                        //     MySQL / Snowflake (same node, same connector) for production.
                        //     Requires the bridge data plane (PULSE_DATAPLANE_MODE=embedded
                        //     or =remote) — on a standalone/lite Pulse the connector isn't
                        //     mounted and the node reports as a failed component mount.
                        sink("Modernized SQL Store (H2)", "jdbc", Map.of(
                                "inputTopic", "cobol-parsed",
                                "jdbc.url", env("H2_URL",
                                        "jdbc:h2:file:/tmp/cobol-modernized;AUTO_SERVER=TRUE"),
                                "jdbc.username", "sa",
                                "jdbc.password", "",
                                "jdbc.sink.table", "modernized_events",
                                "jdbc.sink.insert.mode", "INSERT",
                                "jdbc.sink.auto.create", "true",
                                "jdbc.sink.batch.size", "100"))));
    }

    static Map<String, Object> agent(String label, Map<String, Object> config) {
        return Map.of("type", "agent", "label", label, "config", config);
    }

    /** A connector-backed sink node (mounted by the enterprise bridge). */
    static Map<String, Object> sink(String label, String subType, Map<String, Object> config) {
        return Map.of("type", "sink", "subType", subType, "label", label, "config", config);
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }
}
