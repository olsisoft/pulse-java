// Predictive maintenance — WASM + ONNX + all four Pulse agent engines, deployed
// with the Java SDK only.
//
// The painful problem: industrial machines stream a vendor's BINARY sensor frame
// (vibration, bearing temp, rpm, motor current). The plant wants to predict a
// failure BEFORE it happens and auto-open a work order — without shipping the
// operational telemetry to a cloud model server, and without re-implementing the
// vendor's binary decoder in the engine.
//
// The answer: a sandboxed WASM module decodes the binary frame, an embedded ONNX
// model scores failure risk, and Pulse's four engines chain the rest — all by
// topic:
//
//   machine-telemetry ─▶ (streaming)  operators: [ WASM decode binary → JSON,
//                                                   ONNX mlPredict → riskScore ]  → telemetry-scored
//                     ─▶ (rule-based) gate riskScore > 0.8 → escalate            → maintenance-alerts
//                     ─▶ (llm)        draft a human work order  ◀─ bring your own LLM → workorder-drafts
//                     ─▶ (mcp)        open the ticket in the CMMS via your tools  → workorder-created
//   side branch off the scored stream:
//                     ─▶ (streaming)  per-machine rolling health (keyBy + window) → machine-health
//
// WASM and ONNX both run as operators INSIDE the first streaming agent, in-engine
// and sub-millisecond — no model server, no syscalls in the decoder.
//
// This file ONLY declares + deploys: login → wasm().upload → models().upload →
// pipelines().create. Feed it and read the output with FeedAndRead. The ONNX
// mlPredict operator needs the cluster data plane (PULSE_DATAPLANE_MODE=embedded
// or =remote); see the README "Run on the cluster" section.
//
// Prerequisites:
//   - Java 17+, the pulse-java SDK on the classpath (target/classes) + Jackson.
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - A JWT (PULSE_TOKEN) or PULSE_USER/PULSE_PASSWORD — upload + create are ADMIN.
//   - The WASM module + the ONNX model: prebuilt copies ship at
//     wasm-telemetry-decoder/telemetry-decoder.wasm and model/failure-risk.onnx
//     (rebuild from wasm-telemetry-decoder/ and model/gen_model.py); override the
//     paths with WASM_PATH / MODEL_PATH.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/predictive-maintenance-onnx/Main.java
// Run:     PULSE_URL=http://localhost:9090 PULSE_USER=alice PULSE_PASSWORD=secret \
//            java -cp target/classes:/tmp/ex Main
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseApiException;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Main {

    static final String WASM_MODULE = "telemetry-decoder";
    static final String ML_MODEL = "failure-risk";

    public static void main(String[] args) {
        String url = env("PULSE_URL", "http://localhost:9090");
        String wasmPath = env("WASM_PATH",
                "examples/predictive-maintenance-onnx/wasm-telemetry-decoder/telemetry-decoder.wasm");
        String modelPath = env("MODEL_PATH",
                "examples/predictive-maintenance-onnx/model/failure-risk.onnx");

        PulseClient.Builder b = PulseClient.builder().baseUrl(url);
        if (System.getenv("PULSE_TOKEN") != null) {
            b.token(System.getenv("PULSE_TOKEN"));
        }
        try (PulseClient client = b.build()) {
            if (System.getenv("PULSE_TOKEN") == null && System.getenv("PULSE_USER") != null) {
                client.auth().login(System.getenv("PULSE_USER"), System.getenv("PULSE_PASSWORD"));
            }

            // 1 ─ Register the binary decoder as a sandboxed WASM module (validated
            //     client-side: must export alloc/process/memory and import nothing).
            System.out.println("Uploading WASM module '" + WASM_MODULE + "' from " + wasmPath + " …");
            client.wasm().upload(new PulseClient.WasmResource.UploadOptions()
                    .name(WASM_MODULE)
                    .path(Path.of(wasmPath))
                    .description("Decodes a proprietary 30-byte little-endian sensor frame "
                            + "(base64-wrapped) into JSON features."));
            System.out.println("  ✓ uploaded (or already present)");

            // 2 ─ Register the ONNX failure-risk model. inputSchema is ORDERED — the
            //     mlPredict operator dense-packs features into the model's [1,4]
            //     input tensor IN THIS ORDER, so it must match how the model was
            //     trained: vibration, bearingTemp, rpm, motorCurrent.
            LinkedHashMap<String, String> inputSchema = new LinkedHashMap<>();
            inputSchema.put("vibration", "float");
            inputSchema.put("bearingTemp", "float");
            inputSchema.put("rpm", "float");
            inputSchema.put("motorCurrent", "float");
            System.out.println("Uploading ONNX model '" + ML_MODEL + "' from " + modelPath + " …");
            client.models().upload(new PulseClient.ModelsResource.UploadOptions()
                    .name(ML_MODEL)
                    .path(Path.of(modelPath))
                    .runtime("onnx")
                    .inputSchema(inputSchema)
                    .outputSchema(Map.of("riskScore", "float")));
            System.out.println("  ✓ uploaded (or already present)");

            // 3 ─ Build the multi-engine pipeline definition and deploy it.
            Map<String, Object> pipeline = buildPipeline();
            System.out.println("Creating pipeline 'predictive-maintenance-onnx' (4 engines: "
                    + "streaming[wasm+onnx] → rule-based → llm → mcp + window) …");
            Map<String, Object> created = client.pipelines().create(pipeline);
            System.out.println("  ✓ pipeline id: " + created.get("pipelineId"));

            System.out.println();
            System.out.println("Deployed. Feed `machine-telemetry` and read the hops:");
            System.out.println("  java -cp \"$CP\" FeedAndRead feed");
            System.out.println("  java -cp \"$CP\" FeedAndRead read telemetry-scored   # WASM-decoded + ONNX riskScore");
            System.out.println("  java -cp \"$CP\" FeedAndRead read maintenance-alerts  # only the high-risk machines");
        } catch (PulseApiException e) {
            System.err.println("Pulse API error " + e.statusCode() + " on " + e.path() + ": " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * The pipeline definition. Topics chain each node's outputTopic to the next
     * node's inputTopic.
     *
     * <p>The first streaming agent runs TWO operators in sequence — the WASM
     * decoder then the ONNX {@code mlPredict} scorer — so a raw binary frame
     * becomes a scored event in one in-engine pass. The rule-based gate, the LLM
     * work-order drafter and the MCP CMMS call follow; a windowed side branch
     * keeps per-machine rolling health off the critical path.
     */
    static Map<String, Object> buildPipeline() {
        return Map.of(
                "name", "predictive-maintenance-onnx",
                "description",
                "Binary sensor telemetry → WASM decode → ONNX failure-risk score → "
                        + "rule gate → LLM work order → MCP CMMS, with a per-machine health window.",
                "nodes", List.of(

                        // (1) streaming — WASM decode + ONNX score, in one pass.
                        //     machine-telemetry → telemetry-scored.
                        agent("Decode + Score (streaming)", Map.of(
                                "engine", "streaming",
                                "inputTopic", "machine-telemetry",
                                "outputTopic", "telemetry-scored",
                                "operators", List.of(
                                        // a) sandboxed binary-frame decoder → flat JSON features
                                        Map.of("type", "wasm",
                                                "module", WASM_MODULE,
                                                "onFailure", "DROP"),
                                        // b) embedded ONNX inference. Pulls the four feature keys
                                        //    from the decoded event and lands the model output
                                        //    nested under `risk` → risk.riskScore ∈ [0,1].
                                        Map.of("type", "mlPredict",
                                                "model", ML_MODEL,
                                                "inputFields", List.of(
                                                        "vibration", "bearingTemp", "rpm", "motorCurrent"),
                                                "outputField", "risk")))),

                        // (2) rule-based — deterministic risk gate + audit router. The model's
                        //     score is advisory; THIS is the business policy layer ops tune
                        //     without touching the stream topology. The first rule escalates
                        //     only machines scored above 0.8 to `maintenance-alerts`; the
                        //     `default` rule mirrors EVERY reading to `telemetry-normal` (a full
                        //     audit trail). Net effect (verified): `maintenance-alerts` carries
                        //     ONLY the high-risk readings (riskScore > 0.8) — so the LLM + CMMS
                        //     downstream act on real problems, never on healthy machines.
                        agent("Risk Gate (rule-based)", Map.of(
                                "engine", "rule-based",
                                "inputTopic", "telemetry-scored",
                                "outputTopic", "maintenance-alerts",
                                "rules", List.of(
                                        Map.of("condition", "risk.riskScore > 0.8",
                                                "action", "emit",
                                                "target", "maintenance-alerts",
                                                "label", "high-failure-risk"),
                                        Map.of("condition", "default",
                                                "action", "emit",
                                                "target", "telemetry-normal",
                                                "label", "healthy")))),

                        // (3) llm — draft a human work order from the alert. THE PROVIDER IS
                        //     YOURS: set it in Pulse → Settings → AI — local Ollama (nothing
                        //     leaves the plant; ideal for OT data) or cloud Claude/OpenAI —
                        //     hot-swappable, no redeploy. No provider → echo mode (the flow
                        //     still runs).
                        agent("Work Order Drafter (LLM)", Map.of(
                                "engine", "llm",
                                "inputTopic", "maintenance-alerts",
                                "outputTopic", "workorder-drafts",
                                "systemPrompt",
                                "You are a maintenance planner. From the machine telemetry + "
                                        + "riskScore, write a concise work order as JSON "
                                        + "{\"machineId\":\"…\",\"priority\":\"<low|medium|high|urgent>\","
                                        + "\"summary\":\"<one line>\",\"recommendedAction\":\"<one line>\"}.")),

                        // (4) mcp — open the work order in your CMMS (Maximo, SAP PM, Fiix, …).
                        //     A side branch off `maintenance-alerts`: needs the matching MCP
                        //     plugin in Pulse; without it the node sits `blocked` and does NOT
                        //     affect the main path. Install from Settings → MCP Plugins.
                        agent("CMMS Work Order (MCP)", Map.of(
                                "engine", "mcp",
                                "inputTopic", "maintenance-alerts",
                                "outputTopic", "workorder-created",
                                "mcpTools", List.of("cmms.create_work_order", "cmms.lookup_asset"))),

                        // (5) streaming window — per-machine rolling health off the scored
                        //     stream. Event-time tumbling window (emits as a continuous stream
                        //     advances the watermark). Demo: tumbling(60s) mean vibration +
                        //     max risk per machine — a live health dashboard feed.
                        agent("Machine Health (streaming)", Map.of(
                                "engine", "streaming",
                                "inputTopic", "telemetry-scored",
                                "outputTopic", "machine-health",
                                "operators", List.of(
                                        Map.of("type", "keyBy", "field", "machineId"),
                                        Map.of("type", "window",
                                                "spec", "tumbling(60s)",
                                                "aggregations", Map.of(
                                                        "samples", "count()",
                                                        "avgVibration", "avg(vibration)",
                                                        "maxRisk", "max(risk.riskScore)")))))));
    }

    static Map<String, Object> agent(String label, Map<String, Object> config) {
        return Map.of("type", "agent", "label", label, "config", config);
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }
}
