// Deployer — Kafka in, AI inline, Kafka out, via the Pulse Java SDK.
//
// Your transactions already flow through a Kafka topic. This drops StreamFlow in
// between produce and consume and enriches every record in real time: a WASM
// operator derives the fraud features, an embedded ONNX model scores them, a rule
// gate decides, and the enriched record is produced back to Kafka — where your
// existing consumer reads it, unchanged. No Flink job, no model server, no glue.
//
//   Kafka topic "transactions"   (your stock producer)
//     ① kafka-source  → consumes the topic
//     ② streaming     → [ WASM fraud features , ONNX mlPredict fraudScore ] → transactions-scored
//     ③ kafka sink    → produces "transactions-enriched"  (your stock consumer reads this)
//     ④ rule-based    → fraud.fraudScore > 0.8  → fraud-alerts
//     ⑤ llm           → explain the alert (your provider)
//     ⑥ mcp           → open a case / reason over each alert
//
// Env defaults match the docker-compose service names.
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseApiException;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class Deployer {

    static final String WASM_MODULE = "fraud-features";
    static final String ML_MODEL = "fraud-risk";
    static final String PIPELINE_NAME = "kafka-inline-ai";

    public static void main(String[] args) throws Exception {
        String url = env("PULSE_URL", "http://localhost:9090");
        awaitHealthy(url);

        PulseClient.Builder b = PulseClient.builder().baseUrl(url);
        if (System.getenv("PULSE_TOKEN") != null) b.token(System.getenv("PULSE_TOKEN"));
        try (PulseClient client = b.build()) {
            if (System.getenv("PULSE_TOKEN") == null) {
                String user = env("PULSE_USER", "admin");
                String pass = env("PULSE_PASSWORD", "admin12345");
                ensureAdmin(url, user, pass);
                client.auth().login(user, pass);
            }

            System.out.println("Uploading WASM module '" + WASM_MODULE + "' …");
            client.wasm().upload(new PulseClient.WasmResource.UploadOptions()
                    .name(WASM_MODULE)
                    .path(Path.of(env("WASM_PATH",
                            "examples/kafka-inline-ai-stack/wasm-fraud-features/fraud-features.wasm")))
                    .description("Unwraps the Kafka record and derives the fraud feature vector."));
            System.out.println("  ✓ uploaded");

            // ONNX model. inputSchema is ORDERED — mlPredict packs the features into
            // the model's [1,5] tensor in this exact order.
            java.util.LinkedHashMap<String, String> inputSchema = new java.util.LinkedHashMap<>();
            inputSchema.put("amount_f", "float");
            inputSchema.put("is_foreign", "float");
            inputSchema.put("card_not_present", "float");
            inputSchema.put("night", "float");
            inputSchema.put("velocity_24h", "float");
            System.out.println("Uploading ONNX model '" + ML_MODEL + "' …");
            client.models().upload(new PulseClient.ModelsResource.UploadOptions()
                    .name(ML_MODEL)
                    .path(Path.of(env("MODEL_PATH",
                            "examples/kafka-inline-ai-stack/model/fraud-risk.onnx")))
                    .runtime("onnx").inputSchema(inputSchema)
                    .outputSchema(Map.of("fraudScore", "float")));
            System.out.println("  ✓ uploaded");

            // Idempotent deploy: a re-run of the one-shot deployer must not mint a
            // duplicate pipeline with its own Kafka consumer group.
            String existing = client.pipelines().list().stream()
                    .filter(p -> PIPELINE_NAME.equals(p.get("name")))
                    .map(p -> String.valueOf(p.getOrDefault("pipelineId", p.get("id"))))
                    .findFirst().orElse(null);
            if (existing != null) {
                System.out.println("Pipeline '" + PIPELINE_NAME + "' already deployed (id: "
                        + existing + ") — skipping create (idempotent).");
            } else {
                System.out.println("Creating pipeline '" + PIPELINE_NAME + "' …");
                Map<String, Object> created = client.pipelines().create(buildPipeline());
                System.out.println("  ✓ pipeline id: " + created.get("pipelineId"));
            }
            System.out.println();
            System.out.println("Live. Produce to 'transactions' and consume 'transactions-enriched' "
                    + "— every record now carries an inline fraud score.");
        } catch (PulseApiException e) {
            System.err.println("Pulse API error " + e.statusCode() + " on " + e.path() + ": " + e.getMessage());
            System.exit(1);
        }
    }

    static Map<String, Object> buildPipeline() {
        String bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
        return Map.of(
                "name", PIPELINE_NAME,
                "description",
                "Kafka 'transactions' → WASM fraud features → ONNX fraud score → Kafka "
                        + "'transactions-enriched', with a rule gate, an LLM explainer and an "
                        + "MCP case reasoner on the alert branch.",
                "nodes", List.of(

                        // ① Kafka source — consume the existing topic. Bare topic name; the
                        //    connector talks to your broker directly (its own consumer group).
                        source("Transactions (Kafka)", "kafka-source", cfg(
                                "outputTopic", "transactions-in",
                                "kafka.bootstrap.servers", bootstrap,
                                "kafka.topics", "transactions",
                                "kafka.group.id", "pulse-fraud-inline",
                                "kafka.auto.offset.reset", "earliest")),

                        // ② WASM features + ③ ONNX score, in one streaming pass. The WASM
                        //    unwraps the Kafka envelope and emits the flat feature vector
                        //    alongside the carried-through transaction fields; mlPredict adds
                        //    fraud.fraudScore.
                        agent("Score Fraud (WASM + ONNX)", Map.of(
                                "engine", "streaming",
                                "inputTopic", "transactions-in",
                                "outputTopic", "transactions-scored",
                                "operators", List.of(
                                        Map.of("type", "wasm", "module", WASM_MODULE, "onFailure", "DROP"),
                                        Map.of("type", "mlPredict", "model", ML_MODEL,
                                                "inputFields", List.of("amount_f", "is_foreign",
                                                        "card_not_present", "night", "velocity_24h"),
                                                "outputField", "fraud")))),

                        // ④ Kafka sink — produce the enriched record back to Kafka, where your
                        //    existing consumer reads it unchanged. acks=all for durability.
                        sink("Enriched out (Kafka)", "kafka", Map.of(
                                "inputTopic", "transactions-scored",
                                "kafka.bootstrap.servers", bootstrap,
                                "kafka.topic", "transactions-enriched",
                                "kafka.acks", "all")),

                        // ⑤ rule-based — the fraud policy. Only transactions the model scores
                        //    above 0.8 escalate; everyone else is marked clean.
                        agent("Fraud Gate (rule-based)", Map.of(
                                "engine", "rule-based",
                                "inputTopic", "transactions-scored",
                                "outputTopic", "fraud-alerts",
                                "rules", List.of(
                                        Map.of("condition", "fraud.fraudScore > 0.8", "action", "emit",
                                                "target", "fraud-alerts", "label", "likely-fraud"),
                                        Map.of("condition", "default", "action", "emit",
                                                "target", "transactions-ok", "label", "clean")))),

                        // ⑥ llm — explains why a transaction was flagged. Bring your own
                        //    provider (Ollama / cloud); echo until configured.
                        agent("Explain Alert (LLM)", Map.of(
                                "engine", "llm",
                                "inputTopic", "fraud-alerts",
                                "outputTopic", "fraud-explanations",
                                "systemPrompt",
                                "A transaction was flagged as likely fraud. In one short sentence, "
                                        + "explain the most likely reason from its fields (foreign card, "
                                        + "card-not-present, night, velocity, amount), as JSON "
                                        + "{\"reason\":\"<one line>\",\"recommend\":\"<block|review|allow>\"}.")),

                        // ⑦ mcp — open a case / reason over each alert through your tools
                        //    (built-in reasoner here; swap in your case-management MCP plugin).
                        agent("Case Reasoner (MCP)", Map.of(
                                "engine", "mcp",
                                "inputTopic", "fraud-alerts",
                                "outputTopic", "fraud-cases",
                                "mcpTools", List.of("demo.classifier")))));
    }

    static Map<String, Object> agent(String label, Map<String, Object> config) {
        return Map.of("type", "agent", "label", label, "config", config);
    }

    static Map<String, Object> source(String label, String subType, Map<String, Object> config) {
        return Map.of("type", "source", "subType", subType, "label", label, "config", config);
    }

    static Map<String, Object> sink(String label, String subType, Map<String, Object> config) {
        return Map.of("type", "sink", "subType", subType, "label", label, "config", config);
    }

    static Map<String, Object> cfg(Object... kv) {
        java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    static void awaitHealthy(String url) throws Exception {
        var http = java.net.http.HttpClient.newHttpClient();
        var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url + "/health")).GET().build();
        for (int i = 0; i < 120; i++) {
            try {
                if (http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode() == 200) {
                    System.out.println("Pulse is up."); return;
                }
            } catch (Exception retry) { /* not up yet */ }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Pulse did not become healthy at " + url);
    }

    static void ensureAdmin(String url, String user, String pass) throws Exception {
        var http = java.net.http.HttpClient.newHttpClient();
        String body = "{\"username\":\"" + user + "\",\"password\":\"" + pass + "\"}";
        try {
            http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(url + "/api/auth/register"))
                    .header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
        } catch (Exception ignore) { /* best effort */ }
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }
}
