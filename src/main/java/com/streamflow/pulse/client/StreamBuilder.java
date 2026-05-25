package com.streamflow.pulse.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * B-107 — Kafka-Streams-like declarative DSL that compiles to a Pulse pipeline.
 *
 * <p>The DSL is <b>server-side execution, client-side declaration</b>: the
 * operator chain is built in Java, compiled to the JSON pipeline shape that
 * the Pulse server's {@code StreamingOperatorValidator} accepts, and POSTed
 * to {@code /api/pulse/pipelines}. Stream processing then runs on the Pulse
 * engine (3.6 M evt/s native throughput), not in the calling JVM.
 *
 * <p>This is the opposite of Kafka Streams (which executes in the caller's
 * JVM). The trade-off: you can't do microsecond client-side compute, but you
 * get infinite-scale stateful streaming, durable replicated state queryable
 * via B-106 IQ, and the same DSL works from any of the 5 Pulse SDKs.
 *
 * <h2>Quick start</h2>
 * <pre>{@code
 * StreamBuilder builder = new StreamBuilder("iot-temperature-aggregator")
 *     .fromTopic("sensor-readings", new StreamBuilder.FromTopicOptions().sourceEngine("mqtt"))
 *     .keyBy("deviceId")
 *     .window(
 *         StreamBuilder.Windows.tumbling("60s"),
 *         new StreamBuilder.WindowOptions().aggregations(
 *             Map.of("avgTemp", StreamBuilder.Aggregators.avg("temperature"))))
 *     .filter("avgTemp > 75")
 *     .toTopic(
 *         "sensor-minute-averages",
 *         new StreamBuilder.ToTopicOptions().sinkChannel("email"));
 *
 * try (PulseClient client = PulseClient.builder()
 *         .baseUrl("http://localhost:9090")
 *         .token("ey...")
 *         .build()) {
 *     Map<String, Object> deployed = client.streams().deploy(builder);
 * }
 * }</pre>
 *
 * <p>Supported operators (mirror the 11 validated by
 * {@code com.streamflow.pulse.streaming.StreamingOperatorValidator}):
 * {@code filter}, {@code map}, {@code flatMap}, {@code keyBy}, {@code window},
 * {@code branch}, {@code enrich}, {@code enrichAsync}, {@code cep},
 * {@code broadcastJoin}, {@code cdcJoin}.
 *
 * <p>Conditions and field-expressions are passed as <b>strings</b>. The Pulse
 * streaming runtime parses them server-side. Lambdas / closures are NOT
 * supported because they cannot be serialised to JSON.
 */
public final class StreamBuilder {

    private String name;
    private String description;
    private String agentLabel;
    private String inputTopic;
    private String sourceEngine;
    private Map<String, Object> sourceConfig = new LinkedHashMap<>();
    private String sourceLabel;
    private String outputTopic;
    private String sinkChannel;
    private Map<String, Object> sinkConfig = new LinkedHashMap<>();
    private String sinkLabel;
    private final List<Map<String, Object>> operators = new ArrayList<>();

    /** Builder with no preset name. Use {@link #named(String)} or pass name to {@link #build(String)}. */
    public StreamBuilder() {}

    /** Builder with a preset pipeline name. */
    public StreamBuilder(String name) {
        requireNonBlank("name", name);
        this.name = name;
    }

    // ------------------------------------------------------------------
    // Source
    // ------------------------------------------------------------------

    /** Sets the input topic + default source engine ({@code "kafka"}). */
    public StreamBuilder fromTopic(String topic) {
        return fromTopic(topic, new FromTopicOptions());
    }

    /** Sets the input topic + source node config. */
    public StreamBuilder fromTopic(String topic, FromTopicOptions options) {
        requireNonBlank("topic", topic);
        Objects.requireNonNull(options, "options");
        this.inputTopic = topic;
        this.sourceEngine = options.sourceEngine != null ? options.sourceEngine : "kafka";
        this.sourceConfig = new LinkedHashMap<>(options.sourceConfig != null ? options.sourceConfig : Map.of());
        this.sourceLabel = options.label;
        return this;
    }

    // ------------------------------------------------------------------
    // Operators — each appends one entry to this.operators
    // ------------------------------------------------------------------

    /** Filter operator. {@code condition} is a CEL-like expression string. */
    public StreamBuilder filter(String condition) {
        requireNonBlank("condition", condition);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "filter");
        op.put("condition", condition);
        operators.add(op);
        return this;
    }

    /**
     * Map operator. At least one of {@code fields} or {@code targetType} on the
     * options object must be set (server rejects a map that does nothing).
     */
    public StreamBuilder map(MapOptions options) {
        Objects.requireNonNull(options, "options");
        if (options.fields == null && options.targetType == null) {
            throw new IllegalArgumentException(
                "map operator does nothing — provide 'fields' or 'targetType'");
        }
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "map");
        if (options.fields != null) op.put("fields", new LinkedHashMap<>(options.fields));
        if (options.targetType != null) op.put("targetType", options.targetType);
        operators.add(op);
        return this;
    }

    /** Flat-map: explode an array-valued field into one event per element. */
    public StreamBuilder flatMap(String splitField) {
        requireNonBlank("splitField", splitField);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "flatMap");
        op.put("splitField", splitField);
        operators.add(op);
        return this;
    }

    /** Group the stream by a top-level field value. Required before stateful operators. */
    public StreamBuilder keyBy(String field) {
        requireNonBlank("field", field);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "keyBy");
        op.put("field", field);
        operators.add(op);
        return this;
    }

    /** Window operator with no extra options. */
    public StreamBuilder window(WindowSpec spec) {
        return window(spec, new WindowOptions());
    }

    /** Window operator. Aggregates events inside a window. */
    public StreamBuilder window(WindowSpec spec, WindowOptions options) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(options, "options");
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "window");
        op.put("spec", spec.spec());
        if (options.aggregations != null) {
            op.put("aggregations", new LinkedHashMap<>(options.aggregations));
        }
        if (options.outputTopic != null) op.put("outputTopic", options.outputTopic);
        if (options.trigger != null) op.put("trigger", options.trigger);
        operators.add(op);
        return this;
    }

    /** Same as {@link #window(WindowSpec, WindowOptions)} but takes the raw spec string. */
    public StreamBuilder window(String spec, WindowOptions options) {
        requireNonBlank("spec", spec);
        return window(new WindowSpec(spec), options);
    }

    /** Branch operator: route events to different topics by condition. */
    public StreamBuilder branch(List<BranchSpec> branches) {
        if (branches == null || branches.isEmpty()) {
            throw new IllegalArgumentException("branch operator requires at least one branch");
        }
        List<Map<String, Object>> normalised = new ArrayList<>(branches.size());
        for (int i = 0; i < branches.size(); i++) {
            BranchSpec b = branches.get(i);
            if (b == null || b.condition == null || b.condition.isBlank()) {
                throw new IllegalArgumentException("branch[" + i + "] requires a non-empty 'condition'");
            }
            if (b.topic == null || b.topic.isBlank()) {
                throw new IllegalArgumentException("branch[" + i + "] requires a non-empty 'topic'");
            }
            Map<String, Object> branchMap = new LinkedHashMap<>();
            branchMap.put("condition", b.condition);
            branchMap.put("topic", b.topic);
            normalised.add(branchMap);
        }
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "branch");
        op.put("branches", normalised);
        operators.add(op);
        return this;
    }

    /** Synchronous enrichment: join the stream against a state-store topic. */
    public StreamBuilder enrich(String lookupTopic, String keyField) {
        requireNonBlank("lookupTopic", lookupTopic);
        requireNonBlank("keyField", keyField);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "enrich");
        op.put("lookupTopic", lookupTopic);
        op.put("keyField", keyField);
        operators.add(op);
        return this;
    }

    /** Asynchronous HTTP enrichment. {@code url} supports {@code {field}} placeholders. */
    public StreamBuilder enrichAsync(EnrichAsyncOptions options) {
        Objects.requireNonNull(options, "options");
        requireNonBlank("url", options.url);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "enrichAsync");
        op.put("url", options.url);
        if (options.parallelism != null) op.put("parallelism", options.parallelism);
        if (options.queueSize != null) op.put("queueSize", options.queueSize);
        if (options.timeoutMs != null) op.put("timeoutMs", options.timeoutMs);
        if (options.maxRetries != null) op.put("maxRetries", options.maxRetries);
        if (options.retryBackoffMs != null) op.put("retryBackoffMs", options.retryBackoffMs);
        if (options.ordering != null) op.put("ordering", options.ordering);
        if (options.onFailure != null) op.put("onFailure", options.onFailure);
        operators.add(op);
        return this;
    }

    /** Complex Event Processing: match a sequence of conditions. */
    public StreamBuilder cep(List<Map<String, Object>> sequence) {
        return cep(sequence, new CepOptions());
    }

    /** Complex Event Processing: match a sequence of conditions. */
    public StreamBuilder cep(List<Map<String, Object>> sequence, CepOptions options) {
        if (sequence == null || sequence.isEmpty()) {
            throw new IllegalArgumentException("cep operator requires a non-empty sequence");
        }
        Objects.requireNonNull(options, "options");
        List<Map<String, Object>> copied = new ArrayList<>(sequence.size());
        for (Map<String, Object> step : sequence) {
            copied.add(new LinkedHashMap<>(step));
        }
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "cep");
        op.put("sequence", copied);
        if (options.within != null) op.put("within", options.within);
        if (options.name != null) op.put("name", options.name);
        operators.add(op);
        return this;
    }

    /**
     * B-109 — enrich each event with an LLM completion. {@code prompt}
     * supports {@code {field}} placeholders (and {@code {__payload__}})
     * substituted from the event server-side; the completion lands on the
     * event under {@code options.outputField}.
     */
    public StreamBuilder mapLlm(String prompt, MapLlmOptions options) {
        requireNonBlank("prompt", prompt);
        Objects.requireNonNull(options, "options");
        requireNonBlank("outputField", options.outputField);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "mapLlm");
        op.put("prompt", prompt);
        op.put("outputField", options.outputField);
        if (options.model != null) op.put("model", options.model);
        if (options.temperature != null) op.put("temperature", options.temperature);
        if (options.maxTokens != null) op.put("maxTokens", options.maxTokens);
        if (options.parallelism != null) op.put("parallelism", options.parallelism);
        if (options.ordering != null) op.put("ordering", options.ordering);
        if (options.onFailure != null) op.put("onFailure", options.onFailure);
        if (options.maxCallsPerSec != null) op.put("maxCallsPerSec", options.maxCallsPerSec);
        operators.add(op);
        return this;
    }

    /**
     * B-109 — LLM → typed structured fields merged into the event. The LLM is
     * asked for a JSON object keyed by {@code options.schema}'s fields;
     * missing / malformed fields become null server-side.
     */
    public StreamBuilder extract(ExtractOptions options) {
        Objects.requireNonNull(options, "options");
        requireNonBlank("instruction", options.instruction);
        if (options.schema == null || options.schema.isEmpty()) {
            throw new IllegalArgumentException("extract operator requires a non-empty schema");
        }
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "extract");
        op.put("instruction", options.instruction);
        op.put("schema", new LinkedHashMap<>(options.schema));
        if (options.model != null) op.put("model", options.model);
        if (options.temperature != null) op.put("temperature", options.temperature);
        if (options.maxTokens != null) op.put("maxTokens", options.maxTokens);
        if (options.onFailure != null) op.put("onFailure", options.onFailure);
        operators.add(op);
        return this;
    }

    /**
     * B-109 Phase 2 — invoke an MCP tool per event. {@code options.args}
     * string values support {@code {field}} substitution. On success the tool
     * output is written to {@code options.outputField} (omit for a
     * fire-and-forget side effect).
     */
    public StreamBuilder mcpCall(String tool, McpCallOptions options) {
        requireNonBlank("tool", tool);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "mcpCall");
        op.put("tool", tool);
        if (options != null) {
            if (options.args != null) op.put("args", new LinkedHashMap<>(options.args));
            if (options.outputField != null) op.put("outputField", options.outputField);
            if (options.parallelism != null) op.put("parallelism", options.parallelism);
            if (options.ordering != null) op.put("ordering", options.ordering);
            if (options.onFailure != null) op.put("onFailure", options.onFailure);
        }
        operators.add(op);
        return this;
    }

    /**
     * B-112 — score each event with an embedded ML model. Runs an uploaded
     * ONNX model in-process on the Pulse engine (no model-server hop). The
     * named {@code options.inputFields} are pulled from the event payload and
     * fed to the model; the model's output is written as a nested object under
     * {@code options.outputField} so downstream operators can branch on it
     * (e.g. {@code .filter("prediction.fraud_score > 0.8")}).
     *
     * <p>Upload the model first with {@link PulseClient.ModelsResource#upload}.
     */
    public StreamBuilder mlPredict(MlPredictOptions options) {
        Objects.requireNonNull(options, "options");
        requireNonBlank("model", options.model);
        requireNonBlank("outputField", options.outputField);
        if (options.inputFields == null || options.inputFields.isEmpty()) {
            throw new IllegalArgumentException(
                "mlPredict requires a non-empty list of inputFields");
        }
        for (String f : options.inputFields) {
            if (f == null || f.isBlank()) {
                throw new IllegalArgumentException(
                    "mlPredict inputFields must all be non-blank strings");
            }
        }
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "mlPredict");
        op.put("model", options.model);
        op.put("inputFields", new ArrayList<>(options.inputFields));
        op.put("outputField", options.outputField);
        if (options.parallelism != null) op.put("parallelism", options.parallelism);
        if (options.ordering != null) op.put("ordering", options.ordering);
        if (options.onFailure != null) op.put("onFailure", options.onFailure);
        operators.add(op);
        return this;
    }

    /** Broadcast join: enrich the stream against a fully-replicated table. */
    public StreamBuilder broadcastJoin(BroadcastJoinOptions options) {
        Objects.requireNonNull(options, "options");
        requireNonBlank("joinKeyField", options.joinKeyField);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "broadcastJoin");
        op.put("joinKeyField", options.joinKeyField);
        if (options.streamingTopic != null) op.put("streamingTopic", options.streamingTopic);
        if (options.name != null) op.put("name", options.name);
        if (options.maxBytes != null) op.put("maxBytes", options.maxBytes);
        if (options.refreshMode != null) op.put("refreshMode", options.refreshMode);
        if (options.intervalMillis != null) op.put("intervalMillis", options.intervalMillis);
        operators.add(op);
        return this;
    }

    /** CDC join: stream-table join against a CDC-fed state table. */
    public StreamBuilder cdcJoin(CdcJoinOptions options) {
        Objects.requireNonNull(options, "options");
        requireNonBlank("source", options.source);
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", "cdcJoin");
        op.put("source", options.source);
        if (options.joinKey != null) op.put("joinKey", options.joinKey);
        if (options.table != null) op.put("table", options.table);
        if (options.stateBackend != null) op.put("stateBackend", options.stateBackend);
        operators.add(op);
        return this;
    }

    // ------------------------------------------------------------------
    // Sink
    // ------------------------------------------------------------------

    /** Sets the output topic. No sink node is emitted (downstream consumers subscribe themselves). */
    public StreamBuilder toTopic(String topic) {
        return toTopic(topic, new ToTopicOptions());
    }

    /** Sets the output topic + optional sink node config. */
    public StreamBuilder toTopic(String topic, ToTopicOptions options) {
        requireNonBlank("topic", topic);
        Objects.requireNonNull(options, "options");
        this.outputTopic = topic;
        this.sinkChannel = options.sinkChannel;
        this.sinkConfig = new LinkedHashMap<>(options.sinkConfig != null ? options.sinkConfig : Map.of());
        this.sinkLabel = options.label;
        return this;
    }

    /**
     * Terminate the stream in a connector sink (Segment, Kafka, Postgres, …) —
     * an ergonomic, connector-first alias for {@code toTopic(topic, new
     * ToTopicOptions().sinkChannel(connectorType).sinkConfig(config))} using an
     * intermediate {@code <connectorType>-sink-out} topic. {@code connectorType}
     * is a subType from {@code client.connectors().list()}; bridged connectors
     * require the enterprise bridge JAR on the server.
     */
    public StreamBuilder toConnector(String connectorType, Map<String, Object> config) {
        requireNonBlank("connectorType", connectorType);
        return toTopic(connectorType + "-sink-out",
                new ToTopicOptions().sinkChannel(connectorType).sinkConfig(config));
    }

    /**
     * Terminate the stream in the agent's state store (queryable via B-106 IQ).
     * No sink node is emitted and no output topic is set.
     */
    public StreamBuilder toState() {
        this.outputTopic = null;
        this.sinkChannel = null;
        this.sinkConfig = new LinkedHashMap<>();
        this.sinkLabel = null;
        return this;
    }

    // ------------------------------------------------------------------
    // Metadata
    // ------------------------------------------------------------------

    /** Sets / overrides the pipeline name. */
    public StreamBuilder named(String name) {
        requireNonBlank("name", name);
        this.name = name;
        return this;
    }

    /** Sets the pipeline description. */
    public StreamBuilder describedAs(String description) {
        this.description = description;
        return this;
    }

    /** Sets the display label for the streaming agent node. */
    public StreamBuilder withAgentLabel(String label) {
        requireNonBlank("label", label);
        this.agentLabel = label;
        return this;
    }

    // ------------------------------------------------------------------
    // Compilation
    // ------------------------------------------------------------------

    /** Returns a read-only copy of the recorded operator chain. */
    public List<Map<String, Object>> operators() {
        List<Map<String, Object>> copy = new ArrayList<>(operators.size());
        for (Map<String, Object> op : operators) {
            copy.add(new LinkedHashMap<>(op));
        }
        return Collections.unmodifiableList(copy);
    }

    /** Compile the chain into a Pulse pipeline map ready for POST. Uses the preset name. */
    public Map<String, Object> build() {
        return build(null);
    }

    /**
     * Compile the chain into a Pulse pipeline map ready for POST.
     *
     * @param overrideName override the preset pipeline name; pass {@code null} to use the preset.
     * @throws IllegalStateException if required pieces are missing
     */
    public Map<String, Object> build(String overrideName) {
        String pipelineName = overrideName != null ? overrideName : this.name;
        if (pipelineName == null || pipelineName.isBlank()) {
            throw new IllegalStateException(
                "pipeline name required — pass to constructor or .build(name)");
        }
        if (inputTopic == null) {
            throw new IllegalStateException("no source — call .fromTopic(...) before .build()");
        }
        if (operators.isEmpty()) {
            throw new IllegalStateException(
                "no operators — chain at least one of .filter/.map/.keyBy/... before .build()");
        }

        List<Map<String, Object>> nodes = new ArrayList<>(3);

        // Source node
        Map<String, Object> srcConfig = new LinkedHashMap<>();
        srcConfig.put("engine", sourceEngine);
        srcConfig.put("inputTopic", inputTopic);
        srcConfig.putAll(sourceConfig);
        Map<String, Object> srcNode = new LinkedHashMap<>();
        srcNode.put("type", "source");
        srcNode.put("label", sourceLabel != null ? sourceLabel : sourceEngine + " source");
        srcNode.put("config", srcConfig);
        nodes.add(srcNode);

        // Agent node (streaming processor)
        Map<String, Object> agentConfig = new LinkedHashMap<>();
        agentConfig.put("engine", "streaming");
        agentConfig.put("inputTopic", inputTopic);
        List<Map<String, Object>> opCopy = new ArrayList<>(operators.size());
        for (Map<String, Object> op : operators) {
            opCopy.add(new LinkedHashMap<>(op));
        }
        agentConfig.put("operators", opCopy);
        if (outputTopic != null) agentConfig.put("outputTopic", outputTopic);
        Map<String, Object> agentNode = new LinkedHashMap<>();
        agentNode.put("type", "agent");
        agentNode.put("label", agentLabel != null ? agentLabel : pipelineName);
        agentNode.put("config", agentConfig);
        nodes.add(agentNode);

        // Sink node — only when both an output topic AND a sink channel are set
        if (outputTopic != null && sinkChannel != null) {
            Map<String, Object> sinkConf = new LinkedHashMap<>();
            sinkConf.put("channel", sinkChannel);
            sinkConf.put("inputTopic", outputTopic);
            sinkConf.putAll(sinkConfig);
            Map<String, Object> sinkNode = new LinkedHashMap<>();
            sinkNode.put("type", "sink");
            sinkNode.put("label", sinkLabel != null ? sinkLabel : sinkChannel + " sink");
            sinkNode.put("config", sinkConf);
            nodes.add(sinkNode);
        }

        Map<String, Object> pipeline = new LinkedHashMap<>();
        pipeline.put("name", pipelineName);
        pipeline.put("nodes", nodes);
        if (description != null) pipeline.put("description", description);
        return pipeline;
    }

    // ==================================================================
    // Nested public types — window specs, aggregators, option carriers
    // ==================================================================

    /** A window specification. Compiled to the string form the server expects. */
    public static final class WindowSpec {
        private final String spec;

        public WindowSpec(String spec) {
            if (spec == null || spec.isBlank()) {
                throw new IllegalArgumentException("WindowSpec requires a non-empty spec string");
            }
            this.spec = spec;
        }

        public String spec() { return spec; }

        @Override
        public String toString() { return spec; }

        @Override
        public boolean equals(Object o) {
            return o instanceof WindowSpec w && spec.equals(w.spec);
        }

        @Override
        public int hashCode() { return spec.hashCode(); }
    }

    /** Factory namespace for the 6 window kinds the server understands. */
    public static final class Windows {
        private Windows() {}

        /** Non-overlapping fixed windows: {@code tumbling("60s")}. */
        public static WindowSpec tumbling(String size) {
            requireNonBlank("size", size);
            return new WindowSpec("tumbling(" + size + ")");
        }

        /** Overlapping windows: {@code sliding("10m", "1m")}. */
        public static WindowSpec sliding(String size, String slide) {
            requireNonBlank("size", size);
            requireNonBlank("slide", slide);
            return new WindowSpec("sliding(" + size + "," + slide + ")");
        }

        /** Inactivity-bounded windows: {@code session("30s")}. */
        public static WindowSpec session(String timeout) {
            requireNonBlank("timeout", timeout);
            return new WindowSpec("session(" + timeout + ")");
        }

        /** Single unbounded window. */
        public static WindowSpec global() {
            return new WindowSpec("global");
        }

        /** Event-count tumbling: closes after {@code n} events. */
        public static WindowSpec count(int n) {
            if (n <= 0) {
                throw new IllegalArgumentException("count window size must be positive, got " + n);
            }
            return new WindowSpec("count(" + n + ")");
        }

        /** Event-count sliding: {@code countSliding(100, 10)} = window, slide. */
        public static WindowSpec countSliding(int size, int slide) {
            if (size <= 0 || slide <= 0) {
                throw new IllegalArgumentException(
                    "countSliding requires positive size and slide, got " + size + ", " + slide);
            }
            return new WindowSpec("count_sliding(" + size + "," + slide + ")");
        }
    }

    /** Factory namespace for the 7 aggregation functions the server understands. */
    public static final class Aggregators {
        private Aggregators() {}

        /** Event count — no field required. */
        public static String count() { return "count()"; }

        /** Sum of a numeric field. */
        public static String sum(String field) {
            requireNonBlank("field", field);
            return "sum(" + field + ")";
        }

        /** Average of a numeric field. */
        public static String avg(String field) {
            requireNonBlank("field", field);
            return "avg(" + field + ")";
        }

        /** Minimum value of a numeric field. */
        public static String min(String field) {
            requireNonBlank("field", field);
            return "min(" + field + ")";
        }

        /** Maximum value of a numeric field. */
        public static String max(String field) {
            requireNonBlank("field", field);
            return "max(" + field + ")";
        }

        /** Collect every value of {@code field} into a list. */
        public static String collectList(String field) {
            requireNonBlank("field", field);
            return "collect_list(" + field + ")";
        }

        /** Cardinality of distinct values of {@code field}. */
        public static String distinctCount(String field) {
            requireNonBlank("field", field);
            return "distinct_count(" + field + ")";
        }
    }

    /** Options for {@link #fromTopic(String, FromTopicOptions)}. Mutable fluent setter. */
    public static final class FromTopicOptions {
        String sourceEngine;
        Map<String, Object> sourceConfig;
        String label;

        public FromTopicOptions sourceEngine(String sourceEngine) {
            this.sourceEngine = sourceEngine;
            return this;
        }

        public FromTopicOptions sourceConfig(Map<String, Object> sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        public FromTopicOptions label(String label) {
            this.label = label;
            return this;
        }
    }

    /** Options for {@link #toTopic(String, ToTopicOptions)}. */
    public static final class ToTopicOptions {
        String sinkChannel;
        Map<String, Object> sinkConfig;
        String label;

        public ToTopicOptions sinkChannel(String sinkChannel) {
            this.sinkChannel = sinkChannel;
            return this;
        }

        public ToTopicOptions sinkConfig(Map<String, Object> sinkConfig) {
            this.sinkConfig = sinkConfig;
            return this;
        }

        public ToTopicOptions label(String label) {
            this.label = label;
            return this;
        }
    }

    /** Options for {@link #map(MapOptions)}. At least one of fields / targetType is required. */
    public static final class MapOptions {
        Map<String, String> fields;
        String targetType;

        public MapOptions fields(Map<String, String> fields) {
            this.fields = fields;
            return this;
        }

        public MapOptions targetType(String targetType) {
            this.targetType = targetType;
            return this;
        }
    }

    /** Options for {@link #window(WindowSpec, WindowOptions)}. */
    public static final class WindowOptions {
        Map<String, String> aggregations;
        String outputTopic;
        Object trigger;

        public WindowOptions aggregations(Map<String, String> aggregations) {
            this.aggregations = aggregations;
            return this;
        }

        public WindowOptions outputTopic(String outputTopic) {
            this.outputTopic = outputTopic;
            return this;
        }

        public WindowOptions trigger(Object trigger) {
            this.trigger = trigger;
            return this;
        }
    }

    /** One branch of {@link #branch(List)}. */
    public static final class BranchSpec {
        public final String condition;
        public final String topic;

        public BranchSpec(String condition, String topic) {
            this.condition = condition;
            this.topic = topic;
        }
    }

    /** Options for {@link #enrichAsync(EnrichAsyncOptions)}. */
    public static final class EnrichAsyncOptions {
        String url;
        Integer parallelism;
        Integer queueSize;
        Integer timeoutMs;
        Integer maxRetries;
        Integer retryBackoffMs;
        String ordering;
        String onFailure;

        public EnrichAsyncOptions url(String url) { this.url = url; return this; }
        public EnrichAsyncOptions parallelism(int n) { this.parallelism = n; return this; }
        public EnrichAsyncOptions queueSize(int n) { this.queueSize = n; return this; }
        public EnrichAsyncOptions timeoutMs(int n) { this.timeoutMs = n; return this; }
        public EnrichAsyncOptions maxRetries(int n) { this.maxRetries = n; return this; }
        public EnrichAsyncOptions retryBackoffMs(int n) { this.retryBackoffMs = n; return this; }

        /** Must be {@code "PRESERVE_INPUT"} or {@code "UNORDERED"}. */
        public EnrichAsyncOptions ordering(String ordering) {
            if (ordering != null && !ordering.equals("PRESERVE_INPUT") && !ordering.equals("UNORDERED")) {
                throw new IllegalArgumentException(
                    "ordering must be PRESERVE_INPUT or UNORDERED, got '" + ordering + "'");
            }
            this.ordering = ordering;
            return this;
        }

        /** Must be {@code "EMIT_ERROR"}, {@code "DROP"} or {@code "PASS_THROUGH"}. */
        public EnrichAsyncOptions onFailure(String onFailure) {
            if (onFailure != null
                && !onFailure.equals("EMIT_ERROR")
                && !onFailure.equals("DROP")
                && !onFailure.equals("PASS_THROUGH")) {
                throw new IllegalArgumentException(
                    "onFailure must be EMIT_ERROR, DROP or PASS_THROUGH, got '" + onFailure + "'");
            }
            this.onFailure = onFailure;
            return this;
        }
    }

    /** Options for {@link #cep(List, CepOptions)}. */
    public static final class CepOptions {
        String within;
        String name;

        public CepOptions within(String within) { this.within = within; return this; }
        public CepOptions name(String name) { this.name = name; return this; }
    }

    /** B-109 — options for {@link #mapLlm(String, MapLlmOptions)}. */
    public static final class MapLlmOptions {
        String outputField;
        String model;
        Double temperature;
        Integer maxTokens;
        Integer parallelism;
        String ordering;
        String onFailure;
        Integer maxCallsPerSec;

        public MapLlmOptions outputField(String f) { this.outputField = f; return this; }
        public MapLlmOptions model(String m) { this.model = m; return this; }
        public MapLlmOptions temperature(double t) { this.temperature = t; return this; }
        public MapLlmOptions maxTokens(int n) { this.maxTokens = n; return this; }
        public MapLlmOptions parallelism(int n) { this.parallelism = n; return this; }

        /** Must be {@code "PRESERVE_INPUT"} or {@code "UNORDERED"}. */
        public MapLlmOptions ordering(String o) {
            if (o != null && !o.equals("PRESERVE_INPUT") && !o.equals("UNORDERED")) {
                throw new IllegalArgumentException(
                    "ordering must be PRESERVE_INPUT or UNORDERED, got '" + o + "'");
            }
            this.ordering = o; return this;
        }

        /** Must be {@code "EMIT_ERROR"}, {@code "DROP"} or {@code "PASS_THROUGH"}. */
        public MapLlmOptions onFailure(String f) {
            this.onFailure = checkFailure(f); return this;
        }

        public MapLlmOptions maxCallsPerSec(int n) { this.maxCallsPerSec = n; return this; }
    }

    /** B-109 — options for {@link #extract(ExtractOptions)}. */
    public static final class ExtractOptions {
        String instruction;
        Map<String, String> schema;
        String model;
        Double temperature;
        Integer maxTokens;
        String onFailure;

        public ExtractOptions instruction(String i) { this.instruction = i; return this; }
        public ExtractOptions schema(Map<String, String> s) { this.schema = s; return this; }
        public ExtractOptions model(String m) { this.model = m; return this; }
        public ExtractOptions temperature(double t) { this.temperature = t; return this; }
        public ExtractOptions maxTokens(int n) { this.maxTokens = n; return this; }
        public ExtractOptions onFailure(String f) { this.onFailure = checkFailure(f); return this; }
    }

    /** B-109 Phase 2 — options for {@link #mcpCall(String, McpCallOptions)}. */
    public static final class McpCallOptions {
        Map<String, Object> args;
        String outputField;
        Integer parallelism;
        String ordering;
        String onFailure;

        public McpCallOptions args(Map<String, Object> a) { this.args = a; return this; }
        public McpCallOptions outputField(String f) { this.outputField = f; return this; }
        public McpCallOptions parallelism(int n) { this.parallelism = n; return this; }

        public McpCallOptions ordering(String o) {
            if (o != null && !o.equals("PRESERVE_INPUT") && !o.equals("UNORDERED")) {
                throw new IllegalArgumentException(
                    "ordering must be PRESERVE_INPUT or UNORDERED, got '" + o + "'");
            }
            this.ordering = o; return this;
        }

        public McpCallOptions onFailure(String f) { this.onFailure = checkFailure(f); return this; }
    }

    /** B-112 — options for {@link #mlPredict(MlPredictOptions)}. */
    public static final class MlPredictOptions {
        String model;
        List<String> inputFields;
        String outputField;
        Integer parallelism;
        String ordering;
        String onFailure;

        public MlPredictOptions model(String m) { this.model = m; return this; }
        public MlPredictOptions inputFields(List<String> f) { this.inputFields = f; return this; }
        public MlPredictOptions outputField(String f) { this.outputField = f; return this; }
        public MlPredictOptions parallelism(int n) { this.parallelism = n; return this; }

        /** Must be {@code "PRESERVE_INPUT"} or {@code "UNORDERED"}. */
        public MlPredictOptions ordering(String o) {
            if (o != null && !o.equals("PRESERVE_INPUT") && !o.equals("UNORDERED")) {
                throw new IllegalArgumentException(
                    "ordering must be PRESERVE_INPUT or UNORDERED, got '" + o + "'");
            }
            this.ordering = o; return this;
        }

        /** Must be {@code "EMIT_ERROR"}, {@code "DROP"} or {@code "PASS_THROUGH"}. */
        public MlPredictOptions onFailure(String f) { this.onFailure = checkFailure(f); return this; }
    }

    private static String checkFailure(String f) {
        if (f != null && !f.equals("EMIT_ERROR") && !f.equals("DROP") && !f.equals("PASS_THROUGH")) {
            throw new IllegalArgumentException(
                "onFailure must be EMIT_ERROR, DROP or PASS_THROUGH, got '" + f + "'");
        }
        return f;
    }

    /** Options for {@link #broadcastJoin(BroadcastJoinOptions)}. */
    public static final class BroadcastJoinOptions {
        String joinKeyField;
        String streamingTopic;
        String name;
        Long maxBytes;
        String refreshMode;
        Integer intervalMillis;

        public BroadcastJoinOptions joinKeyField(String f) { this.joinKeyField = f; return this; }
        public BroadcastJoinOptions streamingTopic(String t) { this.streamingTopic = t; return this; }
        public BroadcastJoinOptions name(String n) { this.name = n; return this; }
        public BroadcastJoinOptions maxBytes(long n) { this.maxBytes = n; return this; }

        /** Must be {@code "cdc"}, {@code "periodic"} or {@code "explicit"}. */
        public BroadcastJoinOptions refreshMode(String mode) {
            if (mode != null && !mode.equals("cdc") && !mode.equals("periodic") && !mode.equals("explicit")) {
                throw new IllegalArgumentException(
                    "refreshMode must be cdc, periodic or explicit, got '" + mode + "'");
            }
            this.refreshMode = mode;
            return this;
        }

        public BroadcastJoinOptions intervalMillis(int n) { this.intervalMillis = n; return this; }
    }

    /** Options for {@link #cdcJoin(CdcJoinOptions)}. */
    public static final class CdcJoinOptions {
        String source;
        String joinKey;
        String table;
        String stateBackend;

        public CdcJoinOptions source(String s) { this.source = s; return this; }
        public CdcJoinOptions joinKey(String k) { this.joinKey = k; return this; }
        public CdcJoinOptions table(String t) { this.table = t; return this; }
        public CdcJoinOptions stateBackend(String b) { this.stateBackend = b; return this; }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private static void requireNonBlank(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                name + " must be a non-empty string, got " + (value == null ? "null" : "'" + value + "'"));
        }
    }
}
