package com.streamflow.pulse.client;

import com.streamflow.pulse.client.StreamBuilder.Aggregators;
import com.streamflow.pulse.client.StreamBuilder.BranchSpec;
import com.streamflow.pulse.client.StreamBuilder.BroadcastJoinOptions;
import com.streamflow.pulse.client.StreamBuilder.CdcJoinOptions;
import com.streamflow.pulse.client.StreamBuilder.CepOptions;
import com.streamflow.pulse.client.StreamBuilder.EnrichAsyncOptions;
import com.streamflow.pulse.client.StreamBuilder.ExtractOptions;
import com.streamflow.pulse.client.StreamBuilder.FromTopicOptions;
import com.streamflow.pulse.client.StreamBuilder.MapLlmOptions;
import com.streamflow.pulse.client.StreamBuilder.MapOptions;
import com.streamflow.pulse.client.StreamBuilder.McpCallOptions;
import com.streamflow.pulse.client.StreamBuilder.ToTopicOptions;
import com.streamflow.pulse.client.StreamBuilder.WindowOptions;
import com.streamflow.pulse.client.StreamBuilder.WindowSpec;
import com.streamflow.pulse.client.StreamBuilder.Windows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link StreamBuilder}, {@link Windows}, {@link Aggregators}.
 *
 * <p>Pure DSL tests (no HTTP). The HTTP integration of {@code client.streams()}
 * lives in {@code PulseClientTest.Streams}.
 *
 * <p>Coverage strategy mirrors pulse-py / pulse-js: every operator is exercised
 * at constructor-validation and emitted-shape layers, plus one round-trip test
 * that rebuilds the canonical {@code iot-temperature-aggregator} template
 * field-for-field.
 */
class StreamBuilderTest {

    @Nested
    @DisplayName("Windows factory")
    class WindowsFactory {

        @Test
        void tumblingEmitsExpectedString() {
            assertThat(Windows.tumbling("60s").spec()).isEqualTo("tumbling(60s)");
        }

        @Test
        void slidingEmitsExpectedString() {
            assertThat(Windows.sliding("10m", "1m").spec()).isEqualTo("sliding(10m,1m)");
        }

        @Test
        void sessionEmitsExpectedString() {
            assertThat(Windows.session("30s").spec()).isEqualTo("session(30s)");
        }

        @Test
        void globalEmitsExpectedString() {
            assertThat(Windows.global().spec()).isEqualTo("global");
        }

        @Test
        void countEmitsExpectedString() {
            assertThat(Windows.count(100).spec()).isEqualTo("count(100)");
        }

        @Test
        void countSlidingEmitsExpectedString() {
            assertThat(Windows.countSliding(100, 10).spec()).isEqualTo("count_sliding(100,10)");
        }

        @Test
        void tumblingRejectsBlank() {
            assertThatThrownBy(() -> Windows.tumbling("")).hasMessageContaining("size");
        }

        @Test
        void slidingRejectsBlankEitherArg() {
            assertThatThrownBy(() -> Windows.sliding("10m", "")).hasMessageContaining("slide");
            assertThatThrownBy(() -> Windows.sliding("   ", "1m")).hasMessageContaining("size");
        }

        @Test
        void countRejectsNonPositive() {
            assertThatThrownBy(() -> Windows.count(0)).hasMessageContaining("positive");
            assertThatThrownBy(() -> Windows.count(-5)).hasMessageContaining("positive");
        }

        @Test
        void countSlidingRejectsNonPositive() {
            assertThatThrownBy(() -> Windows.countSliding(100, 0)).hasMessageContaining("positive");
            assertThatThrownBy(() -> Windows.countSliding(0, 10)).hasMessageContaining("positive");
        }

        @Test
        void windowSpecToStringReturnsSpec() {
            assertThat(Windows.tumbling("60s").toString()).isEqualTo("tumbling(60s)");
        }

        @Test
        void windowSpecRejectsBlank() {
            assertThatThrownBy(() -> new WindowSpec("")).hasMessageContaining("non-empty");
            assertThatThrownBy(() -> new WindowSpec("   ")).hasMessageContaining("non-empty");
        }

        @Test
        void windowSpecEqAndHash() {
            WindowSpec a = Windows.tumbling("60s");
            WindowSpec b = Windows.tumbling("60s");
            WindowSpec c = Windows.tumbling("30s");
            assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(c).isNotEqualTo("tumbling(60s)");
        }
    }

    @Nested
    @DisplayName("Aggregators factory")
    class AggregatorsFactory {

        @Test
        void countTakesNoField() {
            assertThat(Aggregators.count()).isEqualTo("count()");
        }

        @Test
        void allEmitExpectedStrings() {
            assertThat(Aggregators.sum("amount")).isEqualTo("sum(amount)");
            assertThat(Aggregators.avg("price")).isEqualTo("avg(price)");
            assertThat(Aggregators.min("latency")).isEqualTo("min(latency)");
            assertThat(Aggregators.max("latency")).isEqualTo("max(latency)");
            assertThat(Aggregators.collectList("sku")).isEqualTo("collect_list(sku)");
            assertThat(Aggregators.distinctCount("user_id")).isEqualTo("distinct_count(user_id)");
        }

        @Test
        void aggregatorsRejectBlankField() {
            assertThatThrownBy(() -> Aggregators.sum("")).hasMessageContaining("field");
            assertThatThrownBy(() -> Aggregators.avg("   ")).hasMessageContaining("field");
            assertThatThrownBy(() -> Aggregators.min("")).hasMessageContaining("field");
            assertThatThrownBy(() -> Aggregators.max("")).hasMessageContaining("field");
            assertThatThrownBy(() -> Aggregators.collectList("")).hasMessageContaining("field");
            assertThatThrownBy(() -> Aggregators.distinctCount("")).hasMessageContaining("field");
        }
    }

    @Nested
    @DisplayName("operators emit validator-accepted shape")
    class Operators {

        @Test
        void filter() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").filter("amount > 1000");
            assertThat(b.operators()).containsExactly(
                Map.of("type", "filter", "condition", "amount > 1000"));
        }

        @Test
        void filterRejectsBlank() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").filter(""))
                .hasMessageContaining("condition");
        }

        @Test
        void mapWithFieldsOnly() {
            Map<String, String> fields = Map.of("alert", "concat(id, '!')");
            StreamBuilder b = new StreamBuilder().fromTopic("in")
                .map(new MapOptions().fields(fields));
            assertThat(b.operators()).containsExactly(
                Map.of("type", "map", "fields", fields));
        }

        @Test
        void mapWithTargetTypeOnly() {
            StreamBuilder b = new StreamBuilder().fromTopic("in")
                .map(new MapOptions().targetType("alert"));
            assertThat(b.operators()).containsExactly(
                Map.of("type", "map", "targetType", "alert"));
        }

        @Test
        void mapRejectsEmpty() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").map(new MapOptions()))
                .hasMessageContaining("does nothing");
        }

        @Test
        void flatMap() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").flatMap("items");
            assertThat(b.operators()).containsExactly(
                Map.of("type", "flatMap", "splitField", "items"));
        }

        @Test
        void flatMapRejectsBlank() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").flatMap(""))
                .hasMessageContaining("splitField");
        }

        @Test
        void keyBy() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").keyBy("deviceId");
            assertThat(b.operators()).containsExactly(
                Map.of("type", "keyBy", "field", "deviceId"));
        }

        @Test
        void keyByRejectsBlank() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").keyBy(""))
                .hasMessageContaining("field");
        }

        @Test
        void windowWithAggregations() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").window(
                Windows.tumbling("60s"),
                new WindowOptions().aggregations(Map.of("avgTemp", Aggregators.avg("temperature"))));
            assertThat(b.operators()).containsExactly(Map.of(
                "type", "window",
                "spec", "tumbling(60s)",
                "aggregations", Map.of("avgTemp", "avg(temperature)")));
        }

        @Test
        void windowAcceptsRawStringSpec() {
            StreamBuilder b = new StreamBuilder().fromTopic("in")
                .window("sliding(10m,1m)", new WindowOptions());
            assertThat(b.operators()).containsExactly(
                Map.of("type", "window", "spec", "sliding(10m,1m)"));
        }

        @Test
        void windowWithOutputTopicAndTrigger() {
            Map<String, Object> trigger = Map.of("kind", "earlyFire", "afterEvents", 10);
            StreamBuilder b = new StreamBuilder().fromTopic("in").window(
                Windows.tumbling("60s"),
                new WindowOptions().outputTopic("late-data").trigger(trigger));
            Map<String, Object> op = b.operators().get(0);
            assertThat(op.get("outputTopic")).isEqualTo("late-data");
            assertThat(op.get("trigger")).isEqualTo(trigger);
        }

        @Test
        void branchEmitsValidatorShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").branch(List.of(
                new BranchSpec("tier == 'gold'", "vip-events"),
                new BranchSpec("tier == 'silver'", "std-events")));
            List<Map<String, Object>> ops = b.operators();
            assertThat(ops).hasSize(1);
            assertThat(ops.get(0).get("type")).isEqualTo("branch");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> branches = (List<Map<String, Object>>) ops.get(0).get("branches");
            assertThat(branches).containsExactly(
                Map.of("condition", "tier == 'gold'", "topic", "vip-events"),
                Map.of("condition", "tier == 'silver'", "topic", "std-events"));
        }

        @Test
        void branchRejectsEmpty() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").branch(List.of()))
                .hasMessageContaining("at least one");
        }

        @Test
        void branchRejectsMissingFields() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in")
                .branch(List.of(new BranchSpec(null, "x"))))
                .hasMessageContaining("condition");
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in")
                .branch(List.of(new BranchSpec("x > 0", ""))))
                .hasMessageContaining("topic");
        }

        @Test
        void enrich() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").enrich("customers", "customerId");
            assertThat(b.operators()).containsExactly(Map.of(
                "type", "enrich",
                "lookupTopic", "customers",
                "keyField", "customerId"));
        }

        @Test
        void enrichRejectsBlankArgs() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").enrich("", "k"))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").enrich("t", ""))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void enrichAsyncFullShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").enrichAsync(new EnrichAsyncOptions()
                .url("https://x.example/lookup/{id}")
                .parallelism(8)
                .queueSize(128)
                .timeoutMs(5000)
                .maxRetries(3)
                .retryBackoffMs(200)
                .ordering("PRESERVE_INPUT")
                .onFailure("EMIT_ERROR"));
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("type", "enrichAsync");
            expected.put("url", "https://x.example/lookup/{id}");
            expected.put("parallelism", 8);
            expected.put("queueSize", 128);
            expected.put("timeoutMs", 5000);
            expected.put("maxRetries", 3);
            expected.put("retryBackoffMs", 200);
            expected.put("ordering", "PRESERVE_INPUT");
            expected.put("onFailure", "EMIT_ERROR");
            assertThat(b.operators()).containsExactly(expected);
        }

        @Test
        void enrichAsyncRejectsBadOrdering() {
            assertThatThrownBy(() -> new EnrichAsyncOptions().ordering("SHUFFLED"))
                .hasMessageContaining("ordering");
        }

        @Test
        void enrichAsyncRejectsBadOnFailure() {
            assertThatThrownBy(() -> new EnrichAsyncOptions().onFailure("EXPLODE"))
                .hasMessageContaining("onFailure");
        }

        @Test
        void cepEmitsValidatorShape() {
            Map<String, Object> step1 = new LinkedHashMap<>();
            step1.put("name", "add");
            step1.put("match", "type == 'ADD_TO_CART'");
            step1.put("within", "10m");
            Map<String, Object> step2 = new LinkedHashMap<>();
            step2.put("name", "view");
            step2.put("match", "type == 'VIEW_CART'");
            step2.put("follow", "followedBy");
            StreamBuilder b = new StreamBuilder().fromTopic("in").cep(
                List.of(step1, step2),
                new CepOptions().within("20m").name("cart-flow"));
            Map<String, Object> op = b.operators().get(0);
            assertThat(op.get("type")).isEqualTo("cep");
            assertThat(op.get("within")).isEqualTo("20m");
            assertThat(op.get("name")).isEqualTo("cart-flow");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> seq = (List<Map<String, Object>>) op.get("sequence");
            assertThat(seq).containsExactly(step1, step2);
        }

        @Test
        void cepRejectsEmptySequence() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").cep(List.of()))
                .hasMessageContaining("non-empty sequence");
        }

        @Test
        void mapLlmFullShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").mapLlm(
                "Summarise: {body}",
                new MapLlmOptions().outputField("summary").model("gemma3:7b")
                    .temperature(0.0).maxTokens(64).parallelism(8)
                    .ordering("UNORDERED").onFailure("PASS_THROUGH").maxCallsPerSec(50));
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("type", "mapLlm");
            expected.put("prompt", "Summarise: {body}");
            expected.put("outputField", "summary");
            expected.put("model", "gemma3:7b");
            expected.put("temperature", 0.0);
            expected.put("maxTokens", 64);
            expected.put("parallelism", 8);
            expected.put("ordering", "UNORDERED");
            expected.put("onFailure", "PASS_THROUGH");
            expected.put("maxCallsPerSec", 50);
            assertThat(b.operators()).containsExactly(expected);
        }

        @Test
        void mapLlmRejectsBlankPromptOrOutputField() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in")
                .mapLlm("", new MapLlmOptions().outputField("x")))
                .hasMessageContaining("prompt");
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in")
                .mapLlm("p", new MapLlmOptions().outputField("")))
                .hasMessageContaining("outputField");
        }

        @Test
        void mapLlmRejectsBadOrdering() {
            assertThatThrownBy(() -> new MapLlmOptions().ordering("SHUFFLED"))
                .hasMessageContaining("ordering");
        }

        @Test
        void extractFullShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").extract(
                new ExtractOptions()
                    .instruction("Extract intent and urgency")
                    .schema(Map.of("intent", "string", "urgency", "int"))
                    .model("gemma3:7b").temperature(0.0));
            Map<String, Object> op = b.operators().get(0);
            assertThat(op.get("type")).isEqualTo("extract");
            assertThat(op.get("instruction")).isEqualTo("Extract intent and urgency");
            assertThat(op.get("schema")).isEqualTo(Map.of("intent", "string", "urgency", "int"));
            assertThat(op.get("model")).isEqualTo("gemma3:7b");
        }

        @Test
        void extractRejectsEmptySchema() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in")
                .extract(new ExtractOptions().instruction("x").schema(Map.of())))
                .hasMessageContaining("schema");
        }

        @Test
        void mcpCallFullShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").mcpCall(
                "crm.lookup_customer",
                new McpCallOptions()
                    .args(Map.of("customer_id", "{customerId}"))
                    .outputField("customer").parallelism(4)
                    .ordering("UNORDERED").onFailure("EMIT_ERROR"));
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("type", "mcpCall");
            expected.put("tool", "crm.lookup_customer");
            expected.put("args", Map.of("customer_id", "{customerId}"));
            expected.put("outputField", "customer");
            expected.put("parallelism", 4);
            expected.put("ordering", "UNORDERED");
            expected.put("onFailure", "EMIT_ERROR");
            assertThat(b.operators()).containsExactly(expected);
        }

        @Test
        void mcpCallMinimalFireAndForget() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").mcpCall("pagerduty.create_incident", null);
            assertThat(b.operators()).containsExactly(
                Map.of("type", "mcpCall", "tool", "pagerduty.create_incident"));
        }

        @Test
        void mcpCallRejectsBlankTool() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").mcpCall("", null))
                .hasMessageContaining("tool");
        }

        @Test
        void broadcastJoinFullShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").broadcastJoin(new BroadcastJoinOptions()
                .joinKeyField("userId")
                .streamingTopic("users-table")
                .name("users-join")
                .maxBytes(10_000_000L)
                .refreshMode("cdc")
                .intervalMillis(30_000));
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("type", "broadcastJoin");
            expected.put("joinKeyField", "userId");
            expected.put("streamingTopic", "users-table");
            expected.put("name", "users-join");
            expected.put("maxBytes", 10_000_000L);
            expected.put("refreshMode", "cdc");
            expected.put("intervalMillis", 30_000);
            assertThat(b.operators()).containsExactly(expected);
        }

        @Test
        void broadcastJoinRejectsBadRefreshMode() {
            assertThatThrownBy(() -> new BroadcastJoinOptions().refreshMode("random"))
                .hasMessageContaining("refreshMode");
        }

        @Test
        void cdcJoinFullShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").cdcJoin(new CdcJoinOptions()
                .source("postgres://orders")
                .joinKey("orderId")
                .table("orders")
                .stateBackend("rocksdb"));
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("type", "cdcJoin");
            expected.put("source", "postgres://orders");
            expected.put("joinKey", "orderId");
            expected.put("table", "orders");
            expected.put("stateBackend", "rocksdb");
            assertThat(b.operators()).containsExactly(expected);
        }

        @Test
        void cdcJoinMinimalShape() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").cdcJoin(
                new CdcJoinOptions().source("postgres://orders"));
            assertThat(b.operators()).containsExactly(
                Map.of("type", "cdcJoin", "source", "postgres://orders"));
        }
    }

    @Nested
    @DisplayName("full pipeline build")
    class Build {

        @Test
        void minimalPipelineBuilds() {
            Map<String, Object> out = new StreamBuilder("p1")
                .fromTopic("in")
                .filter("x > 0")
                .build();
            assertThat(out.get("name")).isEqualTo("p1");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            assertThat(nodes).hasSize(2);
            assertThat(nodes.get(0).get("type")).isEqualTo("source");
            @SuppressWarnings("unchecked")
            Map<String, Object> srcConfig = (Map<String, Object>) nodes.get(0).get("config");
            assertThat(srcConfig.get("engine")).isEqualTo("kafka");
            assertThat(srcConfig.get("inputTopic")).isEqualTo("in");
            assertThat(nodes.get(1).get("type")).isEqualTo("agent");
            @SuppressWarnings("unchecked")
            Map<String, Object> agentConfig = (Map<String, Object>) nodes.get(1).get("config");
            assertThat(agentConfig.get("engine")).isEqualTo("streaming");
            assertThat(agentConfig.get("operators")).isEqualTo(List.of(
                Map.of("type", "filter", "condition", "x > 0")));
        }

        @Test
        void nameCanBeSetViaNamed() {
            Map<String, Object> out = new StreamBuilder().named("p2")
                .fromTopic("in").filter("x > 0").build();
            assertThat(out.get("name")).isEqualTo("p2");
        }

        @Test
        void overrideNameWinsOverConstructor() {
            Map<String, Object> out = new StreamBuilder("ignored")
                .fromTopic("in").filter("x > 0").build("actual");
            assertThat(out.get("name")).isEqualTo("actual");
        }

        @Test
        void descriptionPropagates() {
            Map<String, Object> out = new StreamBuilder("p3").describedAs("desc")
                .fromTopic("in").filter("x > 0").build();
            assertThat(out.get("description")).isEqualTo("desc");
        }

        @Test
        void agentLabelSetter() {
            Map<String, Object> out = new StreamBuilder("p4")
                .withAgentLabel("Per-Device Average")
                .fromTopic("in").filter("x > 0").build();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            assertThat(nodes.get(1).get("label")).isEqualTo("Per-Device Average");
        }

        @Test
        void emitsSinkWhenToTopicHasChannel() {
            Map<String, Object> out = new StreamBuilder("p5")
                .fromTopic("in")
                .filter("x > 0")
                .toTopic("out", new ToTopicOptions().sinkChannel("email"))
                .build();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            assertThat(nodes).hasSize(3);
            assertThat(nodes.get(2)).isEqualTo(Map.of(
                "type", "sink",
                "label", "email sink",
                "config", Map.of("channel", "email", "inputTopic", "out")));
        }

        @Test
        void skipsSinkWhenToTopicHasNoChannel() {
            Map<String, Object> out = new StreamBuilder("p6")
                .fromTopic("in").filter("x > 0").toTopic("out").build();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            assertThat(nodes).hasSize(2);
            @SuppressWarnings("unchecked")
            Map<String, Object> agentConfig = (Map<String, Object>) nodes.get(1).get("config");
            assertThat(agentConfig.get("outputTopic")).isEqualTo("out");
        }

        @Test
        void toStateClearsOutputAndSink() {
            Map<String, Object> out = new StreamBuilder("p7")
                .fromTopic("in")
                .filter("x > 0")
                .toTopic("out", new ToTopicOptions().sinkChannel("email"))
                .toState()
                .build();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            assertThat(nodes).hasSize(2);
            @SuppressWarnings("unchecked")
            Map<String, Object> agentConfig = (Map<String, Object>) nodes.get(1).get("config");
            assertThat(agentConfig.containsKey("outputTopic")).isFalse();
        }

        @Test
        void sourceEngineAndLabelPropagate() {
            Map<String, Object> out = new StreamBuilder("p8")
                .fromTopic("in", new FromTopicOptions()
                    .sourceEngine("mqtt")
                    .sourceConfig(Map.of("qos", 1))
                    .label("Sensor readings"))
                .filter("x > 0")
                .build();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            Map<String, Object> src = nodes.get(0);
            assertThat(src.get("label")).isEqualTo("Sensor readings");
            assertThat(src.get("config")).isEqualTo(
                Map.of("engine", "mqtt", "inputTopic", "in", "qos", 1));
        }

        @Test
        void rejectsMissingName() {
            assertThatThrownBy(() -> new StreamBuilder().fromTopic("in").filter("x > 0").build())
                .hasMessageContaining("name");
        }

        @Test
        void rejectsMissingSource() {
            assertThatThrownBy(() -> new StreamBuilder("p").filter("x > 0").build())
                .hasMessageContaining("source");
        }

        @Test
        void rejectsEmptyOperatorChain() {
            assertThatThrownBy(() -> new StreamBuilder("p").fromTopic("in").build())
                .hasMessageContaining("operators");
        }

        @Test
        void constructorRejectsBlankName() {
            assertThatThrownBy(() -> new StreamBuilder("")).hasMessageContaining("name");
        }

        @Test
        void operatorsReturnsImmutableCopy() {
            StreamBuilder b = new StreamBuilder().fromTopic("in").filter("x > 0");
            List<Map<String, Object>> snapshot = b.operators();
            assertThatThrownBy(() -> snapshot.add(Map.of("type", "tampered")))
                .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void chainOrderingPreserved() {
            Map<String, Object> out = new StreamBuilder("p9")
                .fromTopic("in")
                .filter("a > 0")
                .keyBy("k")
                .window(Windows.tumbling("60s"),
                    new WindowOptions().aggregations(Map.of("cnt", Aggregators.count())))
                .filter("cnt > 5")
                .map(new MapOptions().fields(Map.of("out", "cnt")))
                .build();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            @SuppressWarnings("unchecked")
            Map<String, Object> ac = (Map<String, Object>) nodes.get(1).get("config");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> ops = (List<Map<String, Object>>) ac.get("operators");
            assertThat(ops).extracting(op -> op.get("type"))
                .containsExactly("filter", "keyBy", "window", "filter", "map");
        }

        @Test
        void iotTemplateRoundTrip() {
            // Rebuild the canonical iot-temperature-aggregator template via the
            // DSL and assert it matches the hand-authored JSON shape shipped in
            // streamflow-pulse/src/main/resources/pipeline-templates/.
            Map<String, Object> out = new StreamBuilder("iot-temperature-aggregator")
                .withAgentLabel("Per-Device Average")
                .fromTopic("sensor-readings", new FromTopicOptions()
                    .sourceEngine("mqtt")
                    .label("Sensor readings"))
                .keyBy("deviceId")
                .window(Windows.tumbling("60s"),
                    new WindowOptions().aggregations(
                        Map.of("avgTemp", Aggregators.avg("temperature"))))
                .filter("avgTemp > 75")
                .toTopic("sensor-minute-averages", new ToTopicOptions()
                    .sinkChannel("email")
                    .label("Heat Alert"))
                .build();

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
            assertThat(nodes).extracting(n -> n.get("type"))
                .containsExactly("source", "agent", "sink");

            Map<String, Object> src = nodes.get(0);
            assertThat(src.get("label")).isEqualTo("Sensor readings");
            assertThat(src.get("config")).isEqualTo(
                Map.of("engine", "mqtt", "inputTopic", "sensor-readings"));

            Map<String, Object> agent = nodes.get(1);
            assertThat(agent.get("label")).isEqualTo("Per-Device Average");
            @SuppressWarnings("unchecked")
            Map<String, Object> ac = (Map<String, Object>) agent.get("config");
            assertThat(ac.get("engine")).isEqualTo("streaming");
            assertThat(ac.get("inputTopic")).isEqualTo("sensor-readings");
            assertThat(ac.get("outputTopic")).isEqualTo("sensor-minute-averages");
            assertThat(ac.get("operators")).isEqualTo(List.of(
                Map.of("type", "keyBy", "field", "deviceId"),
                Map.of("type", "window", "spec", "tumbling(60s)",
                    "aggregations", Map.of("avgTemp", "avg(temperature)")),
                Map.of("type", "filter", "condition", "avgTemp > 75")));

            Map<String, Object> sink = nodes.get(2);
            assertThat(sink.get("label")).isEqualTo("Heat Alert");
            assertThat(sink.get("config")).isEqualTo(
                Map.of("channel", "email", "inputTopic", "sensor-minute-averages"));
        }
    }
}
