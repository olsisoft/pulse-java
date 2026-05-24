package com.streamflow.pulse.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.streamflow.pulse.client.exceptions.PulseApiException;
import com.streamflow.pulse.client.exceptions.PulseAuthException;
import com.streamflow.pulse.client.exceptions.PulseNotFoundException;
import com.streamflow.pulse.client.exceptions.PulseRateLimitException;
import com.streamflow.pulse.client.exceptions.PulseValidationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Smoke tests for {@link PulseClient}.
 *
 * <p>Every test is offline — WireMock spins up a real HTTP server on a random
 * port. The point is to pin the wire format the client speaks against the
 * Pulse OpenAPI spec, not to exercise the real Pulse server.
 */
class PulseClientTest {

    private static WireMockServer mockServer;
    private static String baseUrl;

    @BeforeAll
    static void startServer() {
        mockServer = new WireMockServer(wireMockConfig().dynamicPort());
        mockServer.start();
        baseUrl = "http://localhost:" + mockServer.port();
    }

    @AfterAll
    static void stopServer() {
        mockServer.stop();
    }

    @BeforeEach
    void resetStubs() {
        mockServer.resetAll();
    }

    private PulseClient newClient() {
        return PulseClient.builder().baseUrl(baseUrl).build();
    }

    private PulseClient newAuthedClient() {
        return PulseClient.builder().baseUrl(baseUrl).token("fake.jwt.token").build();
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        void tokenIsMutableViaSetter() {
            PulseClient client = newClient();
            assertThat(client.getToken()).isNull();
            client.setToken("abc");
            assertThat(client.getToken()).isEqualTo("abc");
            client.setToken(null);
            assertThat(client.getToken()).isNull();
        }

        @Test
        void baseUrlTrailingSlashStripped() {
            mockServer.stubFor(get("/api/pulse/version")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"version\":\"2.6.0\"}")));
            PulseClient client = PulseClient.builder().baseUrl(baseUrl + "//").build();
            assertThat(client.version()).containsEntry("version", "2.6.0");
        }

        @Test
        void closeIsIdempotent() {
            PulseClient client = newClient();
            client.close();
            client.close();
        }

        @Test
        void builderRequiresBaseUrl() {
            assertThatThrownBy(() -> PulseClient.builder().build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("baseUrl");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("version()")
    class Version {

        @Test
        void publicNoTokenRequired() {
            mockServer.stubFor(get("/api/pulse/version")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"version\":\"2.6.0\",\"edition\":\"desktop\"}")));
            PulseClient client = newClient();
            assertThat(client.getToken()).isNull();
            Map<String, Object> info = client.version();
            assertThat(info).containsEntry("version", "2.6.0")
                    .containsEntry("edition", "desktop");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("auth")
    class Auth {

        @Test
        void loginCachesToken() {
            mockServer.stubFor(post("/api/auth/login")
                    .withRequestBody(equalToJson("{\"username\":\"alice\",\"password\":\"secret\"}"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"token\":\"new.jwt.token\","
                                    + "\"refreshToken\":\"refresh.token\","
                                    + "\"activeOrg\":{\"id\":\"org1\",\"name\":\"Acme\"}}")));
            PulseClient client = newClient();
            Map<String, Object> response = client.auth().login("alice", "secret");
            assertThat(client.getToken()).isEqualTo("new.jwt.token");
            assertThat(response).containsEntry("refreshToken", "refresh.token");
        }

        @Test
        void loginFailureRaisesAuthExceptionAndDoesNotCacheToken() {
            mockServer.stubFor(post("/api/auth/login")
                    .willReturn(aResponse().withStatus(401)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Invalid credentials\"}")));
            PulseClient client = newClient();
            assertThatThrownBy(() -> client.auth().login("alice", "wrong"))
                    .isInstanceOf(PulseAuthException.class)
                    .hasMessageContaining("Invalid credentials");
            assertThat(client.getToken()).isNull();
        }

        @Test
        void refreshCachesNewToken() {
            mockServer.stubFor(post("/api/auth/refresh")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"token\":\"refreshed.jwt\"}")));
            PulseClient client = newClient();
            client.auth().refresh("some-refresh-token");
            assertThat(client.getToken()).isEqualTo("refreshed.jwt");
        }

        @Test
        void organizationsUnwrapsEnvelope() {
            mockServer.stubFor(get("/api/auth/organizations")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"organizations\":[{\"id\":\"o1\",\"name\":\"Acme\"}]}")));
            List<Map<String, Object>> orgs = newAuthedClient().auth().organizations();
            assertThat(orgs).hasSize(1).first().asInstanceOf(
                    org.assertj.core.api.InstanceOfAssertFactories.MAP)
                    .containsEntry("id", "o1");
        }

        @Test
        void switchOrgCachesNewToken() {
            mockServer.stubFor(post("/api/auth/switch-org")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"token\":\"switched.jwt\"}")));
            PulseClient client = newAuthedClient();
            client.auth().switchOrg("org2");
            assertThat(client.getToken()).isEqualTo("switched.jwt");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("pipelines")
    class Pipelines {

        @Test
        void listUnwrapsEnvelope() {
            mockServer.stubFor(get("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"pipelines\":["
                                    + "{\"id\":\"p1\",\"name\":\"demo\"},"
                                    + "{\"id\":\"p2\",\"name\":\"fraud\"}]}")));
            List<Map<String, Object>> pipelines = newAuthedClient().pipelines().list();
            assertThat(pipelines).hasSize(2);
            assertThat(pipelines.get(0)).containsEntry("id", "p1");
        }

        @Test
        void listReturnsEmptyOnMissingEnvelope() {
            mockServer.stubFor(get("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{}")));
            assertThat(newAuthedClient().pipelines().list()).isEmpty();
        }

        @Test
        void getReturnsOnePipeline() {
            mockServer.stubFor(get("/api/pulse/pipelines/p1")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"id\":\"p1\",\"name\":\"demo\"}")));
            Map<String, Object> result = newAuthedClient().pipelines().get("p1");
            assertThat(result).containsEntry("id", "p1");
        }

        @Test
        void getMissingPipelineRaisesNotFound() {
            mockServer.stubFor(get("/api/pulse/pipelines/nope")
                    .willReturn(aResponse().withStatus(404)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"not found\"}")));
            assertThatThrownBy(() -> newAuthedClient().pipelines().get("nope"))
                    .isInstanceOf(PulseNotFoundException.class);
        }

        @Test
        void createReturnsCreatedPipeline() {
            mockServer.stubFor(post("/api/pulse/pipelines")
                    .withRequestBody(containing("\"name\":\"new\""))
                    .willReturn(aResponse().withStatus(201)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"id\":\"p3\",\"name\":\"new\"}")));
            Map<String, Object> result = newAuthedClient().pipelines().create(
                    Map.of("name", "new", "nodes", List.of(Map.of("id", "n1", "type", "source"))));
            assertThat(result).containsEntry("id", "p3");
        }

        @Test
        void createValidationFailureRaisesValidationException() {
            mockServer.stubFor(post("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(400)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Missing required field: nodes\"}")));
            assertThatThrownBy(() -> newAuthedClient().pipelines().create(Map.of("name", "bad")))
                    .isInstanceOf(PulseValidationException.class)
                    .hasMessageContaining("Missing required field");
        }

        @Test
        void deleteReturnsEmptyOn204() {
            mockServer.stubFor(delete("/api/pulse/pipelines/p1")
                    .willReturn(aResponse().withStatus(204)));
            newAuthedClient().pipelines().delete("p1");
            mockServer.verify(1, com.github.tomakehurst.wiremock.client.WireMock
                    .deleteRequestedFor(urlEqualTo("/api/pulse/pipelines/p1")));
        }

        @Test
        void pathParamsAreUrlEncoded() {
            mockServer.stubFor(get("/api/pulse/pipelines/foo%2Fbar")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"id\":\"foo/bar\"}")));
            assertThat(newAuthedClient().pipelines().get("foo/bar"))
                    .containsEntry("id", "foo/bar");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("agents")
    class Agents {

        @Test
        void listUnwrapsEnvelope() {
            mockServer.stubFor(get("/api/pulse/agents")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agents\":[{\"id\":\"a1\",\"name\":\"fraud-detector\","
                                    + "\"engineType\":\"streaming\"}]}")));
            List<Map<String, Object>> agents = newAuthedClient().agents().list();
            assertThat(agents.get(0)).containsEntry("engineType", "streaming");
        }

        @Test
        void getReturnsOneAgent() {
            mockServer.stubFor(get("/api/pulse/agents/a1")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"id\":\"a1\",\"name\":\"fraud-detector\"}")));
            assertThat(newAuthedClient().agents().get("a1")).containsEntry("id", "a1");
        }

        @Test
        void updatePutsFullConfigAndReturnsFreshSnapshot() {
            mockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.put("/api/pulse/agents/a1")
                    .withHeader("Content-Type", containing("application/json"))
                    .withRequestBody(equalToJson("{\"name\":\"fraud-detector-v2\","
                            + "\"engineType\":\"rule-based\","
                            + "\"config\":{\"rules\":[{\"if\":\"amount > 5000\",\"then\":\"block\"}]}}",
                            true, true))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"id\":\"a1\",\"name\":\"fraud-detector-v2\","
                                    + "\"engineType\":\"rule-based\",\"status\":\"running\"}")));

            Map<String, Object> newConfig = new HashMap<>();
            newConfig.put("name", "fraud-detector-v2");
            newConfig.put("engineType", "rule-based");
            newConfig.put("config",
                    Map.of("rules", List.of(Map.of("if", "amount > 5000", "then", "block"))));

            Map<String, Object> result = newAuthedClient().agents().update("a1", newConfig);
            assertThat(result).containsEntry("name", "fraud-detector-v2");
        }

        @Test
        void updateRaisesValidationOnSelfLoop400() {
            mockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.put("/api/pulse/agents/a1")
                    .willReturn(aResponse().withStatus(400)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Agent would self-loop: outputTopic == inputTopic\","
                                    + "\"unsafeFields\":[\"outputTopic\"]}")));
            Map<String, Object> badConfig = new HashMap<>();
            badConfig.put("name", "x");
            badConfig.put("inputTopic", "t");
            badConfig.put("outputTopic", "t");
            assertThatThrownBy(() -> newAuthedClient().agents().update("a1", badConfig))
                    .isInstanceOf(PulseValidationException.class)
                    .hasMessageContaining("self-loop");
        }

        @Test
        void updateRaisesNotFoundOnMissingAgent() {
            mockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.put("/api/pulse/agents/missing")
                    .willReturn(aResponse().withStatus(404)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Agent not found: missing\"}")));
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("name", "x");
            assertThatThrownBy(() -> newAuthedClient().agents().update("missing", cfg))
                    .isInstanceOf(PulseNotFoundException.class);
        }

        @Test
        void deleteReturnsCleanlyOn204() {
            mockServer.stubFor(delete("/api/pulse/agents/a1")
                    .willReturn(aResponse().withStatus(204)));
            // No throw = pass
            newAuthedClient().agents().delete("a1");
        }

        @Test
        void deleteRaisesNotFound() {
            mockServer.stubFor(delete("/api/pulse/agents/missing")
                    .willReturn(aResponse().withStatus(404)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Agent not found\"}")));
            assertThatThrownBy(() -> newAuthedClient().agents().delete("missing"))
                    .isInstanceOf(PulseNotFoundException.class);
        }

        @Test
        void updateWithoutTokenRaisesAuthExceptionBeforeAnyHttpCall() {
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("name", "x");
            assertThatThrownBy(() -> newClient().agents().update("a1", cfg))
                    .isInstanceOf(PulseAuthException.class);
            mockServer.verify(0, com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor(
                    urlEqualTo("/api/pulse/agents/a1")));
        }

        @Test
        void deleteWithoutTokenRaisesAuthExceptionBeforeAnyHttpCall() {
            assertThatThrownBy(() -> newClient().agents().delete("a1"))
                    .isInstanceOf(PulseAuthException.class);
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("templates")
    class Templates {

        @Test
        void listUnwrapsEnvelope() {
            mockServer.stubFor(get("/api/pulse/templates")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"templates\":[{\"id\":\"fraud-detection\","
                                    + "\"name\":\"Fraud Detection\"}]}")));
            List<Map<String, Object>> templates = newAuthedClient().templates().list();
            assertThat(templates.get(0)).containsEntry("id", "fraud-detection");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("events.stream() — B-098 Phase 7 SSE")
    class Events {

        @Test
        void streamYieldsParsedEvents() throws Exception {
            mockServer.stubFor(get("/api/pulse/events/stream")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "text/event-stream")
                            .withBody("data: {\"type\":\"fraud_signal\",\"payload\":{\"customerId\":\"c1\"}}\n\n"
                                    + "data: {\"type\":\"heartbeat\"}\n\n")));

            java.util.concurrent.CopyOnWriteArrayList<Map<String, Object>> collected =
                    new java.util.concurrent.CopyOnWriteArrayList<>();
            newAuthedClient().events().stream(collected::add).get();
            assertThat(collected).hasSize(2);
            assertThat(collected.get(0)).containsEntry("type", "fraud_signal");
            assertThat(collected.get(1)).containsEntry("type", "heartbeat");
        }

        @Test
        void streamSkipsCommentsAndDispatchesOnBlankLine() throws Exception {
            mockServer.stubFor(get("/api/pulse/events/stream")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "text/event-stream")
                            .withBody(": keep-alive\n\n"
                                    + "data: {\"type\":\"a\"}\n\n"
                                    + ": another keep-alive\n\n"
                                    + "data: {\"type\":\"b\"}\n\n")));

            java.util.concurrent.CopyOnWriteArrayList<String> types =
                    new java.util.concurrent.CopyOnWriteArrayList<>();
            newAuthedClient().events().stream(ev -> types.add((String) ev.get("type"))).get();
            assertThat(types).containsExactly("a", "b");
        }

        @Test
        void streamFallbackForNonJSONPayload() throws Exception {
            mockServer.stubFor(get("/api/pulse/events/stream")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "text/event-stream")
                            .withBody("data: not-json-here\n\n")));

            java.util.concurrent.CopyOnWriteArrayList<Map<String, Object>> collected =
                    new java.util.concurrent.CopyOnWriteArrayList<>();
            newAuthedClient().events().stream(collected::add).get();
            assertThat(collected).hasSize(1);
            assertThat(collected.get(0)).containsEntry("data", "not-json-here");
        }

        @Test
        void streamThrowsAuthExceptionWhenNoTokenSet() {
            assertThatThrownBy(() -> newClient().events().stream(ev -> {}))
                    .isInstanceOf(PulseAuthException.class)
                    .hasMessageContaining("no token set");
        }

        @Test
        void streamFailsFutureOn401() {
            mockServer.stubFor(get("/api/pulse/events/stream")
                    .willReturn(aResponse().withStatus(401)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"expired\"}")));

            assertThatThrownBy(() -> newAuthedClient().events().stream(ev -> {}).get())
                    .hasCauseInstanceOf(PulseAuthException.class);
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("iq — B-106 Interactive Queries")
    class Iq {

        // ---- summary ----
        @Test
        void summaryReturnsStateMetadata() {
            mockServer.stubFor(get("/api/pulse/iq/agents/fraud-detector/state")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"fraud-detector\","
                                    + "\"queryable\":true,\"backend\":\"rocksdb\","
                                    + "\"hotSize\":1500,\"hotBytes\":32768,"
                                    + "\"coldSize\":50000,\"coldBytes\":4194304,"
                                    + "\"lastCheckpointId\":42,\"totalSize\":51500}")));
            Map<String, Object> summary = newAuthedClient().iq().summary("fraud-detector");
            assertThat(summary).containsEntry("queryable", true)
                    .containsEntry("backend", "rocksdb");
            assertThat(((Number) summary.get("totalSize")).longValue()).isEqualTo(51500L);
        }

        @Test
        void summaryHandlesNonQueryableAgent() {
            mockServer.stubFor(get("/api/pulse/iq/agents/rule-agent/state")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"rule-agent\","
                                    + "\"queryable\":false,\"backend\":\"none\","
                                    + "\"hotSize\":0,\"hotBytes\":0,"
                                    + "\"coldSize\":0,\"coldBytes\":0,"
                                    + "\"lastCheckpointId\":-1,\"totalSize\":0}")));
            Map<String, Object> summary = newAuthedClient().iq().summary("rule-agent");
            assertThat(summary).containsEntry("queryable", false);
            assertThat(((Number) summary.get("lastCheckpointId")).longValue()).isEqualTo(-1L);
        }

        @Test
        void summaryUrlEncodesAgentIdWithSlash() {
            mockServer.stubFor(get("/api/pulse/iq/agents/tenant%2Fagent/state")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"tenant/agent\","
                                    + "\"queryable\":true,\"backend\":\"rocksdb\","
                                    + "\"hotSize\":0,\"hotBytes\":0,"
                                    + "\"coldSize\":0,\"coldBytes\":0,"
                                    + "\"lastCheckpointId\":0,\"totalSize\":0}")));
            Map<String, Object> summary = newAuthedClient().iq().summary("tenant/agent");
            assertThat(summary).containsEntry("agentId", "tenant/agent");
        }

        // ---- get ----
        @SuppressWarnings("unchecked")
        @Test
        void getReturnsValueAtKey() {
            mockServer.stubFor(get("/api/pulse/iq/agents/fraud-detector/state/value/customer-42")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"fraud-detector\","
                                    + "\"key\":\"customer-42\","
                                    + "\"value\":{\"tx_count_60s\":7,\"total_amount_60s\":12500}}")));
            Map<String, Object> result = newAuthedClient().iq().get("fraud-detector", "customer-42");
            assertThat(result).containsEntry("key", "customer-42");
            Map<String, Object> value = (Map<String, Object>) result.get("value");
            assertThat(((Number) value.get("tx_count_60s")).intValue()).isEqualTo(7);
        }

        @Test
        void getUrlEncodesKeyWithSlash() {
            // Server's URLDecoder reverses our encoding: "user:123/orders" →
            // "user%3A123%2Forders" on the wire, decoded back on receipt.
            mockServer.stubFor(get("/api/pulse/iq/agents/sessions/state/value/user%3A123%2Forders")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"sessions\","
                                    + "\"key\":\"user:123/orders\","
                                    + "\"value\":[\"o1\",\"o2\",\"o3\"]}")));
            Map<String, Object> result = newAuthedClient().iq().get("sessions", "user:123/orders");
            // Cast through List<?> so AssertJ infers the element type for
            // containsExactly without the capture-of-? trap on raw LIST factory.
            @SuppressWarnings("unchecked")
            java.util.List<String> values = (java.util.List<String>) result.get("value");
            assertThat(values).containsExactly("o1", "o2", "o3");
        }

        @Test
        void getReturnsNullValueWhenPresentWithNull() {
            mockServer.stubFor(get("/api/pulse/iq/agents/a1/state/value/k1")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"a1\",\"key\":\"k1\",\"value\":null}")));
            Map<String, Object> result = newAuthedClient().iq().get("a1", "k1");
            assertThat(result).containsKey("value");
            assertThat(result.get("value")).isNull();
        }

        @Test
        void get404KeyNotFoundThrowsWithKeyBody() {
            mockServer.stubFor(get("/api/pulse/iq/agents/a1/state/value/missing-key")
                    .willReturn(aResponse().withStatus(404)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Key not found\","
                                    + "\"agentId\":\"a1\",\"key\":\"missing-key\"}")));
            assertThatThrownBy(() -> newAuthedClient().iq().get("a1", "missing-key"))
                    .isInstanceOfSatisfying(PulseNotFoundException.class, ex -> {
                        assertThat(ex.body()).containsEntry("error", "Key not found")
                                .containsEntry("key", "missing-key");
                    });
        }

        @Test
        void get404AgentNotQueryableThrowsWithReason() {
            mockServer.stubFor(get("/api/pulse/iq/agents/a1/state/value/k1")
                    .willReturn(aResponse().withStatus(404)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Agent has no queryable state\","
                                    + "\"agentId\":\"a1\","
                                    + "\"reason\":\"non-streaming or stopped\"}")));
            assertThatThrownBy(() -> newAuthedClient().iq().get("a1", "k1"))
                    .isInstanceOfSatisfying(PulseNotFoundException.class, ex ->
                            assertThat(ex.body()).containsEntry("reason", "non-streaming or stopped"));
        }

        // ---- scan ----
        @Test
        void scanReturnsEntriesWithDefaultLimit() {
            mockServer.stubFor(get("/api/pulse/iq/agents/a1/state/scan?limit=100")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"a1\",\"entries\":["
                                    + "{\"key\":\"k1\",\"value\":1},"
                                    + "{\"key\":\"k2\",\"value\":2}],"
                                    + "\"count\":2,\"truncated\":false,\"limitApplied\":100}")));
            Map<String, Object> result = newAuthedClient().iq().scan("a1");
            assertThat(((java.util.List<?>) result.get("entries"))).hasSize(2);
            assertThat(result).containsEntry("truncated", false);
        }

        @Test
        void scanPassesThroughRangeParams() {
            mockServer.stubFor(get(urlEqualTo("/api/pulse/iq/agents/a1/state/scan?limit=50&start=alice&end=bob"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"a1\",\"entries\":[],"
                                    + "\"count\":0,\"truncated\":false,\"limitApplied\":50,"
                                    + "\"start\":\"alice\",\"end\":\"bob\"}")));
            newAuthedClient().iq().scan("a1",
                    IQResource.ScanOptions.builder().start("alice").end("bob").limit(50).build());
            // If the request matched, the stub returned 200 — no need for extra verify
        }

        @Test
        void scan404AgentNotQueryableThrows() {
            mockServer.stubFor(get(urlEqualTo("/api/pulse/iq/agents/a1/state/scan?limit=100"))
                    .willReturn(aResponse().withStatus(404)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Agent has no queryable state\","
                                    + "\"agentId\":\"a1\",\"reason\":\"non-streaming or stopped\"}")));
            assertThatThrownBy(() -> newAuthedClient().iq().scan("a1"))
                    .isInstanceOf(PulseNotFoundException.class);
        }

        // ---- listKeys ----
        @Test
        void listKeysReturnsKeysArray() {
            mockServer.stubFor(get(urlEqualTo("/api/pulse/iq/agents/a1/state/keys?limit=100"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"a1\","
                                    + "\"keys\":[\"alpha\",\"beta\",\"gamma\"],"
                                    + "\"count\":3,\"truncated\":false,\"limitApplied\":100}")));
            Map<String, Object> result = newAuthedClient().iq().listKeys("a1");
            @SuppressWarnings("unchecked")
            java.util.List<String> keys = (java.util.List<String>) result.get("keys");
            assertThat(keys).containsExactly("alpha", "beta", "gamma");
        }

        // ---- query ----
        @Test
        void queryFlatWithFilter() {
            mockServer.stubFor(post(urlEqualTo("/api/pulse/iq/agents/fraud-detector/state/query"))
                    .withRequestBody(containing("\"field\":\"tx_count_60s\""))
                    .withRequestBody(containing("\"op\":\"gt\""))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"fraud-detector\","
                                    + "\"entries\":[{\"key\":\"c1\","
                                    + "\"value\":{\"tx_count_60s\":8}}],"
                                    + "\"count\":1,\"totalScanned\":1500,"
                                    + "\"matchedCount\":1,\"truncated\":false,"
                                    + "\"limitApplied\":100}")));
            Map<String, Object> result = newAuthedClient().iq().query("fraud-detector",
                    IQResource.QueryOptions.builder()
                            .filter(IQResource.QueryOptions.leaf("tx_count_60s", "gt", 5))
                            .build());
            assertThat(((Number) result.get("count")).intValue()).isEqualTo(1);
        }

        @Test
        void queryGroupedReturnsGroups() {
            mockServer.stubFor(post(urlEqualTo("/api/pulse/iq/agents/users/state/query"))
                    .withRequestBody(containing("\"groupBy\":\"plan\""))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"users\","
                                    + "\"groups\":[{\"groupKey\":\"free\",\"count\":8420},"
                                    + "{\"groupKey\":\"pro\",\"count\":312}],"
                                    + "\"groupCount\":2,\"totalScanned\":8732,"
                                    + "\"matchedCount\":8732,\"truncated\":false,"
                                    + "\"limitApplied\":100}")));
            Map<String, Object> result = newAuthedClient().iq().query("users",
                    IQResource.QueryOptions.builder().groupBy("plan").build());
            assertThat(((Number) result.get("groupCount")).intValue()).isEqualTo(2);
        }

        @Test
        void queryEmptyOptionsSendsNoBody() {
            mockServer.stubFor(post(urlEqualTo("/api/pulse/iq/agents/a1/state/query"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"agentId\":\"a1\",\"entries\":[],"
                                    + "\"count\":0,\"totalScanned\":0,"
                                    + "\"matchedCount\":0,\"truncated\":false,"
                                    + "\"limitApplied\":100}")));
            Map<String, Object> result = newAuthedClient().iq().query("a1");
            assertThat(result).containsEntry("count", 0);
        }

        @Test
        void query400InvalidFilterThrowsValidation() {
            mockServer.stubFor(post(urlEqualTo("/api/pulse/iq/agents/a1/state/query"))
                    .willReturn(aResponse().withStatus(400)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"filter cannot mix discriminators "
                                    + "(field/and/or/not) at the same level\"}")));
            // Send a knowingly-bad filter via raw map (mixing field + and).
            // Server-side validation rejects.
            Map<String, Object> badFilter = new HashMap<>();
            badFilter.put("field", "a");
            badFilter.put("and", java.util.List.of(
                    IQResource.QueryOptions.leaf("b", "eq", 1)));
            assertThatThrownBy(() -> newAuthedClient().iq().query("a1",
                    IQResource.QueryOptions.builder().filter(badFilter).build()))
                    .isInstanceOf(PulseValidationException.class)
                    .hasMessageContaining("discriminator");
        }

        // ---- auth gating ----
        @Test
        void summaryWithoutTokenThrowsAuthExceptionBeforeAnyHttpCall() {
            assertThatThrownBy(() -> newClient().iq().summary("a1"))
                    .isInstanceOf(PulseAuthException.class);
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("client.streams() (B-107)")
    class Streams {

        @Test
        void accessorExists() {
            assertThat(newClient().streams()).isNotNull();
        }

        @Test
        void compileReturnsMapWithoutHttpCall() {
            // No WireMock stub. If compile() reached the network, WireMock's
            // strict (default) mode would return 404 and we'd notice.
            StreamBuilder b = new StreamBuilder("p")
                    .fromTopic("in")
                    .filter("x > 0");
            Map<String, Object> out = newClient().streams().compile(b);
            assertThat(out.get("name")).isEqualTo("p");
            mockServer.verify(0, getRequestedFor(urlEqualTo("/api/pulse/pipelines")));
        }

        @Test
        void deployPostsBuiltDefinitionToPipelinesEndpoint() {
            mockServer.stubFor(post("/api/pulse/pipelines")
                    .withHeader("Content-Type", containing("application/json"))
                    .willReturn(aResponse().withStatus(201)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"id\":\"p-42\",\"name\":\"fraud-detector\","
                                    + "\"status\":\"running\"}")));

            StreamBuilder b = new StreamBuilder("fraud-detector")
                    .fromTopic("payments")
                    .filter("amount > 1000")
                    .keyBy("customer_id")
                    .window(StreamBuilder.Windows.tumbling("60s"),
                            new StreamBuilder.WindowOptions().aggregations(
                                    Map.of("cnt", StreamBuilder.Aggregators.count())))
                    .filter("cnt > 5")
                    .toTopic("fraud-alerts");

            Map<String, Object> result = newAuthedClient().streams().deploy(b);
            assertThat(result.get("id")).isEqualTo("p-42");

            // Verify the wire body shape (matches the validator-accepted JSON)
            mockServer.verify(1, com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                    urlEqualTo("/api/pulse/pipelines"))
                    .withRequestBody(equalToJson("{\n"
                            + "  \"name\": \"fraud-detector\",\n"
                            + "  \"nodes\": [\n"
                            + "    {\"type\":\"source\",\"label\":\"kafka source\","
                            + "     \"config\":{\"engine\":\"kafka\",\"inputTopic\":\"payments\"}},\n"
                            + "    {\"type\":\"agent\",\"label\":\"fraud-detector\","
                            + "     \"config\":{\"engine\":\"streaming\",\"inputTopic\":\"payments\","
                            + "     \"outputTopic\":\"fraud-alerts\",\"operators\":["
                            + "{\"type\":\"filter\",\"condition\":\"amount > 1000\"},"
                            + "{\"type\":\"keyBy\",\"field\":\"customer_id\"},"
                            + "{\"type\":\"window\",\"spec\":\"tumbling(60s)\","
                            + "\"aggregations\":{\"cnt\":\"count()\"}},"
                            + "{\"type\":\"filter\",\"condition\":\"cnt > 5\"}]}}\n"
                            + "  ]\n"
                            + "}", true, true)));
        }

        @Test
        void deployNameOverridePropagatesToWireBody() {
            mockServer.stubFor(post("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(201)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"id\":\"p\",\"name\":\"renamed\"}")));

            StreamBuilder b = new StreamBuilder("original")
                    .fromTopic("in").filter("x > 0");
            newAuthedClient().streams().deploy(b, "renamed");

            mockServer.verify(com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                    urlEqualTo("/api/pulse/pipelines"))
                    .withRequestBody(containing("\"name\":\"renamed\"")));
        }

        @Test
        void deployWithoutTokenThrowsAuthBeforeAnyHttpCall() {
            StreamBuilder b = new StreamBuilder("p").fromTopic("in").filter("x > 0");
            assertThatThrownBy(() -> newClient().streams().deploy(b))
                    .isInstanceOf(PulseAuthException.class);
            mockServer.verify(0, getRequestedFor(urlEqualTo("/api/pulse/pipelines")));
        }
    }

    // ------------------------------------------------------------------
    @Nested
    @DisplayName("error handling")
    class Errors {

        @Test
        void noTokenSetRaisesAuthExceptionBeforeAnyHttpCall() {
            // No WireMock stub — if the client incorrectly issued the request,
            // WireMock would return a 404 and the test would still pass for the
            // wrong reason. Use verify to confirm no call was made.
            assertThatThrownBy(() -> newClient().pipelines().list())
                    .isInstanceOf(PulseAuthException.class)
                    .hasMessageContaining("No token set");
            mockServer.verify(0, getRequestedFor(urlEqualTo("/api/pulse/pipelines")));
        }

        @Test
        void rateLimitParsesRetryAfterFromBody() {
            mockServer.stubFor(get("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(429)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Rate limit exceeded\","
                                    + "\"errorCode\":\"RATE_LIMITED\","
                                    + "\"retryAfterSeconds\":60,"
                                    + "\"limit\":120,\"remaining\":0}")));
            assertThatThrownBy(() -> newAuthedClient().pipelines().list())
                    .isInstanceOfSatisfying(PulseRateLimitException.class, ex ->
                            assertThat(ex.retryAfterSeconds()).contains(60));
        }

        @Test
        void rateLimitFallsBackToRetryAfterHeader() {
            mockServer.stubFor(get("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(429)
                            .withHeader("Retry-After", "30")
                            .withBody("Too Many Requests")));
            assertThatThrownBy(() -> newAuthedClient().pipelines().list())
                    .isInstanceOfSatisfying(PulseRateLimitException.class, ex ->
                            assertThat(ex.retryAfterSeconds()).contains(30));
        }

        @Test
        void unknown5xxRaisesGenericApiException() {
            mockServer.stubFor(get("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(500)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"error\":\"Internal\",\"errorClass\":\"NPE\"}")));
            assertThatThrownBy(() -> newAuthedClient().pipelines().list())
                    .isInstanceOf(PulseApiException.class)
                    .isNotInstanceOf(PulseAuthException.class)
                    .isNotInstanceOf(PulseNotFoundException.class)
                    .isNotInstanceOf(PulseValidationException.class)
                    .isNotInstanceOf(PulseRateLimitException.class);
        }

        @Test
        void bearerTokenAttachedToOutboundRequest() {
            mockServer.stubFor(get("/api/pulse/pipelines")
                    .withHeader("Authorization", equalTo("Bearer fake.jwt.token"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"pipelines\":[]}")));
            assertThat(newAuthedClient().pipelines().list()).isEmpty();
            mockServer.verify(getRequestedFor(urlEqualTo("/api/pulse/pipelines"))
                    .withHeader("Authorization", equalTo("Bearer fake.jwt.token")));
        }

        @Test
        void userAgentHeaderIsSet() {
            mockServer.stubFor(get("/api/pulse/pipelines")
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"pipelines\":[]}")));
            newAuthedClient().pipelines().list();
            mockServer.verify(getRequestedFor(urlEqualTo("/api/pulse/pipelines"))
                    .withHeader("User-Agent", containing("pulse-client-java")));
        }
    }
}
