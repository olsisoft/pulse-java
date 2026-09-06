package com.streamflow.pulse.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * PVSC and eval suites over the wire.
 *
 * <p>The five Pulse SDKs had no PVSC surface at all — searching for "pvsc"
 * across pulse-js / pulse-py / pulse-rs / pulse-go / pulse-java returned
 * nothing. Anything an operator could do to a topic contract, an arbitration
 * policy or an eval suite was reachable only from the browser.
 *
 * <p>Offline throughout: WireMock serves canned responses on a random port.
 * The point is to pin the wire format, not to exercise a server.
 */
class PvscResourceTest {

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

    private PulseClient client() {
        return PulseClient.builder().baseUrl(baseUrl).token("fake.jwt.token").build();
    }

    @Nested
    @DisplayName("topic contracts")
    class TopicContracts {

        @Test
        void schemasUnwrapsTheEnvelope() {
            mockServer.stubFor(get(urlEqualTo("/api/pulse/pvsc/schemas"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"schemas\":[{\"topic\":\"quotes\"}],\"count\":1}")));

            List<Map<String, Object>> schemas = client().pvsc().schemas();
            assertThat(schemas).hasSize(1);
            assertThat(schemas.get(0)).containsEntry("topic", "quotes");
        }

        @Test
        @DisplayName("the grounding policy reaches the wire — it is the point of the surface")
        void groundingPolicyReachesTheWire() {
            // A grounding policy could be written from Java and from nowhere
            // else until recently. An SDK that dropped it would be the next
            // place it was unreachable from.
            mockServer.stubFor(put(urlEqualTo("/api/pulse/pvsc/schemas"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"status\":\"saved\",\"topic\":\"quotes\"}")));

            client().pvsc().saveSchema(Map.of(
                    "topic", "quotes",
                    "optionalFields", Map.of(
                            "price", Map.of("type", "number", "grounding", "required"))));

            mockServer.verify(putRequestedFor(urlEqualTo("/api/pulse/pvsc/schemas"))
                    .withRequestBody(equalToJson(
                            "{\"topic\":\"quotes\",\"optionalFields\":"
                            + "{\"price\":{\"type\":\"number\",\"grounding\":\"required\"}}}")));
        }

        @Test
        void deleteSendsTheTopicInTheBody() {
            mockServer.stubFor(delete(urlEqualTo("/api/pulse/pvsc/schemas"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"status\":\"deleted\"}")));

            assertThat(client().pvsc().deleteSchema("quotes"))
                    .containsEntry("status", "deleted");
        }
    }

    @Nested
    @DisplayName("arbitration")
    class Arbitration {

        @Test
        void setStancesWritesThroughTheConfigRoute() {
            mockServer.stubFor(put(urlEqualTo("/api/pulse/pvsc/config"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"status\":\"updated\"}")));

            client().pvsc().setStances(List.of(
                    Map.of("domain", "legal", "precedence", 1, "veto", true)));

            mockServer.verify(putRequestedFor(urlEqualTo("/api/pulse/pvsc/config"))
                    .withRequestBody(equalToJson(
                            "{\"arbitrationStances\":"
                            + "[{\"domain\":\"legal\",\"precedence\":1,\"veto\":true}]}")));
        }

        @Test
        @DisplayName("clearing the stances sends an empty list, not a missing key")
        void clearingSendsAnEmptyList() {
            // Clearing disables arbitration. Omitting the key would read as
            // "leave it alone", so the operator's clear would do nothing.
            mockServer.stubFor(put(urlEqualTo("/api/pulse/pvsc/config"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"status\":\"updated\"}")));

            client().pvsc().setStances(null);

            mockServer.verify(putRequestedFor(urlEqualTo("/api/pulse/pvsc/config"))
                    .withRequestBody(equalToJson("{\"arbitrationStances\":[]}")));
        }
    }

    @Nested
    @DisplayName("metrics and DLQ")
    class MetricsAndDlq {

        @Test
        void metricsSurfacesTheQuorumYield() {
            mockServer.stubFor(get(urlEqualTo("/api/pulse/pvsc/metrics"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"pvscModeCount\":300,"
                                    + "\"quorumInformationYield\":0.3333,"
                                    + "\"quorumRedundantGuardianCalls\":400}")));

            assertThat(client().pvsc().metrics())
                    .containsEntry("quorumRedundantGuardianCalls", 400);
        }

        @Test
        void dlqListAndReinject() {
            mockServer.stubFor(get(urlEqualTo("/api/pulse/pvsc/dlq"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"entries\":[{\"eventId\":\"e1\","
                                    + "\"rejectedBy\":\"pvsc-agent-gate\"}],\"total\":1}")));
            mockServer.stubFor(post(urlEqualTo("/api/pulse/pvsc/dlq/reinject"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"reinjected\":true}")));

            PulseClient client = client();
            assertThat(client.pvsc().dlq().get(0))
                    .containsEntry("rejectedBy", "pvsc-agent-gate");

            client.pvsc().reinject("e1");
            mockServer.verify(postRequestedFor(urlEqualTo("/api/pulse/pvsc/dlq/reinject"))
                    .withRequestBody(equalToJson("{\"eventId\":\"e1\"}")));
        }
    }

    @Nested
    @DisplayName("evals")
    class Evals {

        @Test
        void suitesAndCases() {
            mockServer.stubFor(get(urlEqualTo("/api/pulse/evals"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"suites\":[\"pricing\"],\"count\":1}")));
            mockServer.stubFor(get(urlPathEqualTo("/api/pulse/evals/cases"))
                    .withQueryParam("suite", equalTo("pricing"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"cases\":[{\"caseId\":\"c1\"}],\"count\":1}")));

            PulseClient client = client();
            assertThat(client.evals().suites()).containsExactly("pricing");
            assertThat(client.evals().cases("pricing").get(0))
                    .containsEntry("caseId", "c1");
        }

        @Test
        @DisplayName("a REGRESSION is a verdict to branch on, not a thrown exception")
        void regressionIsAVerdict() {
            // The server answers 200 with blocksRelease=true because the run
            // succeeded. A caller in CI branches on blocksRelease; if this
            // threw, "the suite regressed" and "the call broke" would be the
            // same event.
            mockServer.stubFor(post(urlEqualTo("/api/pulse/evals/run"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"suiteId\":\"pricing\",\"total\":10,\"passing\":7,"
                                    + "\"failing\":3,\"baseline\":9,\"gate\":\"REGRESSION\","
                                    + "\"blocksRelease\":true,\"summary\":\"REGRESSION\","
                                    + "\"cases\":[]}")));

            Map<String, Object> report = client().evals().run("pricing");
            assertThat(report).containsEntry("gate", "REGRESSION")
                    .containsEntry("blocksRelease", true);
        }

        @Test
        void recordBaselineSendsTheSuiteId() {
            mockServer.stubFor(post(urlEqualTo("/api/pulse/evals/baseline"))
                    .willReturn(aResponse().withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("{\"suiteId\":\"pricing\",\"baseline\":7}")));

            client().evals().recordBaseline("pricing");
            mockServer.verify(postRequestedFor(urlEqualTo("/api/pulse/evals/baseline"))
                    .withRequestBody(equalToJson("{\"suiteId\":\"pricing\"}")));
        }
    }
}
