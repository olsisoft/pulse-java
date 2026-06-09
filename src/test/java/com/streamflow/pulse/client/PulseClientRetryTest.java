package com.streamflow.pulse.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.streamflow.pulse.client.exceptions.PulseApiException;
import com.streamflow.pulse.client.exceptions.PulseNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * B-170 / GA-gate #312 — opt-in retry policy (mirrors the Python reference).
 *
 * <p>Each test drives a real request through {@link PulseClient} against a
 * WireMock server with sequenced (scenario) responses, and asserts the request
 * count. Offline — WireMock binds a random port.
 */
class PulseClientRetryTest {

    private static WireMockServer server;
    private static String baseUrl;

    @BeforeAll
    static void startServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        baseUrl = "http://localhost:" + server.port();
    }

    @AfterAll
    static void stopServer() {
        server.stop();
    }

    @AfterEach
    void reset() {
        server.resetAll();
    }

    /** Fast backoff so the tests don't actually sleep. */
    private PulseClient fastClient(int maxRetries, boolean nonIdempotent) {
        return PulseClient.builder()
                .baseUrl(baseUrl)
                .token("t")
                .maxRetries(maxRetries)
                .retryBackoff(Duration.ofMillis(1))
                .retryMaxBackoff(Duration.ofMillis(2))
                .retryNonIdempotent(nonIdempotent)
                .build();
    }

    @Test
    void offByDefault_oneAttempt() {
        server.stubFor(get(urlEqualTo("/api/pulse/version"))
                .willReturn(aResponse().withStatus(503)));
        PulseClient client = PulseClient.builder().baseUrl(baseUrl).build(); // no maxRetries
        assertThatThrownBy(client::version)
                .isInstanceOf(PulseApiException.class)
                .satisfies(e -> assertThat(((PulseApiException) e).statusCode()).isEqualTo(503));
        server.verify(exactly(1), getRequestedFor(urlEqualTo("/api/pulse/version")));
    }

    @Test
    void idempotentGetRetriedOn5xxThenSucceeds() {
        server.stubFor(get(urlEqualTo("/api/pulse/version")).inScenario("s")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(503)).willSetStateTo("ok"));
        server.stubFor(get(urlEqualTo("/api/pulse/version")).inScenario("s")
                .whenScenarioStateIs("ok")
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json").withBody("{\"v\":1}")));

        Map<String, Object> out = fastClient(2, false).version();
        assertThat(out).containsEntry("v", 1);
        server.verify(exactly(2), getRequestedFor(urlEqualTo("/api/pulse/version")));
    }

    @Test
    void exhaustsRetriesThenThrows() {
        server.stubFor(get(urlEqualTo("/api/pulse/version"))
                .willReturn(aResponse().withStatus(503)));
        assertThatThrownBy(() -> fastClient(2, false).version())
                .isInstanceOf(PulseApiException.class);
        server.verify(exactly(3), getRequestedFor(urlEqualTo("/api/pulse/version"))); // initial + 2
    }

    @Test
    void rateLimitRetriedForPostHonouringRetryAfter() {
        server.stubFor(post(urlEqualTo("/api/pulse/pipelines")).inScenario("s")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(429)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"retryAfterSeconds\":0}")).willSetStateTo("ok"));
        server.stubFor(post(urlEqualTo("/api/pulse/pipelines")).inScenario("s")
                .whenScenarioStateIs("ok")
                .willReturn(aResponse().withStatus(201)
                        .withHeader("Content-Type", "application/json").withBody("{\"id\":\"p1\"}")));

        Map<String, Object> out = fastClient(1, false).pipelines().create(Map.of("name", "x"));
        assertThat(out).containsEntry("id", "p1");
        server.verify(exactly(2), postRequestedFor(urlEqualTo("/api/pulse/pipelines")));
    }

    @Test
    void post5xxNotRetriedByDefault() {
        server.stubFor(post(urlEqualTo("/api/pulse/pipelines"))
                .willReturn(aResponse().withStatus(503)));
        assertThatThrownBy(() -> fastClient(3, false).pipelines().create(Map.of("name", "x")))
                .isInstanceOf(PulseApiException.class);
        server.verify(exactly(1), postRequestedFor(urlEqualTo("/api/pulse/pipelines")));
    }

    @Test
    void post5xxRetriedWhenOptedIn() {
        server.stubFor(post(urlEqualTo("/api/pulse/pipelines")).inScenario("s")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(503)).willSetStateTo("ok"));
        server.stubFor(post(urlEqualTo("/api/pulse/pipelines")).inScenario("s")
                .whenScenarioStateIs("ok")
                .willReturn(aResponse().withStatus(201)
                        .withHeader("Content-Type", "application/json").withBody("{\"id\":\"p1\"}")));

        Map<String, Object> out = fastClient(2, true).pipelines().create(Map.of("name", "x"));
        assertThat(out).containsEntry("id", "p1");
        server.verify(exactly(2), postRequestedFor(urlEqualTo("/api/pulse/pipelines")));
    }

    @Test
    void transportErrorRetriedForGet() {
        server.stubFor(get(urlEqualTo("/api/pulse/pipelines")).inScenario("s")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)).willSetStateTo("ok"));
        server.stubFor(get(urlEqualTo("/api/pulse/pipelines")).inScenario("s")
                .whenScenarioStateIs("ok")
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json").withBody("{\"pipelines\":[]}")));

        assertThat(fastClient(1, false).pipelines().list()).isEmpty();
        server.verify(exactly(2), getRequestedFor(urlEqualTo("/api/pulse/pipelines")));
    }

    @Test
    void terminal404NotRetried() {
        server.stubFor(get(urlEqualTo("/api/pulse/pipelines/nope"))
                .willReturn(aResponse().withStatus(404)
                        .withHeader("Content-Type", "application/json").withBody("{\"error\":\"nope\"}")));
        assertThatThrownBy(() -> fastClient(3, false).pipelines().get("nope"))
                .isInstanceOf(PulseNotFoundException.class);
        server.verify(exactly(1), getRequestedFor(urlEqualTo("/api/pulse/pipelines/nope")));
    }
}
