package com.streamflow.pulse.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.pulse.client.exceptions.PulseApiException;
import com.streamflow.pulse.client.exceptions.PulseAuthException;
import com.streamflow.pulse.client.exceptions.PulseClientException;
import com.streamflow.pulse.client.exceptions.PulseNotFoundException;
import com.streamflow.pulse.client.exceptions.PulseRateLimitException;
import com.streamflow.pulse.client.exceptions.PulseValidationException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Official Java client for the StreamFlow Pulse REST API.
 *
 * <p>Built on {@code java.net.http.HttpClient} (JDK 11+) so the only runtime
 * dependency is Jackson (which is on every Spring Boot / Quarkus / Micronaut
 * classpath already). Targets JDK 17 LTS as the floor — the broadest
 * enterprise reach.
 *
 * <h2>Quick start</h2>
 *
 * <pre>{@code
 * PulseClient client = PulseClient.builder()
 *         .baseUrl("http://localhost:9090")
 *         .build();
 *
 * client.auth().login("alice", "secret");
 *
 * for (Map<String, Object> pipeline : client.pipelines().list()) {
 *     System.out.println(pipeline.get("name"));
 * }
 * }</pre>
 *
 * <h2>Authentication</h2>
 *
 * Three patterns are supported:
 *
 * <ol>
 *   <li><b>Username + password</b>: call {@link AuthResource#login(String, String)}.
 *       The returned JWT is cached on the client for subsequent calls.</li>
 *   <li><b>Pre-minted JWT</b>: set via {@link Builder#token(String)}. Used for
 *       CI / service-account flows where the token is minted out-of-band.</li>
 *   <li><b>Token rotation</b>: call {@link #setToken(String)} from a background
 *       refresh loop. Long-running daemons should persist the {@code refreshToken}
 *       returned by {@code login()} and call
 *       {@link AuthResource#refresh(String)} before the JWT expires (default 1 h
 *       TTL).</li>
 * </ol>
 *
 * <h2>Thread safety</h2>
 *
 * Instances are thread-safe. The internal {@link HttpClient} pools connections;
 * a single {@code PulseClient} can be shared across an entire application.
 *
 * <h2>Spec source of truth</h2>
 *
 * Every method corresponds 1:1 to an endpoint in the Pulse OpenAPI 3.1 spec
 * ({@code streamflow-pulse/src/main/resources/openapi/openapi.yaml}). Drift is
 * caught at PR time by the in-tree spec invariant tests (B-103).
 */
public final class PulseClient implements AutoCloseable {

    private static final String USER_AGENT = "pulse-client-java/2.6.0";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final String baseUrl;
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final Duration timeout;

    private volatile String token;

    private final AuthResource auth;
    private final PipelinesResource pipelines;
    private final AgentsResource agents;
    private final TemplatesResource templates;
    private final UsersResource users;
    private final EventsResource events;
    private final IQResource iq;
    private final StreamsResource streams;

    private PulseClient(Builder b) {
        this.baseUrl = stripTrailingSlash(Objects.requireNonNull(b.baseUrl, "baseUrl is required"));
        this.token = b.token;
        this.timeout = b.timeout != null ? b.timeout : DEFAULT_TIMEOUT;
        this.mapper = b.mapper != null ? b.mapper : new ObjectMapper();
        this.http = b.httpClient != null
                ? b.httpClient
                : HttpClient.newBuilder()
                        .connectTimeout(this.timeout)
                        .build();
        this.auth = new AuthResource(this);
        this.pipelines = new PipelinesResource(this);
        this.agents = new AgentsResource(this);
        this.templates = new TemplatesResource(this);
        this.users = new UsersResource(this);
        this.events = new EventsResource(this);
        this.iq = new IQResource(this);
        this.streams = new StreamsResource(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    // ------------------------------------------------------------------
    // Resource accessors
    // ------------------------------------------------------------------
    public AuthResource auth() { return auth; }
    public PipelinesResource pipelines() { return pipelines; }
    public AgentsResource agents() { return agents; }
    public TemplatesResource templates() { return templates; }
    public UsersResource users() { return users; }
    public EventsResource events() { return events; }
    public IQResource iq() { return iq; }
    public StreamsResource streams() { return streams; }

    // ------------------------------------------------------------------
    // Internal accessors used by EventsResource for SSE streaming. Package-
    // private — not part of the public surface.
    // ------------------------------------------------------------------
    String baseUrlInternal() { return baseUrl; }
    HttpClient httpClientInternal() { return http; }
    ObjectMapper mapperInternal() { return mapper; }
    Duration timeoutInternal() { return timeout; }

    // ------------------------------------------------------------------
    // Token management
    // ------------------------------------------------------------------
    /** The current bearer token, or {@code null} if none has been set. */
    public String getToken() { return token; }

    /** Sets the bearer token used by subsequent authenticated requests. */
    public void setToken(String token) { this.token = token; }

    // ------------------------------------------------------------------
    // Top-level endpoints
    // ------------------------------------------------------------------
    /**
     * {@code GET /api/pulse/version} — public, no JWT required. Returns the
     * Pulse server's build + version metadata.
     */
    public Map<String, Object> version() {
        return request("GET", "/api/pulse/version", null, false);
    }

    /** Closes the underlying HTTP client. After this call the instance must not be used. */
    @Override
    public void close() {
        // java.net.http.HttpClient doesn't expose close() until JDK 21,
        // and even then it's a no-op for the default pool. We keep the
        // method on the public surface for try-with-resources usage and
        // future-proofing.
    }

    // ------------------------------------------------------------------
    // Internal: request execution + error translation
    // ------------------------------------------------------------------
    Map<String, Object> request(String method, String path, Object body, boolean authenticated) {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(timeout)
                .header("User-Agent", USER_AGENT)
                .header("Accept", "application/json");

        if (authenticated) {
            if (token == null || token.isEmpty()) {
                Map<String, Object> err = Map.of(
                        "error",
                        "No token set. Call client.auth().login(...) first or pass .token(...) to the builder.");
                throw new PulseAuthException(401, path, err);
            }
            reqBuilder.header("Authorization", "Bearer " + token);
        }

        if (body == null) {
            reqBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            byte[] payload;
            try {
                payload = mapper.writeValueAsBytes(body);
            } catch (IOException e) {
                throw new PulseClientException("Failed to serialise request body for " + path, e);
            }
            reqBuilder.header("Content-Type", "application/json");
            reqBuilder.method(method, HttpRequest.BodyPublishers.ofByteArray(payload));
        }

        HttpResponse<String> response;
        try {
            response = http.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new PulseClientException("HTTP transport failure on " + method + " " + path, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulseClientException("Interrupted while calling " + method + " " + path, e);
        }

        int status = response.statusCode();
        String responseBody = response.body();

        if (status == 204) {
            return Collections.emptyMap();
        }

        if (status >= 200 && status < 300) {
            if (responseBody == null || responseBody.isEmpty()) {
                return Collections.emptyMap();
            }
            try {
                Map<String, Object> parsed = mapper.readValue(responseBody, MAP_TYPE);
                return parsed != null ? parsed : Collections.emptyMap();
            } catch (IOException e) {
                throw new PulseClientException(
                        "Failed to parse JSON response from " + method + " " + path, e);
            }
        }

        // Non-2xx — translate to a typed exception
        throw translateError(status, path, response, responseBody);
    }

    private PulseApiException translateError(int status, String path, HttpResponse<String> response,
                                              String responseBody) {
        Map<String, Object> parsedBody = null;
        if (responseBody != null && !responseBody.isEmpty()) {
            try {
                parsedBody = mapper.readValue(responseBody, MAP_TYPE);
            } catch (IOException ignored) {
                // Body was not JSON — wrap as a single "error" field so callers
                // still get something useful out of getBody().
                Map<String, Object> raw = new HashMap<>();
                raw.put("error", responseBody.length() > 200 ? responseBody.substring(0, 200) : responseBody);
                parsedBody = raw;
            }
        }

        switch (status) {
            case 401:
                return new PulseAuthException(status, path, parsedBody);
            case 404:
                return new PulseNotFoundException(status, path, parsedBody);
            case 400:
                return new PulseValidationException(status, path, parsedBody);
            case 429: {
                Integer retryAfter = null;
                if (parsedBody != null) {
                    Object v = parsedBody.get("retryAfterSeconds");
                    if (v instanceof Number n) retryAfter = n.intValue();
                }
                if (retryAfter == null) {
                    Optional<String> header = response.headers().firstValue("Retry-After");
                    if (header.isPresent()) {
                        try {
                            retryAfter = Integer.parseInt(header.get().trim());
                        } catch (NumberFormatException ignored) {
                            // not an integer (HTTP-date form) — leave as null
                        }
                    }
                }
                return new PulseRateLimitException(status, path, parsedBody, retryAfter);
            }
            default:
                return new PulseApiException(status, path, parsedBody);
        }
    }

    private static String stripTrailingSlash(String url) {
        int end = url.length();
        while (end > 1 && url.charAt(end - 1) == '/') end--;
        return url.substring(0, end);
    }

    // ------------------------------------------------------------------
    // Resource classes — one per OpenAPI tag.
    // ------------------------------------------------------------------

    /** {@code client.auth()} — authentication + session management. */
    public static final class AuthResource {
        private final PulseClient client;
        AuthResource(PulseClient client) { this.client = client; }

        /**
         * {@code POST /api/auth/login} — exchanges username + password for a JWT.
         *
         * <p>On success, the returned token is cached on the parent client so
         * subsequent calls authenticate automatically.
         */
        public Map<String, Object> login(String username, String password) {
            Map<String, Object> body = Map.of("username", username, "password", password);
            Map<String, Object> response = client.request("POST", "/api/auth/login", body, false);
            cacheToken(response);
            return response;
        }

        /** {@code POST /api/auth/refresh} — exchanges a refresh token for a fresh JWT. */
        public Map<String, Object> refresh(String refreshToken) {
            Map<String, Object> body = Map.of("refreshToken", refreshToken);
            Map<String, Object> response = client.request("POST", "/api/auth/refresh", body, false);
            cacheToken(response);
            return response;
        }

        /** {@code GET /api/auth/organizations} — orgs the current user is a member of. */
        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> organizations() {
            Map<String, Object> result = client.request("GET", "/api/auth/organizations", null, true);
            Object orgs = result.get("organizations");
            if (orgs instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
        }

        /**
         * {@code POST /api/auth/switch-org} — switches the active organisation.
         * The new JWT (with updated orgId claim) is cached on the parent client.
         */
        public Map<String, Object> switchOrg(String orgId) {
            Map<String, Object> body = Map.of("orgId", orgId);
            Map<String, Object> response = client.request("POST", "/api/auth/switch-org", body, true);
            cacheToken(response);
            return response;
        }

        private void cacheToken(Map<String, Object> response) {
            Object token = response.get("token");
            if (token instanceof String s && !s.isEmpty()) {
                client.setToken(s);
            }
        }
    }

    /** {@code client.pipelines()} — create / list / inspect / delete pipelines. */
    public static final class PipelinesResource {
        private final PulseClient client;
        PipelinesResource(PulseClient client) { this.client = client; }

        /** {@code GET /api/pulse/pipelines} — every pipeline in the current org. */
        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> list() {
            Map<String, Object> result = client.request("GET", "/api/pulse/pipelines", null, true);
            Object pipelines = result.get("pipelines");
            if (pipelines instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
        }

        /** {@code GET /api/pulse/pipelines/{id}} — one pipeline by id. */
        public Map<String, Object> get(String pipelineId) {
            return client.request("GET", "/api/pulse/pipelines/" + encode(pipelineId), null, true);
        }

        /**
         * {@code POST /api/pulse/pipelines} — creates + deploys a new pipeline.
         * The definition must follow the CreatePipelineRequest schema (see
         * openapi.yaml). At minimum: {@code name} + {@code nodes}.
         */
        public Map<String, Object> create(Map<String, Object> definition) {
            return client.request("POST", "/api/pulse/pipelines", definition, true);
        }

        /** {@code DELETE /api/pulse/pipelines/{id}} — tears down the pipeline. */
        public void delete(String pipelineId) {
            client.request("DELETE", "/api/pulse/pipelines/" + encode(pipelineId), null, true);
        }
    }

    /** {@code client.agents()} — list / get / update / delete deployed agents. */
    public static final class AgentsResource {
        private final PulseClient client;
        AgentsResource(PulseClient client) { this.client = client; }

        /** {@code GET /api/pulse/agents} — every deployed agent in the current org. */
        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> list() {
            Map<String, Object> result = client.request("GET", "/api/pulse/agents", null, true);
            Object agents = result.get("agents");
            if (agents instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
        }

        /** {@code GET /api/pulse/agents/{id}} — one agent by id. */
        public Map<String, Object> get(String agentId) {
            return client.request("GET", "/api/pulse/agents/" + encode(agentId), null, true);
        }

        /**
         * B-115 Phase 1 — {@code PUT /api/pulse/agents/{id}}: replace the agent's config.
         *
         * <p>{@code config} is the FULL agent config (not a partial merge) — at
         * minimum {@code name}; optional fields ({@code engineType},
         * {@code inputTopic}, {@code outputTopic}, {@code description},
         * {@code instances}, {@code monthlyBudget}, {@code config}) fall back
         * to safe defaults when omitted. See the {@code UpdateAgentRequest}
         * schema in {@code openapi.yaml}.
         *
         * <p>Today this triggers a full stop + persist + start cycle on the
         * engine side — the agent is briefly unavailable while the swap
         * happens. Existing state in the agent's keyed store is preserved.
         * Phase 2 (B-115-engine) will add atomic event-boundary swap so
         * hot-reloadable changes apply with no downtime.
         *
         * <p>Returns the post-update agent snapshot (same shape as {@link #get}).
         * Throws {@link com.streamflow.pulse.client.exceptions.PulseValidationException}
         * on a bad config, {@link com.streamflow.pulse.client.exceptions.PulseNotFoundException}
         * if the agent doesn't exist.
         */
        public Map<String, Object> update(String agentId, Map<String, Object> config) {
            return client.request("PUT", "/api/pulse/agents/" + encode(agentId), config, true);
        }

        /**
         * {@code DELETE /api/pulse/agents/{id}} — stop the agent + remove its
         * config row. The agent's keyed state store is also dropped. Requires
         * the {@code AGENT_DELETE} permission.
         */
        public void delete(String agentId) {
            client.request("DELETE", "/api/pulse/agents/" + encode(agentId), null, true);
        }
    }

    /** {@code client.templates()} — first-party pipeline template catalog. */
    public static final class TemplatesResource {
        private final PulseClient client;
        TemplatesResource(PulseClient client) { this.client = client; }

        /** {@code GET /api/pulse/templates} — the 223+ first-party templates. */
        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> list() {
            Map<String, Object> result = client.request("GET", "/api/pulse/templates", null, true);
            Object templates = result.get("templates");
            if (templates instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
        }
    }

    /** {@code client.users()} — user management (admin only). */
    public static final class UsersResource {
        private final PulseClient client;
        UsersResource(PulseClient client) { this.client = client; }

        /**
         * {@code GET /api/pulse/users} — every user in the current org. Requires
         * the caller to have the USERS_LIST permission (Owner / Platform Admin
         * personas by default — see B-105).
         */
        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> list() {
            Map<String, Object> result = client.request("GET", "/api/pulse/users", null, true);
            Object users = result.get("users");
            if (users instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
        }
    }

    private static String encode(String pathSegment) {
        return java.net.URLEncoder.encode(pathSegment, java.nio.charset.StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    // ------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------
    /** Fluent builder for {@link PulseClient}. */
    public static final class Builder {
        private String baseUrl;
        private String token;
        private Duration timeout;
        private HttpClient httpClient;
        private ObjectMapper mapper;

        private Builder() {}

        /** Required — the Pulse server URL (e.g. {@code http://localhost:9090}). */
        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        /** Optional — pre-minted JWT to attach as {@code Authorization: Bearer <token>}. */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /** Optional — per-request timeout. Default 30 seconds. */
        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Optional — bring-your-own {@link HttpClient} (for shared connection
         * pools, custom proxies, TLS config, etc.). If omitted, the SDK creates
         * its own.
         */
        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /** Optional — bring-your-own Jackson {@link ObjectMapper}. */
        public Builder objectMapper(ObjectMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public PulseClient build() {
            return new PulseClient(this);
        }
    }
}
