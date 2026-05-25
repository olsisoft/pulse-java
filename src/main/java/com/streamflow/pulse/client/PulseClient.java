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
    private final ModelsResource models;
    private final ConnectorsResource connectors;
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
        this.models = new ModelsResource(this);
        this.connectors = new ConnectorsResource(this);
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
    public ModelsResource models() { return models; }
    /** {@code client.connectors()} — the connector catalogue (B-093 family + every native/bridged connector). */
    public ConnectorsResource connectors() { return connectors; }
    public StreamsResource streams() { return streams; }

    /**
     * B-114 — open a bidirectional duplex channel to an agent.
     *
     * <p>Returns a {@link DuplexChannel} that streams events IN and receives
     * the agent's correlated outputs OUT on a single WebSocket — the
     * synchronous-decision path (fraud, pricing, A/B assignment). The endpoint
     * runs on the Pulse WebSocket port (REST port + 1); the URL is derived from
     * the client's {@code baseUrl} with the JWT attached as a {@code token}
     * query param.
     *
     * <p>The returned channel is {@link AutoCloseable} — use try-with-resources.
     * The connection is opened (and the server's {@code connected} handshake
     * awaited) lazily on the first {@link DuplexChannel#send} / blocking call,
     * so callers can construct + configure before connecting.
     *
     * <pre>{@code
     * try (DuplexChannel ch = client.duplex("fraud-detector")) {
     *     String cid = ch.send(Map.of("amount", 5000), "tx-1");
     *     Map<String, Object> out = ch.recv();   // out.get("correlation_id") == "tx-1"
     * }
     * }</pre>
     */
    public DuplexChannel duplex(String agentId) {
        if (agentId == null || agentId.isBlank()) {
            throw new IllegalArgumentException("agentId must be a non-empty string");
        }
        String url = DuplexChannel.deriveWsUrl(baseUrl, agentId, token);
        return new DuplexChannel(url, http, mapper);
    }

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

    /**
     * Issues a {@code multipart/form-data} POST and translates errors. Used by
     * {@link ModelsResource#upload}. {@code fileFieldName} names the binary
     * part; {@code formFields} are emitted as text parts in insertion order.
     */
    Map<String, Object> requestMultipart(String method, String path, String fileFieldName,
                                         String fileName, byte[] fileBytes,
                                         Map<String, String> formFields) {
        if (token == null || token.isEmpty()) {
            Map<String, Object> err = Map.of(
                    "error",
                    "No token set. Call client.auth().login(...) first or pass .token(...) to the builder.");
            throw new PulseAuthException(401, path, err);
        }

        String boundary = "----PulseJavaBoundary" + Long.toHexString(System.nanoTime());
        byte[] payload = buildMultipartBody(boundary, fileFieldName, fileName, fileBytes, formFields);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(timeout)
                .header("User-Agent", USER_AGENT)
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .method(method, HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();

        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
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
        throw translateError(status, path, response, responseBody);
    }

    private static byte[] buildMultipartBody(String boundary, String fileFieldName, String fileName,
                                             byte[] fileBytes, Map<String, String> formFields) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        java.nio.charset.Charset utf8 = java.nio.charset.StandardCharsets.UTF_8;
        String dashBoundary = "--" + boundary + "\r\n";
        try {
            // Text fields first (insertion order preserved by LinkedHashMap).
            if (formFields != null) {
                for (Map.Entry<String, String> e : formFields.entrySet()) {
                    if (e.getValue() == null) continue;
                    out.write(dashBoundary.getBytes(utf8));
                    out.write(("Content-Disposition: form-data; name=\"" + e.getKey() + "\"\r\n\r\n")
                            .getBytes(utf8));
                    out.write(e.getValue().getBytes(utf8));
                    out.write("\r\n".getBytes(utf8));
                }
            }
            // Binary file part.
            out.write(dashBoundary.getBytes(utf8));
            out.write(("Content-Disposition: form-data; name=\"" + fileFieldName
                    + "\"; filename=\"" + fileName + "\"\r\n").getBytes(utf8));
            out.write("Content-Type: application/octet-stream\r\n\r\n".getBytes(utf8));
            out.write(fileBytes);
            out.write("\r\n".getBytes(utf8));
            // Closing boundary.
            out.write(("--" + boundary + "--\r\n").getBytes(utf8));
        } catch (IOException e) {
            // ByteArrayOutputStream never throws — guard for completeness.
            throw new PulseClientException("Failed to assemble multipart body", e);
        }
        return out.toByteArray();
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

    /**
     * {@code client.connectors()} — the connector catalogue (the B-093 analytics
     * family + every native / bridged connector), the same list the Pipeline
     * Studio palette and {@code pulse connectors list} show. Each entry is
     * {@code {subType, displayName, configFields}}; use the {@code subType} as a
     * sink/source node {@code type} in a pipeline definition deployed via
     * {@code client.pipelines().deploy(...)}. Bridged connectors appear only when
     * the enterprise bridge JAR is on the server classpath.
     */
    public static final class ConnectorsResource {
        private final PulseClient client;
        ConnectorsResource(PulseClient client) { this.client = client; }

        /** {@code GET /api/pulse/connectors} — {@code {"sources": [...], "sinks": [...]}}. */
        public Map<String, Object> list() {
            return client.request("GET", "/api/pulse/connectors", null, true);
        }

        /** Just the sink connectors. */
        public List<Map<String, Object>> sinks() { return entries("sinks"); }

        /** Just the source connectors. */
        public List<Map<String, Object>> sources() { return entries("sources"); }

        @SuppressWarnings("unchecked")
        private List<Map<String, Object>> entries(String kind) {
            Object value = list().get(kind);
            if (value instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
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

    /**
     * {@code client.models()} — B-112 embedded ML model registry.
     *
     * <p>Upload ONNX models that the streaming {@code mlPredict} operator scores
     * events against, in-process on the Pulse engine (no model-server hop).
     * Models are org-scoped; upload / delete require the ADMIN role.
     *
     * <pre>{@code
     * client.models().upload(new PulseClient.ModelsResource.UploadOptions()
     *         .name("fraud-classifier")
     *         .path(Path.of("./model.onnx"))
     *         .inputSchema(Map.of("amount", "float", "country", "string"))
     *         .outputSchema(Map.of("fraud_score", "float", "label", "string")));
     *
     * builder.fromTopic("transactions").mlPredict(new StreamBuilder.MlPredictOptions()
     *         .model("fraud-classifier")
     *         .inputFields(List.of("amount", "country"))
     *         .outputField("prediction")).toTopic("scored");
     * }</pre>
     */
    public static final class ModelsResource {
        private final PulseClient client;
        ModelsResource(PulseClient client) { this.client = client; }

        /**
         * {@code POST /api/pulse/ml-models} — upload (or replace) a model as
         * {@code multipart/form-data}: a binary {@code model} part plus text
         * fields {@code name}, {@code runtime}, and (when set) {@code inputSchema}
         * / {@code outputSchema} (each a JSON object string).
         *
         * <p>Supply the model bytes either by {@link UploadOptions#path(Path)} or
         * {@link UploadOptions#data(byte[])}. Replacing an existing name
         * hot-swaps the model with no agent restart.
         *
         * @return the persisted model metadata (name, runtime, sha256, version, …).
         */
        public Map<String, Object> upload(UploadOptions options) {
            Objects.requireNonNull(options, "options");
            if (options.name == null || options.name.isBlank()) {
                throw new IllegalArgumentException("name must be a non-empty string");
            }
            if ((options.path == null) == (options.data == null)) {
                throw new IllegalArgumentException("provide exactly one of 'path' or 'data'");
            }

            byte[] blob;
            String fileName;
            if (options.path != null) {
                try {
                    blob = java.nio.file.Files.readAllBytes(options.path);
                } catch (IOException e) {
                    throw new PulseClientException(
                            "Failed to read model file " + options.path, e);
                }
                java.nio.file.Path fn = options.path.getFileName();
                fileName = fn != null ? fn.toString() : options.name + ".onnx";
            } else {
                blob = options.data;
                fileName = options.name + ".onnx";
            }
            if (blob.length == 0) {
                throw new IllegalArgumentException("model bytes are empty");
            }

            String runtime = options.runtime != null ? options.runtime : "onnx";
            Map<String, String> form = new java.util.LinkedHashMap<>();
            form.put("name", options.name);
            form.put("runtime", runtime);
            ObjectMapper mapper = client.mapperInternal();
            try {
                if (options.inputSchema != null) {
                    form.put("inputSchema", mapper.writeValueAsString(options.inputSchema));
                }
                if (options.outputSchema != null) {
                    form.put("outputSchema", mapper.writeValueAsString(options.outputSchema));
                }
            } catch (IOException e) {
                throw new PulseClientException("Failed to serialise model schema", e);
            }

            return client.requestMultipart(
                    "POST", "/api/pulse/ml-models", "model", fileName, blob, form);
        }

        /** {@code GET /api/pulse/ml-models} — models registered for the caller's org. */
        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> list() {
            Map<String, Object> result = client.request("GET", "/api/pulse/ml-models", null, true);
            Object models = result.get("models");
            if (models instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
        }

        /** {@code GET /api/pulse/ml-models/{name}} — metadata for one model. */
        public Map<String, Object> get(String name) {
            requireModelName(name);
            return client.request("GET", "/api/pulse/ml-models/" + encode(name), null, true);
        }

        /** {@code DELETE /api/pulse/ml-models/{name}} — remove a model (ADMIN). */
        public void delete(String name) {
            requireModelName(name);
            client.request("DELETE", "/api/pulse/ml-models/" + encode(name), null, true);
        }

        private static void requireModelName(String name) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("name must be a non-empty string");
            }
        }

        /** Options for {@link #upload(UploadOptions)}. Exactly one of path / data is required. */
        public static final class UploadOptions {
            String name;
            java.nio.file.Path path;
            byte[] data;
            String runtime;
            Map<String, String> inputSchema;
            Map<String, String> outputSchema;

            /** Model name referenced by {@code mlPredict(model=...)}. Required. */
            public UploadOptions name(String name) { this.name = name; return this; }
            /** Filesystem path to the {@code .onnx} file (alternative to {@link #data}). */
            public UploadOptions path(java.nio.file.Path path) { this.path = path; return this; }
            /** Raw model bytes (alternative to {@link #path}). */
            public UploadOptions data(byte[] data) { this.data = data; return this; }
            /** Model runtime — only {@code "onnx"} is supported today. Defaults to {@code "onnx"}. */
            public UploadOptions runtime(String runtime) { this.runtime = runtime; return this; }
            /** Ordered feature-name → type map used to pack the input tensor. */
            public UploadOptions inputSchema(Map<String, String> s) { this.inputSchema = s; return this; }
            /** Output-name → type map (informational). */
            public UploadOptions outputSchema(Map<String, String> s) { this.outputSchema = s; return this; }
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
