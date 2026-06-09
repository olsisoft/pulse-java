# io.streamflowmesh:pulse-client — Java SDK for StreamFlow Pulse

Official Java client for [Pulse](https://github.com/olsisoft/pulse-java) — the AI Agent Platform. Targets **JDK 17 LTS**, single runtime dependency (Jackson), built on `java.net.http.HttpClient` (no Apache HttpClient / OkHttp pulled in).

```java
PulseClient client = PulseClient.builder()
        .baseUrl("http://localhost:9090")
        .build();

client.auth().login("alice", "secret");

for (Map<String, Object> pipeline : client.pipelines().list()) {
    System.out.println(pipeline.get("name"));
}
```

## Install

### Maven

```xml
<dependency>
    <groupId>io.streamflowmesh</groupId>
    <artifactId>pulse-client</artifactId>
    <version>2.6.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.streamflowmesh:pulse-client:2.6.0'
```

Requires **JDK 17 or higher**. Only runtime dep is Jackson 2.18+ — already on every Spring Boot / Quarkus / Micronaut / Helidon classpath.

## Why pulse-client (Java)

- **Tiny footprint** — `java.net.http.HttpClient` (JDK built-in) + Jackson. No transitive bloat.
- **Sibling parity** — same surface + naming as the Python (`pulse-py`) and JavaScript (`@olsisoft/pulse-client`) SDKs.
- **Spec-aligned** — every method corresponds 1:1 to an endpoint in the [Pulse OpenAPI 3.1 spec](../streamflow-pulse/src/main/resources/openapi/openapi.yaml). Drift caught at PR time by the in-tree spec invariant tests (B-103).
- **Builder + thread-safe** — a single `PulseClient` can be shared across an entire Spring Boot app; the internal `HttpClient` pools connections.

## Quick start

```java
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseAuthException;
import com.streamflow.pulse.client.exceptions.PulseNotFoundException;
import com.streamflow.pulse.client.exceptions.PulseRateLimitException;

PulseClient client = PulseClient.builder()
        .baseUrl(System.getenv("PULSE_URL"))
        .build();

// Login (auto-caches JWT)
try {
    client.auth().login(System.getenv("PULSE_USER"), System.getenv("PULSE_PASSWORD"));
} catch (PulseAuthException e) {
    throw new IllegalStateException("Bad credentials", e);
}

// List + inspect
for (Map<String, Object> p : client.pipelines().list()) {
    System.out.printf("%s — %s%n", p.get("name"), p.get("status"));
}

// Create from a template
Map<String, Object> newPipeline = client.pipelines().create(Map.of(
        "name", "my-fraud-detector",
        "templateId", "fintech-fraud-detection-realtime",
        "nodes", List.of(
                Map.of("id", "src",  "type", "source", "subType", "kafka-source"),
                Map.of("id", "agt",  "type", "agent",  "subType", "streaming"),
                Map.of("id", "sink", "type", "sink",   "subType", "telegram"))));
```

## Supported surfaces (v2.7.x)

| Resource | Methods | Notes |
|---|---|---|
| `client.auth()` | `login()`, `refresh()`, `organizations()`, `switchOrg()` | Auto-caches JWT after `login` / `refresh` / `switchOrg`. |
| `client.pipelines()` | `list()`, `get(id)`, `create(definition)`, `delete(id)` | `definition` follows the `CreatePipelineRequest` schema. |
| `client.agents()` | `list()`, `get(id)` | Read-only — agents are owned by pipelines. |
| `client.templates()` | `list()` | The 223+ first-party templates. |
| `client.users()` | `list()` | Requires `USERS_LIST` permission (Owner / Platform Admin personas). |
| `client.version()` | top-level | Public — no JWT required. |

Full ~112-endpoint surface documented in Swagger UI at `<pulse-server>/api-docs`. Less-used methods land opportunistically as user-facing demand surfaces.

## Embedded ML inference & duplex

Score events with an uploaded ONNX model in-process (B-112), and open a
bidirectional duplex channel for synchronous decisions (B-114). Full guide:
[ML inference & duplex](https://github.com/olsisoft/pulse-java/blob/dev/docs/SDK-ML-INFERENCE-AND-DUPLEX.md).

```java
// Upload + score with an ONNX model (no model-server hop)
client.models().upload(new PulseClient.ModelsResource.UploadOptions()
        .name("fraud").path(Path.of("./fraud.onnx"))
        .inputSchema(Map.of("amount", "float", "country", "float")));
builder.fromTopic("transactions")
    .mlPredict(new StreamBuilder.MlPredictOptions()
        .model("fraud").inputFields(List.of("amount", "country")).outputField("prediction"))
    .filter("prediction.fraud_score > 0.8").toTopic("flagged");

// Duplex: one connection, send in / receive the correlated output (built-in WebSocket)
try (DuplexChannel ch = client.duplex("fraud-detector")) {
    String cid = ch.send(Map.of("amount", 5000), "tx-1");
    Map<String, Object> signal = ch.recv();   // blocks for the correlated output
}
```

## Sandboxed WASM operators

Upload a WebAssembly module and run it over each event, sandboxed in pure-Java
Chicory on the engine (B-110) — no host syscalls, bounded linear memory. Any
`wasm32` toolchain (Rust, TinyGo, AssemblyScript, C) can author a module
against the alloc/process ABI.

```java
// Upload a module (validated: must parse, import no host functions, export alloc/process/memory)
client.wasm().upload(new PulseClient.WasmResource.UploadOptions()
        .name("pii-redactor").path(Path.of("./redactor.wasm"))
        .description("strip PII from the payload"));

builder.fromTopic("events")
    .wasm(new StreamBuilder.WasmOptions().module("pii-redactor"))
    .toTopic("clean");
```

## Authentication

### Where credentials come from

The SDK authenticates as a **Pulse user** — there are no separate API keys to
provision. A username + password (or a JWT minted from them) is all you need,
and they live in **your own Pulse instance**, not on streamflowmesh.io.

1. **First run → bootstrap admin.** The very first account is created either by
   the first-run screen of the Pulse web/desktop app, or by a single
   *unauthenticated* `POST /api/auth/register` with a `{"username","password"}`
   body **while no user exists yet**. That first user is granted **ADMIN**. As
   soon as any user exists, `/api/auth/register` locks down and requires an admin
   JWT — so the open bootstrap can only ever mint the very first account.
2. **Additional users.** An admin creates more accounts from **Settings → Users**
   in the Pulse UI (or an admin-authenticated `register` call). Give each CI job
   or service integration its own dedicated user rather than sharing the admin.
3. **Exchange for a token.** `auth().login(user, pass)` returns a short-lived
   **access JWT** (~1 h TTL) plus a **refresh token**; the client caches the
   access token automatically. In CI, either call `login` at startup, or pass a
   pre-minted JWT (pattern 2 below) and refresh it before it expires.

`baseUrl(...)` points at *your* Pulse server — `http://localhost:9090` for a
local `pulse --headless` or desktop install, or your deployed Pulse URL.

### Passing the token to the client

```java
// 1. Username + password (interactive / CLI tools)
PulseClient client = PulseClient.builder().baseUrl("http://localhost:9090").build();
client.auth().login("alice", "secret");

// 2. Pre-minted JWT (CI / service accounts)
PulseClient client = PulseClient.builder()
        .baseUrl("http://localhost:9090")
        .token(System.getenv("PULSE_JWT"))
        .build();

// 3. Long-running daemon with refresh-token rotation
client.auth().refresh(persistedRefreshToken);   // automatically caches the new JWT
```

## Error handling

```java
import com.streamflow.pulse.client.exceptions.*;

try {
    client.pipelines().get("nope");
} catch (PulseNotFoundException e) {
    log.info("Doesn't exist — fine");
} catch (PulseRateLimitException e) {
    Thread.sleep(e.retryAfterSeconds().orElse(60) * 1000L);
    // retry
} catch (PulseClientException e) {
    log.error("Pulse call failed: {}", e.getMessage(), e);
}
```

Every `PulseApiException` carries `statusCode()`, `path()`, and `body()` so log lines + bug reports are actionable.

## Custom HTTP client (proxies, TLS, shared pools)

The builder accepts a bring-your-own `HttpClient` for shared connection pools, custom proxies, mTLS config, etc.:

```java
HttpClient shared = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .proxy(ProxySelector.of(new InetSocketAddress("proxy.acme.com", 3128)))
        .build();

PulseClient client = PulseClient.builder()
        .baseUrl("http://pulse.acme.com")
        .httpClient(shared)
        .build();
```

## Development

```bash
git clone https://github.com/olsisoft/pulse-java.git
cd pulse-java

# Run tests (JUnit 5 + WireMock, fully offline)
mvn test

# Build the jar
mvn package

# Install to local Maven repo
mvn install
```

CI runs the same on every push touching `pulse-java/` — see `.github/workflows/pulse-java.yaml`.

## Automatic retry (opt-in)

Off by default — one attempt per request. Enable bounded, full-jitter
exponential-backoff retries on the builder:

```java
PulseClient client = PulseClient.builder()
        .baseUrl("http://localhost:9090")
        .maxRetries(3) // 0 = off (default)
        .build();
```

429 (rate limited) is retried for any method, honouring `Retry-After`;
`retryOnStatus` 5xx (default `502/503/504`) and transport errors are retried only
for idempotent methods (GET/HEAD/PUT/DELETE) unless `retryNonIdempotent(true)`;
terminal 4xx are never retried.

## Local pipeline simulation (Python-only today)

The streams DSL is **client-side declaration, server-side execution**:
`streams().compile(builder)` builds the pipeline JSON locally (no network) and
`streams().deploy(builder)` runs it on the Pulse engine. This SDK has **no
in-process simulator** — to validate a pipeline before deploy, `compile()` and
inspect the JSON, or deploy to a dev Pulse.

> A local `TopologyTestDriver`-style executor that runs a streams pipeline
> in-process over sample events (`StreamBuilder.simulate(events)`) currently
> exists **only in the Python SDK** (`streamflow-pulse-client`). Cross-language
> parity is tracked as **B-169** (issue #311); until then, local simulation is a
> Python-exclusive capability.

## Roadmap

- **v2.5.x** — current sync API, 5 core resources, `version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — event-stream consumer via `Flow.Publisher`: `client.events().stream().subscribe(subscriber)` consuming `/api/pulse/events/stream` (SSE).
- **B-098 satellite** — once `olsisoft/pulse-java` exists, this in-tree code lifts out wholesale and publishes to Maven Central via the existing OSSRH / Sonatype Central Portal flow.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.
