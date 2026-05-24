# com.streamflow:pulse-client — Java SDK for StreamFlow Pulse

Official Java client for [Pulse](https://github.com/olsisoft/streamflow) — the AI Agent Platform. Targets **JDK 17 LTS**, single runtime dependency (Jackson), built on `java.net.http.HttpClient` (no Apache HttpClient / OkHttp pulled in).

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
    <groupId>com.streamflow</groupId>
    <artifactId>pulse-client</artifactId>
    <version>2.6.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.streamflow:pulse-client:2.6.0'
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

## Supported surfaces (v2.6.0)

| Resource | Methods | Notes |
|---|---|---|
| `client.auth()` | `login()`, `refresh()`, `organizations()`, `switchOrg()` | Auto-caches JWT after `login` / `refresh` / `switchOrg`. |
| `client.pipelines()` | `list()`, `get(id)`, `create(definition)`, `delete(id)` | `definition` follows the `CreatePipelineRequest` schema. |
| `client.agents()` | `list()`, `get(id)` | Read-only — agents are owned by pipelines. |
| `client.templates()` | `list()` | The 223+ first-party templates. |
| `client.users()` | `list()` | Requires `USERS_LIST` permission (Owner / Platform Admin personas). |
| `client.version()` | top-level | Public — no JWT required. |

Full ~112-endpoint surface documented in Swagger UI at `<pulse-server>/api-docs`. Less-used methods land opportunistically as user-facing demand surfaces.

## Authentication

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
git clone https://github.com/olsisoft/streamflow.git
cd streamflow/pulse-java

# Run tests (JUnit 5 + WireMock, fully offline)
mvn test

# Build the jar
mvn package

# Install to local Maven repo
mvn install
```

CI runs the same on every push touching `pulse-java/` — see `.github/workflows/pulse-java.yaml`.

## Roadmap

- **v2.5.x** — current sync API, 5 core resources, `version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — event-stream consumer via `Flow.Publisher`: `client.events().stream().subscribe(subscriber)` consuming `/api/pulse/events/stream` (SSE).
- **B-098 satellite** — once `olsisoft/pulse-java` exists, this in-tree code lifts out wholesale and publishes to Maven Central via the existing OSSRH / Sonatype Central Portal flow.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.
