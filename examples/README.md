# Pulse Java SDK — Examples

Five runnable examples showing how an application drives the **StreamFlow event
mesh** through Pulse. The SDK *declares* the work; Pulse runs it on the cluster
(sharded, replicated) — `app → SDK → Pulse API → bridge → mesh`.

## Use cases

| # | Class | What it shows |
|---|-------|---------------|
| 1 | [`RealtimeWindowedAggregation.java`](RealtimeWindowedAggregation.java) | Per-merchant 1-minute tumbling-window rollup (`count`/`sum`/`avg`/`max`) → topic |
| 2 | [`EventsLiveAndReplay.java`](EventsLiveAndReplay.java) | Tail the live event stream (`CompletableFuture` callback) **and** replay a key's state history |
| 3 | [`InteractiveQuery.java`](InteractiveQuery.java) | Interactive Query — `summary` / point `get` / bounded `scan` / filtered + grouped `query` |
| 4 | [`AiEnrichmentPipeline.java`](AiEnrichmentPipeline.java) | Agentic stream — LLM sentiment → `extract` structured fields → MCP CRM lookup |
| 5 | [`StreamToConnector.java`](StreamToConnector.java) | Discover sink connectors, then `filter` → sink a stream to a ClickHouse connector |

## Prerequisites

- **Java 17+** and the SDK on the classpath (Maven `com.streamflow:pulse-client`).
- A reachable **Pulse** instance — embedded mesh, or attached to a StreamFlow
  cluster (Settings → Data Plane → REMOTE).

## Run

Build the SDK once (`mvn -q -DskipTests package` → `target/classes`), then:

```bash
export PULSE_URL=http://localhost:9090      # your Pulse base URL

javac -cp target/classes -d /tmp/ex examples/*.java

java -cp target/classes:/tmp/ex RealtimeWindowedAggregation
java -cp target/classes:/tmp/ex EventsLiveAndReplay
java -cp target/classes:/tmp/ex InteractiveQuery
java -cp target/classes:/tmp/ex AiEnrichmentPipeline
java -cp target/classes:/tmp/ex StreamToConnector
```

Add `.token(System.getenv("PULSE_TOKEN"))` to the client builder if your Pulse
requires auth.
