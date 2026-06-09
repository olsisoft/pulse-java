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

## Use-case ladder (simplest → most complex)

A graduated 5-example ladder, all sharing ONE domain — **card-payments fraud
monitoring**. Events flow on the `card-authorizations` topic
(`{cardId, merchantId, amount, ts}`); the fraud rule is **more than 5
authorizations on one card in a 60s tumbling window**. Each example talks to a
**live Pulse at `PULSE_URL`** (default `http://localhost:9090`) and builds on the
concepts of the one before it.

| # | Class | What it shows |
|---|-------|---------------|
| 1 | [`Usecase1ConnectAndList.java`](Usecase1ConnectAndList.java) | Connectivity / hello-world — `version()`, optional login, `pipelines().list()` + `connectors().list()` |
| 2 | [`Usecase2DeployVelocityPipeline.java`](Usecase2DeployVelocityPipeline.java) | Streams DSL — `StreamBuilder` 60s tumbling-window velocity guard → `compile()` then `deploy()` |
| 3 | [`Usecase3InteractiveQuery.java`](Usecase3InteractiveQuery.java) | Interactive Query — `summary` / filtered `query` (txCount > 5) / point `get`, with caller-side `PulseRateLimitException` retry |
| 4 | [`Usecase4EventsAndReplay.java`](Usecase4EventsAndReplay.java) | Live `events().stream(...)` of fraud-alerts (cancel the future) + `events().replay(...)` of one card's state history |
| 5 | [`Usecase5SynchronousDecision.java`](Usecase5SynchronousDecision.java) | Synchronous ALLOW/DENY over `client.duplex("fraud-decider")` (B-114) — `send()` a charge, `recv()` the decision |

Build the SDK once (`mvn -q -DskipTests package` → `target/classes`), put Jackson
on the classpath, then compile + run each:

```bash
export PULSE_URL=http://localhost:9090      # your Pulse base URL
export PULSE_USER=alice PULSE_PASSWORD=secret   # or: export PULSE_TOKEN=<jwt>

# Jackson jars (adjust versions to match your local ~/.m2)
M2=$HOME/.m2/repository
JACK=$M2/com/fasterxml/jackson/core/jackson-databind/2.18.2/jackson-databind-2.18.2.jar:$M2/com/fasterxml/jackson/core/jackson-core/2.18.2/jackson-core-2.18.2.jar:$M2/com/fasterxml/jackson/core/jackson-annotations/2.18.2/jackson-annotations-2.18.2.jar

javac -cp "target/classes:$JACK" -d /tmp/ex examples/Usecase*.java

java -cp "target/classes:$JACK:/tmp/ex" Usecase1ConnectAndList
java -cp "target/classes:$JACK:/tmp/ex" Usecase2DeployVelocityPipeline
java -cp "target/classes:$JACK:/tmp/ex" Usecase3InteractiveQuery
java -cp "target/classes:$JACK:/tmp/ex" Usecase4EventsAndReplay
java -cp "target/classes:$JACK:/tmp/ex" Usecase5SynchronousDecision
```

> The ladder examples expect a reachable Pulse at `PULSE_URL`.
> [`CardFraudVelocityGuard.java`](CardFraudVelocityGuard.java) remains the
> all-in-one, **self-contained runnable demo** — it boots an in-process
> `EmbeddedPulse` so it runs with no live server and exercises every SDK surface
> in one file.
