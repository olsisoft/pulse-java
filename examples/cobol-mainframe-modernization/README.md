# COBOL mainframe modernization — a multi-engine pipeline (Java SDK)

A legacy mainframe emits **fixed-width COBOL copybook** records. Nobody wants to
rewrite the parser, the data must enter a modern real-time flow, and — being
regulated/sensitive — ideally **nothing leaves the box**.

This example wraps the COBOL parser as a **sandboxed WebAssembly module** (zero
host syscalls) and chains Pulse's agent engines by topic, then lands the result
in a **real SQL table** via a connector — all deployed with the **Java SDK** in
`Main.java`:

```
 your mainframe bridge ─▶ legacy-cobol  (fixed-width records)
                              │
 (1) streaming + WASM   COBOL copybook → JSON                 → cobol-parsed
 (2) rule-based         deterministic business gate           → cobol-ruled
 (3) llm                classify the free-text MEMO            → modernized-events   ◀── bring your own LLM
        side branches off the parsed / ruled stream:
 (4) mcp                enrich via your MCP tools (CRM, …)     → cobol-enriched-mcp
 (5) streaming window   per-account velocity (keyBy + window)  → account-velocity
 (6) sink (jdbc)        modernized records → queryable SQL     → H2 / Postgres / …
```

You feed `legacy-cobol`; the COBOL parser, the business rules, the LLM, and the
SQL sink all run in between. A 60-year-old fixed-width feed becomes **rows you
can `SELECT`** — with no bespoke JDBC code.

## Bring your own LLM (node 3)

The LLM node is **provider-agnostic and hot-swappable — no redeploy**. Choose it
in **Pulse → Settings → AI**:

- **Local — Ollama** (`provider: ollama`): nothing leaves the machine. The right
  default for sensitive mainframe data.
- **Cloud — Claude / OpenAI** (`provider: anthropic` / `openai`): set the API key.
- **No provider → echo mode**: the pipeline still runs; the node passes a stub
  through so you can wire the rest first.

It's a setting, not code — switch providers and the same pipeline keeps running.

## What's verified live

Two ways to run this, and what each proves:

| Run mode | Data plane | Verified end-to-end |
|---|---|---|
| **Standalone Pulse** (FREE) | in-JVM "lite" bus | nodes 1→3 (WASM parse → rules → LLM → `modernized-events`) |
| **Cluster** (embedded/remote engine) | the real StreamFlow engine | **all of the above + the JDBC sink writing real SQL rows + native-client ingest** |

On the cluster the full chain was reproduced (Pulse 2.7.x, embedded engine, no
license): 10 fixed-width COBOL records → WASM parse → rules → **10 rows in H2**:

```
ROW COUNT = 10
 accountId  | txnCode | amount   | productCode | memo
 ACC0000001 | DR      |  1500.75 | LOAN        | Monthly loan repayment branch 22
 ACC0000003 | DR      | 99999.00 | WIRE        | Outbound wire to vendor ACME
 ACC0000007 | DR      |    50.00 | CARD        | POS purchase 1 … 6
 …
 -- per-account velocity, straight from SQL:
 ACC0000007 : 6 txns, total 300.00      ← the burst, now a GROUP BY
```

| Node | Engine | Standalone | Cluster |
|---|---|---|---|
| 1 — COBOL parse | streaming + WASM | ✅ live | ✅ live |
| 2 — Business rules | rule-based | ✅ live | ✅ live |
| 3 — Memo classifier | llm | ✅ runs (echo) → `modernized-events` | ✅ + point at Ollama / a cloud key |
| 4 — MCP enrichment | mcp | deploys; `blocked` w/o plugin (side branch) | install the MCP plugin to activate |
| 5 — Account velocity | streaming window | deploys | event-time window; emits once a continuous stream advances the watermark (the SQL `GROUP BY` above gives the same velocity instantly) |
| 6 — JDBC sink | sink connector | not mounted (no bridge) → reported as failed component | ✅ **live — 10 rows in H2** |

The **main path (1→2→3) works standalone with no external dependency.** Nodes 4–6
fan off the stream so they never block it; node 6 needs the cluster data plane.

## The COBOL copybook

```text
01 TXN-RECORD.                            (80-column ASCII record)
   05 ACCT-ID       PIC X(10).   pos  1-10
   05 TXN-CODE      PIC X(02).   pos 11-12
   05 AMOUNT        PIC 9(10)V99. pos 13-24   (12 digits; last 2 = cents)
   05 PRODUCT-CODE  PIC X(04).   pos 25-28
   05 CURRENCY      PIC X(03).   pos 29-31
   05 POST-DATE     PIC X(08).   pos 32-39   (YYYYMMDD)
   05 MEMO          PIC X(40).   pos 40-79
   05 FILLER        PIC X(01).   pos 80
```

The WASM module turns one such record into, e.g.:

```json
{"accountId":"ACC0000003","txnCode":"DR","amount":99999.00,"productCode":"WIRE",
 "currency":"USD","postDate":"2026-06-09","memo":"Outbound wire to vendor ACME",
 "_source":"cobol-copybook"}
```

## Run it (standalone)

**0. A reachable Pulse** at `PULSE_URL` (default `http://localhost:9090`) and an
admin account (`PULSE_USER`/`PULSE_PASSWORD`, or a `PULSE_TOKEN`).

**1. Build the SDK** (once) and set a classpath that includes Jackson:

```bash
cd pulse-java
mvn -q -DskipTests package                 # → target/classes
mvn -q dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt
export CP="target/classes:$(cat /tmp/cp.txt)"
```

**2. (Optional) rebuild the WASM module.** A prebuilt copy ships at
`wasm-cobol-parser/cobol-copybook-parser.wasm`; to rebuild from Rust:

```bash
rustup target add wasm32-unknown-unknown   # once
cargo build --release --target wasm32-unknown-unknown \
    --manifest-path examples/cobol-mainframe-modernization/wasm-cobol-parser/Cargo.toml
cp examples/cobol-mainframe-modernization/wasm-cobol-parser/target/wasm32-unknown-unknown/release/\
pulse_wasm_cobol_parser.wasm \
   examples/cobol-mainframe-modernization/wasm-cobol-parser/cobol-copybook-parser.wasm
```

**3. Deploy + feed, then read — one program:**

```bash
# Main uses the pulse-java SDK on $CP. (NativeFeeder is built separately in
# "Run on the cluster" — it needs the engine SDK, not $CP.)
javac -cp "$CP" -d /tmp/ex examples/cobol-mainframe-modernization/Main.java
export PULSE_URL=http://localhost:9090 PULSE_USER=alice PULSE_PASSWORD=secret

java -cp "$CP:/tmp/ex" Main                          # deploy the pipeline AND feed it
java -cp "$CP:/tmp/ex" Main read cobol-parsed        # see the WASM-parsed JSON
java -cp "$CP:/tmp/ex" Main read modernized-events   # see the wasm→rule→llm output
# …or query the sink table `modernized_events` in your database.
```

`Main` deploys the pipeline and then **produces records into it with the SDK**
(`client.events().publish("legacy-cobol", record)`); from the input topic on, the
engine runs every stage to the SQL sink. `client.events().read(topic, limit)`
reads any hop back — both go through Pulse's HTTP event API, so there's no Kafka
client to set up.

## Run on the cluster (the JDBC sink + native ingest)

The JDBC sink (node 6) and the native client need the **real StreamFlow engine**,
not the standalone "lite" bus. Two topologies:

### A. Embedded engine (single process, no license)

Pulse runs the engine in-JVM. Put the cluster-bridge jars (the bridge +
`streamflow-core` / `streamflow-sdk` / `streamflow-agent-runtime` + an H2 driver)
in a directory and point Pulse at it:

```bash
PULSE_BRIDGE_DIR=/path/to/bridge-libs \
PULSE_DATAPLANE_MODE=embedded \
java -jar streamflow-pulse.jar --port 9090 --headless
# boot log shows: Gate D — DataPlaneRuntime installed (initialBackend=streamflow)
```

Then deploy + feed exactly as above. The sink auto-creates `modernized_events`
and every parsed record becomes a row. Inspect it with any H2 client
(`AUTO_SERVER=TRUE` lets you query while Pulse holds the file):

```bash
# jdbc:h2:file:/tmp/cobol-modernized;AUTO_SERVER=TRUE  (user sa, no password)
SELECT "accountId", COUNT(*), SUM("amount") FROM "modernized_events" GROUP BY "accountId";
```

The sink node in `Main.java` is just:

```java
sink("Modernized SQL Store (H2)", "jdbc", Map.of(
        "inputTopic", "cobol-parsed",
        "jdbc.url", "jdbc:h2:file:/tmp/cobol-modernized;AUTO_SERVER=TRUE",
        "jdbc.sink.table", "modernized_events",
        "jdbc.sink.insert.mode", "INSERT",
        "jdbc.sink.auto.create", "true"));
```

**Point it at production by changing one line** — `jdbc.url` to Postgres / MySQL /
Snowflake (same `jdbc` connector, mounted by the bridge). No code change.

### B. Remote engine + native client (`NativeFeeder`) — the real cluster

Here Pulse is a **client of a separately-deployed StreamFlow node** (not in-JVM).
The node exposes the **native binary protocol** on `:9094` — the production
ingest path (millions of events/s, not Kafka, not HTTP). Point Pulse at the node
and feed it with the native client:

```bash
# Pulse → the remote cluster's data plane (native transport):
PULSE_DATAPLANE_MODE=remote \
PULSE_DATAPLANE_REMOTE_BOOTSTRAP=node-host:9094 \
PULSE_DATAPLANE_REMOTE_NATIVE=true PULSE_DATAPLANE_REMOTE_NATIVE_PORT=9094 \
PULSE_BRIDGE_DIR=/path/to/bridge-libs \
java -jar streamflow-pulse.jar --port 9090 --headless
# boot log: EnterpriseDataPlaneTransport active path=remote-native ready

# Feed over the native client (engine SDK, not the pulse-java SDK).
# IMPORTANT: Pulse namespaces pipeline topics by org on the mesh, so target the
# physical topic pulse.<orgId>.legacy-cobol (single-tenant default org below):
NATIVE_CP="$(find . -name 'streamflow-sdk-*.jar' -o -name 'streamflow-core-*.jar' \
    -o -name 'streamflow-common-*.jar' | grep -v sources | tr '\n' ':')"
javac -cp "$NATIVE_CP" -d /tmp/exn examples/cobol-mainframe-modernization/NativeFeeder.java
STREAMFLOW_TOPIC=pulse.00000000-0000-0000-0000-000000000001.legacy-cobol \
java --enable-native-access=ALL-UNNAMED -cp "$NATIVE_CP:/tmp/exn" NativeFeeder node-host 9094
```

`NativeFeeder` uses `StreamFlowClient.nativeClient(host, 9094, …)` and keys each
record by account id, so a hot account's records stay ordered on one partition.
Pulse then consumes that topic, runs the pipeline, and the H2 sink fills exactly
as in topology A.

**Verified live** (single engine node + Pulse `remote-native`): native feed of 10
records → `pulse.<org>.legacy-cobol` → WASM parse → JDBC sink → **rows in H2**,
with `ACC0000007`'s 6 records ordered on one partition (velocity burst intact:
`6 txns, total 300.00`). Without `STREAMFLOW_TOPIC` it produces to bare
`legacy-cobol` — handy for a standalone engine you consume directly with the
native CLI consumer, but Pulse (which listens on the namespaced topic) won't see
it.

## Production notes

- **Sandboxed parser.** The WASM module imports nothing and has no syscalls —
  safe to run untrusted/legacy parser logic on sensitive records.
- **Data residency.** Keep the LLM local (Ollama) and no record content leaves
  the host.
- **Per-operator failure** (`onFailure`: `PASS_THROUGH` / `DROP` / `EMIT_ERROR`)
  + a DLQ topic; `ordering: PRESERVE_INPUT` where order matters.
- **Sink durability.** The `jdbc` connector commits per batch and checkpoints the
  offset; on a real DB use `jdbc.sink.insert.mode=UPSERT` with `jdbc.sink.pk.fields`
  for idempotent re-delivery.
- **Credentials** (LLM key, DB password, MCP plugin creds) live in Pulse
  settings/env — never in the pipeline definition.
- **EBCDIC**: this demo assumes ASCII fixed-width. For EBCDIC mainframe data,
  do the EBCDIC→ASCII transcode inside the same WASM guest before field slicing.
