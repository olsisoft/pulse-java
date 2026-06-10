# Predictive maintenance — WASM + ONNX + the four agent engines (Java SDK)

Industrial machines stream a vendor's **binary** sensor frame (vibration, bearing
temperature, rpm, motor current). The plant wants to **predict a failure before it
happens** and **auto-open a work order** — without shipping operational telemetry
to a cloud model server, and without re-implementing the vendor's binary decoder
in the engine.

This example does it with **one Java SDK file** (`Main.java`): a sandboxed **WASM**
module decodes the binary frame, an embedded **ONNX** model scores failure risk,
and Pulse's **four agent engines** chain the rest — all in-engine, sub-millisecond,
no external model server:

```
 machine-telemetry  (proprietary BINARY frames, little-endian, base64-wrapped)
   ┌─ (1) streaming   operators: [ WASM decode binary → JSON,
   │                               ONNX mlPredict → risk.riskScore ∈ [0,1] ] → telemetry-scored
   ├─ (2) rule-based  gate riskScore > 0.8 → escalate; default → audit trail → maintenance-alerts
   ├─ (3) llm         draft a human work order  ◀── bring your own LLM           → workorder-drafts
   ├─ (4) mcp         open the ticket in your CMMS via your tools                → workorder-created
   └─ (side) streaming  per-machine rolling health (keyBy + window)              → machine-health
```

**WASM and ONNX both run as operators *inside* the first streaming agent** — a raw
binary frame becomes a risk-scored event in one in-engine pass.

## Bring your own LLM (node 3)

The LLM node is **provider-agnostic and hot-swappable — no redeploy**. Choose it
in **Pulse → Settings → AI**:

- **Local — Ollama** (`provider: ollama`): nothing leaves the plant. The right
  default for OT / operational data.
- **Cloud — Claude / OpenAI** (`provider: anthropic` / `openai`): set the API key.
- **No provider → echo mode**: the pipeline still runs; the node passes a stub
  through so you can wire the rest first.

It's a setting, not code — switch providers and the same pipeline keeps running.

## What's verified live

Reproduced end-to-end on the real StreamFlow engine (Pulse 2.7.x, embedded engine,
no license) — feed 10 binary frames, read each hop:

```
telemetry-scored  = 10   every frame WASM-decoded + ONNX-scored
maintenance-alerts =  6   the rule gate — ONLY readings with riskScore > 0.8
workorder-drafts   =  6   the LLM drafted a work order per alert
telemetry-normal   = 10   the default rule's full audit trail
```

The ONNX model's scores (it's a tiny, auditable logistic scorer — see below):

```
 machine    vib  temp  rpm   cur   riskScore   → gated?
 MCH-0001   2.1   54  1500  12.0    0.047        no  (healthy)
 MCH-0002   2.4   57  1500  12.5    0.086        no
 MCH-0003   6.6   74  1500  17.0    0.957        YES (warning)
 MCH-0009  11.5   96  1500  22.5    0.9999       YES (failing)
 MCH-0007  → a developing bearing fault across six readings:
            0.176 → 0.594 → 0.918 → 0.992 → 0.999 → 0.9999   (escalates as it degrades)
```

| Node | Engine | Out of the box | To activate fully |
|---|---|---|---|
| 1a — Binary decode | streaming · WASM | ✅ live — sandboxed, zero syscalls | — |
| 1b — Failure-risk score | streaming · ONNX `mlPredict` | ✅ live — embedded, sub-ms | swap in your own `.onnx` (same I/O schema) |
| 2 — Risk gate | rule-based | ✅ live — `maintenance-alerts` carries only riskScore > 0.8 | tune the threshold / add severity rules |
| 3 — Work order | llm | ✅ runs (echo) → `workorder-drafts` | point it at Ollama or a cloud key |
| 4 — CMMS ticket | mcp | deploys; `blocked` w/o plugin (side branch) | install the CMMS MCP plugin (Settings → MCP Plugins) |
| side — Machine health | streaming window | deploys; event-time windowing | feed a continuous stream (watermark advances the window) |

The main path (1→2→3) needs no external dependency beyond the cluster data plane.
Node 4 and the window fan off so they never block it.

## The binary frame (30 bytes, little-endian)

```text
off  size  field            type
  0    4   magic "SFM1"     u8[4]
  4    8   machineId         ascii (space-padded)
 12    4   vibration_mm_s    f32 LE
 16    4   bearing_temp_c    f32 LE
 20    2   rpm               u16 LE
 22    4   motor_current_a   f32 LE
 26    4   timestamp         u32 LE (epoch seconds)
```

Transported **base64-wrapped** in the event value (the standard way binary
telemetry crosses an MQTT/Kafka/HTTP hop, lossless through JSON). The WASM module
base64-decodes then parses the layout into flat JSON:

```json
{"machineId":"MCH-0009","vibration":11.5,"bearingTemp":96,"rpm":1500,
 "motorCurrent":22.5,"ts":1718000000,"_source":"telemetry-binary"}
```

Those four numeric fields, in order, are what the ONNX `mlPredict` operator scores.

## The ONNX model

`model/failure-risk.onnx` is a deliberately tiny, fully-explainable scorer so the
predictions are auditable:

```
riskScore = sigmoid(W · [vibration, bearingTemp, rpm, motorCurrent] + b)   # ∈ [0,1]
```

A stock ONNX graph (`MatMul → Add → Sigmoid`, opset 13) any ONNX runtime can run —
here the Pulse engine's embedded `onnxruntime`. A prebuilt copy ships; rebuild (or
retrain with real weights) with `model/gen_model.py` (needs only `pip install onnx
numpy`). **Swap in your own model** — keep the input order
`[vibration, bearingTemp, rpm, motorCurrent]` and a single `[1,1]` output, and the
pipeline is unchanged. The `mlPredict` operator dense-packs the named features into
the model's `[1,4]` input tensor **in the upload `inputSchema` order**, so that
order must match how the model was trained.

## Run it

The WASM `wasm` operator and the ONNX `mlPredict` operator both run in the
streaming engine, so this example needs the **cluster data plane** (the real
StreamFlow engine), not the standalone "lite" bus. The simplest way is the
**embedded** engine — single process, no license:

```bash
# Pulse with the cluster-bridge jars on a bridge dir + the embedded engine on:
PULSE_BRIDGE_DIR=/path/to/bridge-libs \
PULSE_DATAPLANE_MODE=embedded \
java -jar streamflow-pulse.jar --port 9090 --headless
# boot log: Embedded ML inference ready … | Gate D — initialBackend=streamflow
```

**1. Build the SDK + classpath (with Jackson):**

```bash
cd pulse-java
mvn -q -DskipTests package
mvn -q dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt
export CP="target/classes:$(cat /tmp/cp.txt)"
```

**2. (Optional) rebuild the WASM module and/or the ONNX model.** Prebuilt copies
ship; to rebuild:

```bash
# WASM (Rust → wasm32):
rustup target add wasm32-unknown-unknown   # once
cargo build --release --target wasm32-unknown-unknown \
    --manifest-path examples/predictive-maintenance-onnx/wasm-telemetry-decoder/Cargo.toml
cp examples/predictive-maintenance-onnx/wasm-telemetry-decoder/target/wasm32-unknown-unknown/release/\
pulse_wasm_telemetry_decoder.wasm \
   examples/predictive-maintenance-onnx/wasm-telemetry-decoder/telemetry-decoder.wasm

# ONNX (Python → .onnx):
pip install onnx numpy
python3 examples/predictive-maintenance-onnx/model/gen_model.py
```

**3. Deploy, feed, read:**

```bash
javac -cp "$CP" -d /tmp/ex \
    examples/predictive-maintenance-onnx/Main.java \
    examples/predictive-maintenance-onnx/FeedAndRead.java
export PULSE_URL=http://localhost:9090 PULSE_USER=alice PULSE_PASSWORD=secret

java -cp "$CP:/tmp/ex" Main                              # upload WASM + ONNX + deploy
java -cp "$CP:/tmp/ex" FeedAndRead feed                  # publish sample BINARY frames
java -cp "$CP:/tmp/ex" FeedAndRead read telemetry-scored    # WASM-decoded + ONNX riskScore
java -cp "$CP:/tmp/ex" FeedAndRead read maintenance-alerts  # only the high-risk machines
java -cp "$CP:/tmp/ex" FeedAndRead read workorder-drafts    # the LLM work orders
```

`FeedAndRead` talks to Pulse's HTTP event API (`/api/pulse/events`), so it needs no
Kafka client — just the JDK + Jackson.

### Run on a remote cluster

To run against a separately-deployed StreamFlow node instead of the embedded
engine, point Pulse at it (`PULSE_DATAPLANE_MODE=remote
PULSE_DATAPLANE_REMOTE_BOOTSTRAP=node-host:9094 PULSE_DATAPLANE_REMOTE_NATIVE=true
PULSE_DATAPLANE_REMOTE_NATIVE_PORT=9094`). The deploy + feed steps are identical;
you can also feed the binary frames straight to the node's native protocol with
StreamFlow's native client (binary-safe — no base64 needed). See the COBOL example's
"Run on the cluster" section for the native-client recipe and the org-namespaced
topic naming (`pulse.<org>.machine-telemetry`).

## Production notes

- **Sandboxed decoder.** The WASM module imports nothing and has no syscalls — safe
  to run an untrusted/vendor binary decoder on operational data.
- **No model server.** The ONNX model runs in-engine via `mlPredict`; one session is
  cached per `(org, model)` and hot-swaps when you re-upload — no Triton/KServe hop.
- **Data residency.** Keep the LLM local (Ollama) and no telemetry content leaves
  the plant; the model + decoder are already in-process.
- **Tune the gate, not the topology.** Failure thresholds live in the rule-based
  node (`risk.riskScore > 0.8`) — ops change policy without redeploying the stream.
- **Per-operator failure** (`onFailure`: `PASS_THROUGH` / `DROP` / `EMIT_ERROR`) +
  a DLQ topic; `ordering: PRESERVE_INPUT` where per-machine order matters.
- **Credentials** (LLM key, CMMS plugin creds) live in Pulse settings/env — never in
  the pipeline definition.
- **EBCDIC / other layouts:** adjust the field offsets in the WASM `src/lib.rs`; the
  rest of the pipeline is unchanged.
