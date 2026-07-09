# Kafka in, AI inline, Kafka out

**Your transactions already flow through a Kafka topic. Drop StreamFlow in between
produce and consume, and every record comes out the other side scored by a real
model — with no Flink job, no model server, and no glue code.**

```
  Kafka topic "transactions"   ← your stock producer writes here, unchanged
        │
        │  ① kafka-source connector — consumes the topic (its own consumer group)
        ▼
  ② streaming agent
       ├─ WASM operator   — unwraps the record, derives the fraud features (sandboxed, no syscalls)
       └─ ONNX mlPredict  — scores the features → fraud.fraudScore
        │
        ├─ ③ kafka sink → "transactions-enriched"   ← your stock consumer reads here, unchanged
        ├─ ④ rule-based  → fraud.fraudScore > 0.8 → fraud-alerts
        │       ├─ ⑤ llm  → one-line explanation  ◀── bring your own LLM
        │       └─ ⑥ mcp  → open a case / reason over the alert
        └─ (clean) → transactions-ok
```

Every brick is wired by the **Pulse Java SDK** in one `pipelines().create(...)` call
(`Deployer.java`). The transform that turns the raw Kafka record into model features
is **one line of Rust** compiled to WebAssembly. The model is a 225-byte ONNX file.

---

## Run it

You need Docker. Pick a topology:

### Keep your Kafka (drop in beside it)

A real Apache Kafka broker holds the data; Pulse runs the StreamFlow engine
in-process (embedded) and bridges the topic in and out.

```bash
docker compose up -d
./demo.sh
```

### Replace your Kafka (one binary)

There is no separate Kafka. The StreamFlow engine **is** the broker: it speaks the
Kafka wire protocol on `:9092` (your stock clients connect here, unchanged) and the
native mesh protocol on `:9094` (Pulse connects here as a client). The pipeline
definition does not change by a single line.

```bash
docker compose -f docker-compose.mesh.yml up -d
./demo.sh
```

`demo.sh` produces five transactions with a **stock Kafka console producer**, then
consumes `transactions-enriched` with a **stock Kafka console consumer**, and shows
the inline fraud score the pipeline added to each record:

```
  t-1001  CoffeeBar  US  amount=42    fraudScore=0.076  ok
  t-1003  Grocer     US  amount=76    fraudScore=0.081  ok
  t-9001  SkyMart    FR  amount=4200  fraudScore=0.975  🚨 FRAUD
  t-9002  GiftCards  RO  amount=2750  fraudScore=0.962  🚨 FRAUD
```

---

## How it works

### ① / ③  Kafka in and out — bare topics, your clients unchanged

The `kafka-source` and `kafka` (sink) connectors are the production-grade Kafka
connectors from `streamflow-connect`, exposed to Pulse through the cluster bridge.
They consume and produce **bare Kafka topic names** against whatever broker you
point `kafka.bootstrap.servers` at — your existing cluster, or the StreamFlow node
itself. Your producers and consumers don't know StreamFlow is in the path.

The source wraps each consumed record in an envelope
`{"topic","key","payload":"<the raw record>","ts"}`; the sink produces the enriched
pipeline payload back as the Kafka record value.

### ②  The WASM transform — one line of Rust

`mlPredict` needs the features flat and numeric. The raw Kafka record is JSON nested
(and escaped) inside the source envelope. The bridge between them is a sandboxed
WebAssembly operator. The **entire** operator ABI is one line — `pulse_wasm_guest`
generates the `alloc` / `process` / memory exports the engine calls:

```rust
pulse_wasm_guest::operator!(|event: &[u8]| -> Vec<u8> { parse(event) });
```

`parse` unwraps the envelope and derives four risk signals the model wants
(`is_foreign`, `card_not_present`, `night`, `velocity_24h`) plus the amount — a small
`no_std` function over bytes (see `wasm-fraud-features/src/lib.rs`). It compiles to a
**7 KB** `wasm32` module with **zero host imports**, so the guest can compute over the
event but cannot make a single syscall. The fuel limit guards against infinite loops;
the memory cap guards against runaway growth.

Build it (a prebuilt `fraud-features.wasm` already ships, so this is optional):

```bash
cd wasm-fraud-features
rustup target add wasm32-unknown-unknown
cargo build --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/pulse_wasm_fraud_features.wasm fraud-features.wasm
```

### ③  The ONNX model — auditable, 225 bytes

`fraud-risk.onnx` is `sigmoid(W · features + b)` over the five features, in this order:

```
[ amount_f , is_foreign , card_not_present , night , velocity_24h ]
```

The categorical risk signals dominate, so a large but otherwise-normal purchase isn't
auto-flagged. The `inputSchema` order in `Deployer.java` is what packs the features
into the model's `[1,5]` tensor — keep them aligned. Regenerate (optional):

```bash
cd model
pip install onnx numpy
python3 gen_model.py
```

Swap in your own real model: keep the input/output names and feature order, drop in
the `.onnx`, done. The runtime is ONNX Runtime, so anything you can export to ONNX
(scikit-learn, XGBoost, PyTorch, TensorFlow) runs here unchanged.

### ④–⑥  Act on the score

The rule gate, the LLM explainer and the MCP case reasoner all read the **same**
`transactions-scored` / `fraud-alerts` stream. The LLM is `echo` until you point it at
a provider (Ollama or a cloud model in `pulse-config.yaml`); the MCP node uses the
built-in `demo.classifier` reasoner — swap in your case-management MCP plugin for real
account lookups.

---

## Embedded → event mesh

The two compose files are the same pipeline at two scales:

| | `docker-compose.yml` | `docker-compose.mesh.yml` |
|---|---|---|
| **Engine** | in-process inside Pulse (embedded) | a standalone StreamFlow node (the mesh) |
| **Broker** | a separate Apache Kafka | the StreamFlow node itself (Kafka wire on `:9092`) |
| **Pulse data plane** | `PULSE_DATAPLANE_MODE=embedded` | `PULSE_DATAPLANE_MODE=remote` → `streamflow:9094` |
| **You get** | one-command local / single box | Raft-replicated durability, shard-per-core scale, exactly-once |
| **Pipeline definition** | identical | identical |

Moving to the mesh is a data-plane switch, not a rewrite. The mesh owns the log
(durable, replicated) and the scale (shard-per-core); Pulse owns the agents and the
connectors. Add more `streamflow` nodes to the bootstrap list to grow it, with no
change to your pipelines.

---

## Take it to production

- **Point at your real Kafka.** Set `kafka.bootstrap.servers` (and the
  `kafka.security.protocol` / `kafka.sasl.*` fields the connectors expose) to your
  cluster in `Deployer.java`. The source commits offsets after publish (at-least-once);
  the sink supports `acks=all` and producer-side exactly-once via `kafka.transactional.id`.
- **Replace the model.** Drop in your `.onnx`; keep the feature order aligned with the
  `inputSchema`.
- **Replace the features.** Edit `parse` in the WASM operator and rebuild — it's the
  only place the record shape is interpreted.
- **Wire a real LLM and MCP plugin** for the alert branch.
- **Scale out** with `docker-compose.mesh.yml` and more engine nodes.

## Files

```
Deployer.java                 the whole pipeline, via the Pulse Java SDK (one create call)
wasm-fraud-features/          Rust no_std WASM operator (+ prebuilt fraud-features.wasm)
model/                        ONNX generator (+ prebuilt fraud-risk.onnx)
docker-compose.yml            embedded: external Kafka + in-process engine
docker-compose.mesh.yml       mesh: StreamFlow as the broker + Pulse remote
config/streamflow.properties  the engine node (Kafka wire + native protocol on)
pulse-config.yaml             turns the source/sink connector runtimes on
deployer/                     builds Deployer.java against the published pulse-client
demo.sh                       produce → consume-enriched walkthrough (stock Kafka CLI)
```
