#!/usr/bin/env bash
# Guided walkthrough of the Kafka inline-AI stack.
#
#   docker compose up -d                       # (or: -f docker-compose.mesh.yml up -d)
#   ./demo.sh
#
# It waits for the pipeline to be live, produces a handful of transactions to the
# Kafka topic `transactions` with a STOCK Kafka console producer, then consumes
# `transactions-enriched` with a STOCK Kafka console consumer and shows the inline
# fraud score the pipeline added to every record. Finally it shows the alert
# branch (rule gate → LLM explanation) via the Pulse API. ~30s.
set -euo pipefail

PULSE_URL="${PULSE_URL:-http://localhost:9090}"
PULSE_USER="${PULSE_USER:-admin}"; PULSE_PASSWORD="${PULSE_PASSWORD:-admin12345}"
DC="${DC:-docker compose}"
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"      # resolves to the broker in both topologies
KT="$DC exec -T kafka-tools /opt/kafka/bin"

bold() { printf '\033[1m%s\033[0m\n' "$1"; }
dim()  { printf '\033[2m%s\033[0m\n' "$1"; }
ok()   { printf '\033[32m✓\033[0m %s\n' "$1"; }
step() { printf '\n\033[1;36m▶ %s\033[0m\n' "$1"; }

jwt() {
  curl -fsS "$PULSE_URL/api/auth/login" -H 'content-type: application/json' \
    -d "{\"username\":\"$PULSE_USER\",\"password\":\"$PULSE_PASSWORD\"}" \
    | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4
}
read_topic() {
  curl -fsS "$PULSE_URL/api/pulse/events/$1?limit=${2:-5}" -H "Authorization: Bearer $TOKEN" \
    | python3 -c '
import sys,json
for e in json.load(sys.stdin).get("events",[]):
    v=e.get("value","")
    try: v=json.dumps(json.loads(v))
    except Exception: pass
    print("   ", (v[:260]+"…") if len(v)>260 else v)' 2>/dev/null || echo "   (none)"
}

bold "Kafka inline-AI — live walkthrough"

step "1/5  Wait for the pipeline to be live"
for i in $(seq 1 60); do TOKEN="$(jwt 2>/dev/null || true)"; [ -n "${TOKEN:-}" ] && break; sleep 2; done
[ -n "${TOKEN:-}" ] || { echo "Pulse not reachable — is the stack up? ($DC up -d)"; exit 1; }
ok "Pulse is up and authenticated"

step "2/5  Produce transactions to Kafka topic 'transactions' (stock console producer)"
dim  "Three legit, two fraud (foreign + card-not-present + 3am + high velocity):"
$KT/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic transactions >/dev/null <<'TX'
{"txId":"t-1001","account":"acc-10","amount":42,"merchant":"CoffeeBar","country":"US","cardPresent":true,"hourOfDay":9,"velocity24h":2}
{"txId":"t-1002","account":"acc-10","amount":130,"merchant":"BookShop","country":"US","cardPresent":true,"hourOfDay":13,"velocity24h":1}
{"txId":"t-1003","account":"acc-22","amount":76,"merchant":"Grocer","country":"US","cardPresent":true,"hourOfDay":18,"velocity24h":3}
{"txId":"t-9001","account":"acc-10","amount":4200,"merchant":"SkyMart","country":"FR","cardPresent":false,"hourOfDay":3,"velocity24h":9}
{"txId":"t-9002","account":"acc-22","amount":2750,"merchant":"GiftCards","country":"RO","cardPresent":false,"hourOfDay":2,"velocity24h":11}
TX
ok "produced 5 transactions"

step "3/5  Consume 'transactions-enriched' — every record now carries an inline fraud score"
dim  "Stock console consumer (Kafka in → WASM features → ONNX score → Kafka out):"
$KT/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic transactions-enriched \
  --from-beginning --max-messages 5 --timeout-ms 20000 2>/dev/null \
  | python3 -c '
import sys,json
for line in sys.stdin:
    line=line.strip()
    if not line: continue
    try:
        d=json.loads(line)
        fs=(d.get("fraud") or {}).get("fraudScore")
        flag="🚨 FRAUD" if (fs is not None and fs>0.8) else "ok"
        print(f"   {d.get(\"txId\"):>7}  {str(d.get(\"merchant\")):<10} {str(d.get(\"country\")):<3} amount={d.get(\"amount\"):<5} fraudScore={fs:.3f}  {flag}")
    except Exception:
        print("   ",line[:200])' || echo "   (still in flight — re-run step 3)"

step "4/5  The alert branch — rule gate (fraudScore > 0.8) → fraud-alerts"
read_topic fraud-alerts 5

step "5/5  LLM explanation for each alert (echo until you wire a provider)"
read_topic fraud-explanations 5

printf '\n'; bold "Done — plain Kafka in, plain Kafka out, with WASM + ONNX + a rule gate inline."
dim  "No Flink job, no model server, no glue. Consume more:"
dim  "  $DC exec kafka-tools /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic transactions-enriched --from-beginning"
