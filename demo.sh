#!/usr/bin/env bash
# demo.sh — end-to-end demo of the kafka-adapter-telemetry pipeline.
#
# Preconditions: `docker compose up -d --wait` already ran (Kafka + Oracle up),
# and the jars were packaged (`mvn -q package`). The script starts the two
# services, runs the simulation profiles and prints the SQL counts that prove
# idempotency and alerting. It does not bring infrastructure up or tear it down.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATEWAY_JAR="$REPO_ROOT/adapter-gateway/target/adapter-gateway-0.1.0-SNAPSHOT.jar"
HUB_JAR="$REPO_ROOT/telemetry-hub/target/telemetry-hub-0.1.0-SNAPSHOT.jar"
KAFKA_CONTAINER="telemetry-kafka"
ORACLE_CONTAINER="telemetry-oracle"
SQLPLUS="sqlplus -s telemetry/telemetry@//localhost:1521/telemetry"
GATEWAY_URL="http://localhost:8081"
HUB_URL="http://localhost:8082"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

section() { echo; echo "=== $* ==="; }

container_healthy() {
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" = "healthy" ]
}

wait_container() {
  local name="$1" i
  for i in $(seq 1 60); do
    container_healthy "$name" && return 0
    sleep 2
  done
  echo "ERROR: container $name is not healthy" >&2
  return 1
}

wait_http() {
  local url="$1" name="$2" i
  for i in $(seq 1 60); do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo "$name is UP"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: $name did not come up at $url" >&2
  return 1
}

sql_count() {
  docker exec -i "$ORACLE_CONTAINER" $SQLPLUS <<SQL | awk 'NF {print $1; exit}'
SET HEADING OFF
SET FEEDBACK OFF
SET PAGESIZE 0
SELECT COUNT(*) FROM $1;
EXIT
SQL
}

# Wait until the hub has drained the topic (event count stable for 3 reads).
wait_events_stable() {
  local prev cur stable=0
  prev="$(sql_count telemetry_event)"
  while [ "$stable" -lt 3 ]; do
    sleep 2
    cur="$(sql_count telemetry_event)"
    if [ "$cur" = "$prev" ]; then
      stable=$((stable + 1))
    else
      stable=0
      prev="$cur"
    fi
  done
}

simulate() {
  local profile="$1"
  echo "-- POST /api/v1/telemetry/simulate?profile=$profile"
  curl -sf -X POST "$GATEWAY_URL/api/v1/telemetry/simulate?profile=$profile"
  echo
}

section "Preconditions"
wait_container "$KAFKA_CONTAINER"
wait_container "$ORACLE_CONTAINER"
[ -f "$GATEWAY_JAR" ] || { echo "ERROR: $GATEWAY_JAR not found. Run 'mvn -q package' first." >&2; exit 1; }
[ -f "$HUB_JAR" ] || { echo "ERROR: $HUB_JAR not found. Run 'mvn -q package' first." >&2; exit 1; }
echo "Kafka and Oracle healthy, jars present."

section "Dashboard build"
if [ ! -f "$REPO_ROOT/dashboard/dist/index.html" ]; then
  if command -v npm >/dev/null 2>&1; then
    (cd "$REPO_ROOT/dashboard" && npm ci && npm run build)
  else
    echo "WARNING: npm not found; the dashboard will not be served" >&2
  fi
fi

section "Starting services"
java -jar "$GATEWAY_JAR" > /tmp/adapter-gateway.log 2>&1 &
PIDS+=($!)
java -jar "$HUB_JAR" > /tmp/telemetry-hub.log 2>&1 &
PIDS+=($!)
wait_http "$HUB_URL/actuator/health" "telemetry-hub (8082)"
wait_http "$GATEWAY_URL/actuator/health" "adapter-gateway (8081)"

section "Profile low — clean end-to-end flow (20 events)"
simulate low
wait_events_stable

section "Adapters as seen by the hub (GET /api/v1/adapters)"
curl -sf "$HUB_URL/api/v1/adapters" | python3 -m json.tool 2>/dev/null || curl -sf "$HUB_URL/api/v1/adapters"
echo

section "Profile high — 3 consecutive DOWN -> exactly 1 alert per adapter (500 events)"
simulate high
wait_events_stable

section "Profile overload — duplicates + corrupt JSON -> idempotency + DLT (2000 events)"
simulate overload
wait_events_stable
COUNT_RUN1="$(sql_count telemetry_event)"
echo "telemetry_event after 1st overload run: $COUNT_RUN1"

section "Re-running overload — deterministic generator, same event ids: count must NOT change"
simulate overload
wait_events_stable
COUNT_RUN2="$(sql_count telemetry_event)"
echo "telemetry_event after 2nd overload run: $COUNT_RUN2"
if [ "$COUNT_RUN1" != "$COUNT_RUN2" ]; then
  echo "FAIL: event count changed ($COUNT_RUN1 -> $COUNT_RUN2): duplicates were persisted" >&2
  exit 1
fi
echo "OK: idempotency — count unchanged across overload re-run."

section "Final SQL counts"
echo "telemetry_event: $(sql_count telemetry_event)"
echo "adapter_alert:   $(sql_count adapter_alert)  (exactly 1 per adapter per DOWN streak)"

section "Done"
echo "Corrupt JSON lands in the DLT topic; verify with:"
echo "  docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-console-consumer.sh \\"
echo "    --bootstrap-server localhost:9092 --topic adapter.telemetry.v1.dlt --from-beginning --timeout-ms 5000"
