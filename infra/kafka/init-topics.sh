#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# init-topics.sh — Create StreamFlow Kafka topics
#
# Usage (from repo root or infra/ directory):
#   ./infra/kafka/init-topics.sh
#
# Prerequisites: docker compose -f infra/docker-compose.dev.yml up -d (Kafka healthy)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.dev.yml"
BOOTSTRAP="localhost:9092"

# Wait for Kafka to be reachable (max 60 s)
echo "Waiting for Kafka at ${BOOTSTRAP}..."
for i in $(seq 1 12); do
  if docker compose -f "$COMPOSE_FILE" exec -T kafka \
       kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" &>/dev/null; then
    echo "Kafka is up."
    break
  fi
  echo "  attempt ${i}/12 — retrying in 5 s..."
  sleep 5
done

create_topic() {
  local topic="$1"
  local partitions="$2"
  echo "  Creating topic: ${topic} (${partitions} partitions, RF=1)"
  docker compose -f "$COMPOSE_FILE" exec -T kafka \
    kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
}

echo ""
echo "Creating StreamFlow Kafka topics..."
create_topic "viewer-events"      6   # keyed by streamId; high-throughput viewer events
create_topic "stream-health"      3   # encoder / CDN telemetry
create_topic "alerts"             3   # fired alert events (24 h retention in prod)
create_topic "metrics-aggregated" 3   # processor → API gateway internal topic

echo ""
echo "Topic list:"
docker compose -f "$COMPOSE_FILE" exec -T kafka \
  kafka-topics --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "Done. All StreamFlow topics created."
