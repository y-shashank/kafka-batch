#!/usr/bin/env bash
# ==============================================================================
# create_kafka_topics.sh
#
# Creates all KafkaBatch Kafka topics.  Idempotent — topics that already exist
# are skipped rather than errored.  Mirrors the partition layout used by
# KafkaBatch::Topics.specs (lib/kafka_batch/topics.rb).
#
# USAGE
#   ./bin/create_kafka_topics.sh
#
# ENVIRONMENT VARIABLES
#   KAFKA_BROKERS           Comma-separated broker list (default: localhost:9092)
#   KAFKA_PREFIX            Topic name prefix, e.g. "myapp" → "myapp.kafka_batch.jobs"
#                           Leave empty for no prefix (default: empty).
#   PARTITIONS              Override partition count for EVERY topic (default: per-topic)
#   REPLICATION_FACTOR      Replication factor for every topic   (default: 1)
#   KAFKA_TOPICS_CMD        Path to kafka-topics.sh or 'rpk topic'
#                           (default: kafka-topics.sh, falls back to rpk)
#   INCLUDE_FAIRNESS        Set to "true" to create fairness ingest/ready topics
#                           (default: true — creates them so they're ready when needed)
#   BOOTSTRAP_EXTRA_ARGS    Extra args forwarded to every kafka-topics.sh call,
#                           e.g. "--command-config /etc/kafka/client.properties"
# ==============================================================================

set -euo pipefail

# ── Config from env ─────────────────────────────────────────────────────────

BROKERS="${KAFKA_BROKERS:-localhost:9092}"
RAW_PREFIX="${KAFKA_PREFIX:-}"
PREFIX="${RAW_PREFIX:+${RAW_PREFIX}.}"     # "myapp" → "myapp."; empty → ""
RF="${REPLICATION_FACTOR:-1}"
INCLUDE_FAIRNESS="${INCLUDE_FAIRNESS:-true}"
EXTRA="${BOOTSTRAP_EXTRA_ARGS:-}"

# Detect Kafka CLI: kafka-topics.sh preferred, fall back to rpk.
if [ -n "${KAFKA_TOPICS_CMD:-}" ]; then
  KAFKA_CMD="${KAFKA_TOPICS_CMD}"
elif command -v kafka-topics.sh &>/dev/null; then
  KAFKA_CMD="kafka-topics.sh"
elif command -v kafka-topics &>/dev/null; then
  KAFKA_CMD="kafka-topics"
elif command -v rpk &>/dev/null; then
  KAFKA_CMD="rpk topic"
else
  echo "ERROR: no Kafka CLI found (kafka-topics.sh, kafka-topics, or rpk)."
  echo "       Set KAFKA_TOPICS_CMD to the correct path/command."
  exit 1
fi

# ── Topic definitions ────────────────────────────────────────────────────────
# Format: "topic_name:partitions"  — mirrors KafkaBatch::Topics::DEFAULT_PARTITIONS
# Sized for ~150 pods × concurrency 10. Tune execution topics to (pods × concurrency)
# before first deploy; Kafka cannot shrink partitions later.

TOPICS=(
  # ── Core control-plane ────────────────────────────────────────────────────
  "${PREFIX}kafka_batch.jobs:768"
  "${PREFIX}kafka_batch.events:48"
  "${PREFIX}kafka_batch.callbacks:6"
  "${PREFIX}kafka_batch.dead_letter:3"

  # ── Tiered retry topics ───────────────────────────────────────────────────
  "${PREFIX}kafka_batch.jobs.retry.short:12"
  "${PREFIX}kafka_batch.jobs.retry.medium:12"
  "${PREFIX}kafka_batch.jobs.retry.large:12"

  # ── Priority queue topics (fast/slow × p0/p1) ─────────────────────────────
  "${PREFIX}kafka_batch.jobs.fast_p0:768"
  "${PREFIX}kafka_batch.jobs.fast_p1:768"
  "${PREFIX}kafka_batch.jobs.slow_p0:768"
  "${PREFIX}kafka_batch.jobs.slow_p1:768"

  # ── Delayed jobs (perform_in / perform_at) durable payload store ───────────
  "${PREFIX}kafka_batch.scheduled:48"
)

# Fairness lanes (time + throughput). A worker picks one via `fairness_type`;
# both lanes run at once, each with its own ingest → ready topics. Ingest topics
# need many partitions (≈ max concurrent tenants).
if [ "${INCLUDE_FAIRNESS}" = "true" ]; then
  TOPICS+=(
    "${PREFIX}kafka_batch.fair_time_ingest:64"
    "${PREFIX}kafka_batch.fair_time_ready:768"
    "${PREFIX}kafka_batch.fair_throughput_ingest:64"
    "${PREFIX}kafka_batch.fair_throughput_ready:768"
  )
fi

# ── Helpers ──────────────────────────────────────────────────────────────────

GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; NC='\033[0m'

info()    { echo -e "${GREEN}[create]${NC}  $*"; }
skip()    { echo -e "${YELLOW}[skip]${NC}    $*"; }
err_msg() { echo -e "${RED}[FAILED]${NC}  $*" >&2; }

topic_exists() {
  local name="$1"
  if [[ "${KAFKA_CMD}" == rpk* ]]; then
    rpk topic list 2>/dev/null | grep -qx "${name}" || return 1
  else
    ${KAFKA_CMD} --bootstrap-server "${BROKERS}" ${EXTRA} \
      --list 2>/dev/null | grep -qx "${name}" || return 1
  fi
}

create_topic() {
  local name="$1" parts="$2"
  if [[ "${KAFKA_CMD}" == rpk* ]]; then
    rpk topic create "${name}" \
      --partitions "${parts}" \
      --replicas "${RF}" \
      --brokers "${BROKERS}" ${EXTRA}
  else
    ${KAFKA_CMD} --bootstrap-server "${BROKERS}" ${EXTRA} \
      --create \
      --topic "${name}" \
      --partitions "${parts}" \
      --replication-factor "${RF}"
  fi
}

# ── Main ─────────────────────────────────────────────────────────────────────

echo ""
echo "KafkaBatch topic provisioning"
echo "  brokers            : ${BROKERS}"
echo "  prefix             : '${PREFIX}' (${RAW_PREFIX:-none})"
echo "  replication factor : ${RF}"
echo "  partitions         : ${PARTITIONS:-per-topic defaults}"
echo "  fairness topics    : ${INCLUDE_FAIRNESS}"
echo ""

CREATED=0; SKIPPED=0; FAILED=0

for entry in "${TOPICS[@]}"; do
  name="${entry%%:*}"
  default_parts="${entry##*:}"
  parts="${PARTITIONS:-${default_parts}}"

  if topic_exists "${name}"; then
    skip "${name}  (already exists)"
    ((SKIPPED++)) || true
    continue
  fi

  if create_topic "${name}" "${parts}" 2>&1; then
    info "${name}  (partitions=${parts} rf=${RF})"
    ((CREATED++)) || true
  else
    err_msg "${name}"
    ((FAILED++)) || true
  fi
done

echo ""
echo "Done: ${CREATED} created, ${SKIPPED} skipped, ${FAILED} failed."
echo ""

if [ "${FAILED}" -gt 0 ]; then
  echo "ERROR: ${FAILED} topic(s) failed to create." >&2
  exit 1
fi
