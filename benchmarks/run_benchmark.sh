#!/usr/bin/env bash
# Orchestrates one full benchmark run: records hardware info, samples container
# resource usage throughout, drives load against the live API, waits for the queue
# to drain, and analyzes the result straight from Postgres.
#
# Requires the full stack already running: docker-compose up -d
#
# Usage: benchmarks/run_benchmark.sh [run_name] [duration_seconds] [rate] [burst_size]
set -euo pipefail

RUN_NAME="${1:-run_$(date +%Y%m%d_%H%M%S)}"
DURATION="${2:-30}"
RATE="${3:-100}"
BURST="${4:-0}"
RUN_DIR="benchmarks/results/${RUN_NAME}"
DRAIN_WAIT="${DRAIN_WAIT_SECONDS:-20}"  # extra time after load stops, for near-future jobs to complete

mkdir -p "$RUN_DIR"

echo "=== Chronos benchmark: ${RUN_NAME} ==="
echo "duration=${DURATION}s rate=${RATE}/s burst=${BURST}"

echo "--- recording hardware info ---"
{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "platform: $(uname -a)"
  echo "cpu_count (host): $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo unknown)"
  echo "--- colima vm resources ---"
  colima status 2>&1 || echo "colima not in use"
  docker info --format 'docker CPUs: {{.NCPU}}  Memory: {{.MemTotal}}' 2>&1 || true
} > "${RUN_DIR}/hardware_info.txt"
cat "${RUN_DIR}/hardware_info.txt"

TOTAL_MONITOR_SECONDS=$((DURATION + DRAIN_WAIT + 10))
echo "--- starting resource monitor for ${TOTAL_MONITOR_SECONDS}s (background) ---"
bash benchmarks/monitor_resources.sh "$TOTAL_MONITOR_SECONDS" "${RUN_DIR}/resource_samples.jsonl" &
MONITOR_PID=$!

echo "--- generating load ---"
# No extra volume mount needed: the api service's docker-compose.yml entry already
# bind-mounts the whole repo at /srv, so "$RUN_DIR" (relative to the repo root) is
# the same path on both sides.
docker-compose run --rm -T \
  api python -m benchmarks.generate_load \
    --base-url http://api:8000 \
    --duration-seconds "$DURATION" \
    --rate "$RATE" \
    --burst-size "$BURST" \
    --output-dir "$RUN_DIR"

echo "--- waiting ${DRAIN_WAIT}s for near-future jobs to complete ---"
sleep "$DRAIN_WAIT"

echo "--- analyzing pipeline from the database ---"
docker-compose run --rm -T \
  api python -m benchmarks.measure_pipeline --run-dir "$RUN_DIR"

echo "--- waiting for resource monitor to finish ---"
wait "$MONITOR_PID" || true

echo "--- summarizing resource usage ---"
docker-compose run --rm -T \
  api python -m benchmarks.summarize_resources --run-dir "$RUN_DIR"

echo "=== done. Results in ${RUN_DIR}/ ==="
ls -la "$RUN_DIR"
