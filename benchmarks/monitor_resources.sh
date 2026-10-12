#!/usr/bin/env bash
# Samples `docker stats` for the Chronos containers every 2s for the given duration
# and appends one JSON line per sample per container to the output file. Run this
# in the background (or in a separate terminal) while generate_load.py runs.
#
# Usage: benchmarks/monitor_resources.sh <duration_seconds> <output_file> [compose_project_prefix]
set -euo pipefail

DURATION="${1:-60}"
OUTPUT="${2:-benchmarks/results/latest/resource_samples.jsonl}"
PREFIX="${3:-highprecisiondistributedjobscheduler}"

mkdir -p "$(dirname "$OUTPUT")"
: > "$OUTPUT"

END=$(( $(date +%s) + DURATION ))
while [ "$(date +%s)" -lt "$END" ]; do
  TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  docker stats --no-stream --format '{{json .}}' \
    "${PREFIX}-api-1" "${PREFIX}-dispatcher-1" "${PREFIX}-outbox-publisher-1" \
    "${PREFIX}-worker-1" "${PREFIX}-reconciliation-1" "${PREFIX}-postgres-1" \
    "${PREFIX}-localstack-1" 2>/dev/null \
    | while read -r line; do
        echo "{\"sampled_at\":\"${TS}\",$(echo "$line" | sed 's/^{//')" >> "$OUTPUT"
      done
  sleep 2
done
