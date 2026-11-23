#!/usr/bin/env bash
# Captura ISR y offsets de los topicos de negocio.
set -euo pipefail
OUTDIR="${1:-/tmp/m5}"
mkdir -p "$OUTDIR"

run() { docker exec kafka bash -lc "env -u JAVA_TOOL_OPTIONS $*"; }

echo "== topics describe ==" > "$OUTDIR/topics.txt"
run kafka-topics --bootstrap-server localhost:9092 --describe \
  | grep -E "^Topic: ventas" -A3 >> "$OUTDIR/topics.txt" 2>&1 || true
echo "== under-replicated (vacio si sano) ==" >> "$OUTDIR/topics.txt"
run kafka-topics --bootstrap-server localhost:9092 --describe --under-replicated-partitions >> "$OUTDIR/topics.txt" 2>&1 || true
echo "== consumer group ventas-aggregator =" > "$OUTDIR/group.txt"
run kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group ventas-aggregator >> "$OUTDIR/group.txt" 2>&1 || true
echo "done: $OUTDIR"
