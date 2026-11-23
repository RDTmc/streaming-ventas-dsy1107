#!/usr/bin/env bash
# Envía dos veces el mismo evento (mismo eventId) a ventas-raw para demostrar
# la deduplicación por estado. Se ejecuta en kafka-1, donde vive el contenedor kafka.
# Uso: ./demo-dedupe.sh [eventId]
set -euo pipefail

ID="${1:-dedupe-e2e-$(date +%s)}"
STORE="${STORE:-Tienda Dedupe}"
TOPIC="${TOPIC:-ventas-raw}"
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

JSON=$(cat <<EOF
{"eventId":"$ID","eventTime":"$TS","store":"$STORE","storeFormat":"tienda","city":"Santiago","channel":"presencial","category":"ropa","product":"Polera","quantity":1,"unitPriceClp":1000,"totalClp":1000,"customerId":"cliente-0001","paymentMethod":"debito"}
EOF
)

for _ in 1 2; do
  printf '%s||%s\n' "$ID" "$JSON"
done | docker exec -i kafka bash -lc "env -u JAVA_TOOL_OPTIONS kafka-console-producer --bootstrap-server localhost:9092 --topic $TOPIC --property parse.key=true --property key.separator='||'"

echo "enviado 2 veces: id=$ID store=$STORE eventTime=$TS"
