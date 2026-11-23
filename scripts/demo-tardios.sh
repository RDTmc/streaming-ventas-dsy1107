#!/usr/bin/env bash
# Demuestra la política de tardíos con la ventana y gracia configuradas.
#   - evento dentro de gracia (eventTime = ahora - 20 s) -> se agrega
#   - evento fuera de gracia (eventTime = ahora - 180 s) -> se descarta, NO va a DLQ
# Se ejecuta en kafka-1, donde vive el contenedor kafka.
set -euo pipefail

TOPIC="${TOPIC:-ventas-raw}"
IN_ID="gracia-$(date +%s)"
OUT_ID="tardia-$(date +%s)"
IN_TS=$(date -u -d '20 seconds ago' +%Y-%m-%dT%H:%M:%SZ)
OUT_TS=$(date -u -d '180 seconds ago' +%Y-%m-%dT%H:%M:%SZ)

send() {
  local id="$1" ts="$2" store="$3"
  local json
  json=$(cat <<EOF
{"eventId":"$id","eventTime":"$ts","store":"$store","storeFormat":"tienda","city":"Santiago","channel":"presencial","category":"ropa","product":"Polera","quantity":1,"unitPriceClp":1000,"totalClp":1000,"customerId":"cliente-0001","paymentMethod":"debito"}
EOF
)
  printf '%s||%s\n' "$id" "$json" | docker exec -i kafka bash -lc \
    "env -u JAVA_TOOL_OPTIONS kafka-console-producer --bootstrap-server localhost:9092 --topic $TOPIC --property parse.key=true --property key.separator='||'"
}

send "$IN_ID" "$IN_TS" "Tienda Gracia"
send "$OUT_ID" "$OUT_TS" "Tienda Tardia"

echo "dentro de gracia: id=$IN_ID store=Tienda Gracia eventTime=$IN_TS"
echo "fuera de gracia : id=$OUT_ID store=Tienda Tardia eventTime=$OUT_TS"
