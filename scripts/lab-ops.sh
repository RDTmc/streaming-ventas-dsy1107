#!/usr/bin/env bash
# Scripts reproducibles para el laboratorio de ventas (EP5/EP6).
# Se ejecutan en la EC2 kafka-1 (donde viven los contenedores kafka y zookeeper).
# Uso: source o ejecutar cada uno por separado.
set -euo pipefail

KAFKA="docker exec -i kafka bash -lc 'env -u JAVA_TOOL_OPTIONS'"
BOOTSTRAP="localhost:9092"
RAW="ventas-raw"
AGG="ventas-agregadas"
DLQ="ventas-raw-dlq"

# -----------------------------------------------------------------------------
# 1. Crear / describir topicos de negocio
# -----------------------------------------------------------------------------
cmd_create_topics() {
  echo "== Creando topicos de negocio (si no existen) =="
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --create --topic $RAW --partitions 4 --replication-factor 2 --if-not-exists \
    --config min.insync.replicas=1 --config retention.ms=604800000"   # 7 dias
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --create --topic $AGG --partitions 2 --replication-factor 2 --if-not-exists \
    --config min.insync.replicas=1 --config retention.ms=2592000000"  # 30 dias
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --create --topic $DLQ --partitions 4 --replication-factor 2 --if-not-exists \
    --config min.insync.replicas=1 --config retention.ms=2592000000"  # 30 dias
  echo "OK topicos listos"
}

# -----------------------------------------------------------------------------
# 2. Estado del cluster (topicos, ISR, grupos, offsets, lag)
# -----------------------------------------------------------------------------
cmd_describe_cluster() {
  echo "== Topicos del cluster =="
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --list | grep -E '^ventas-|^__' | sort"
  echo
  echo "== Descripcion de topicos de negocio =="
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic $RAW --topic $AGG --topic $DLQ"
  echo
  echo "== Particiones bajo-replicadas (debe estar vacio) =="
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --describe --under-replicated-partitions" || true
  echo
  echo "== Grupos de consumo y lag =="
  $KAFKA "kafka-consumer-groups --bootstrap-server $BOOTSTRAP --describe --group ventas-aggregator" || true
}

# -----------------------------------------------------------------------------
# 3. Ejecutar escenarios de prueba (produce lotes controlados)
# -----------------------------------------------------------------------------
cmd_scenarios() {
  local mode="${1:-mixed}"
  local count="${2:-20}"
  local interval="${3:-0.3}"
  echo "== Ejecutando escenarios (modo=$mode, eventos=$count, intervalo=$interval s) =="
  docker exec -i kafka bash -lc "
    docker exec -i ventas-app-producer-1 bash -lc 'true' >/dev/null 2>&1 || true
  " >/dev/null 2>&1 || true
  # Se lanza un productor de un solo uso sobre la EC2 app para no romper el contenedor estable.
  ssh_app "cd ~/lab/app && docker compose -f docker-compose.app.yaml --env-file app.env run --rm \
    -e PRODUCE_MODE=$mode -e PRODUCER_MAX_EVENTS=$count -e PRODUCE_INTERVAL=$interval producer"
  echo "OK escenarios enviados"
}

# -----------------------------------------------------------------------------
# 4. Caida controlada de broker 2 (escenario de tolerancia a fallos)
# -----------------------------------------------------------------------------
cmd_broker_failure() {
  echo "== Prueba de caida de broker 2 =="
  echo "ISR ANTES:"
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic $RAW --topic $AGG --topic $DLQ" \
    | grep -E 'Partition|Isr:' | head -20
  ssh_kafka2 "docker stop kafka"
  echo "broker 2 detenido. Esperando rebalanceo (30 s)..."
  sleep 30
  echo "ISR DURANTE LA CAIDA:"
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic $RAW --topic $AGG --topic $DLQ" \
    | grep -E 'Partition|Isr:' | head -20
  ssh_kafka2 "docker start kafka"
  echo "broker 2 reiniciado. Esperando recuperacion de ISR (60 s)..."
  sleep 60
  echo "ISR DESPUES DE LA RECUPERACION:"
  $KAFKA "kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic $RAW --topic $AGG --topic $DLQ" \
    | grep -E 'Partition|Isr:' | head -20
  echo "OK prueba de broker 2 terminada"
}

# -----------------------------------------------------------------------------
# helpers (requieren definirlas en el entorno que invoque este script)
# -----------------------------------------------------------------------------
ssh_app() { ssh -i "${LAB_KEY_PATH:-}" ubuntu@"${APP_PUBLIC_IP:-}" "$@"; }
ssh_kafka2() { ssh -i "${LAB_KEY_PATH:-}" ubuntu@"${KAFKA2_PUBLIC_IP:-}" "$@"; }

case "${1:-}" in
  create-topics) cmd_create_topics ;;
  describe-cluster) cmd_describe_cluster ;;
  scenarios) cmd_scenarios "${2:-mixed}" "${3:-20}" "${4:-0.3}" ;;
  broker-failure) cmd_broker_failure ;;
  *)
    echo "Uso: $0 {create-topics|describe-cluster|scenarios [modo] [count] [interval]|broker-failure}"
    exit 1
    ;;
esac
