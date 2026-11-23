# Línea base M0 — laboratorio EA3 antes de las extensiones EP5/EP6

Fecha: 2026-11-23
Rama: `feature/streaming-resiliente`
Propósito: congelar el estado sano del laboratorio y permitir rollback antes de modificar la app.

## 1. Infraestructura

| Host | Rol | IP privada | EIP |
|---|---|---|---|
| kafka-1 | ZooKeeper + broker 1 + Kafka UI | 10.10.0.33 | 107.22.182.208 |
| kafka-2 | broker 2 | 10.10.0.126 | 3.233.42.157 |
| app | productor + Kafka Streams | 10.10.0.25 | 54.173.211.212 |
| monitoring | Prometheus + Grafana + Loki + Alloy | 10.10.0.61 | 98.82.45.103 |

## 2. Tópicos de negocio (RF=2, min.insync.replicas=1)

| Tópico | Particiones | Uso |
|---|---|---|
| `ventas-raw` | 4 | entrada de ventas |
| `ventas-agregadas` | 2 | salida de ventanas por tienda |
| `ventas-raw-dlq` | 4 | DLQ (vacía en la línea base) |

Tópicos internos de Streams: changelog de supresión, changelog y repartición de `ventas-por-tienda`.
Detalle completo en `topics-describe.txt`.

## 3. App

- Consumer group: `ventas-aggregator`.
- Topología base: `ventas-raw` → `selectKey(store)` → tumbling 60 s → `ventas-agregadas`.
- `exactly_once_v2` activo.
- Estado local en `/tmp/kafka-streams` (efímero; se restaura desde changelog).
- Versión desplegada: imágenes `ventas-app-producer:latest` (`a1ea8cb86232`) y `ventas-app-consumer:latest` (`b27621ea8774`).

## 4. Rollback

```bash
# En la EC2 app
docker tag ventas-app-producer:v1-base ventas-app-producer:latest
docker tag ventas-app-consumer:v1-base ventas-app-consumer:latest
cd ~/lab/app
docker compose -f docker-compose.app.yaml --env-file app.env up -d
```

Las imágenes `:v1-base` ya están creadas en la EC2 app.

## 5. Archivos de evidencia

- `verify-lab.txt`: salida completa de la verificación (31 comprobaciones).
- `topics-describe.txt`: `kafka-topics --describe` del clúster completo.
- `consumer-groups.txt`: grupos de consumo activos.

## 6. Observaciones de la corrida

- El laboratorio se estaba recuperando de un reinicio: broker 1 aún no estaba en ISR al
  primer intento y una regla pasó por `pending`. No son fallas permanentes.
- Se endurecieron dos comprobaciones del script:
  - `Streams en RUNNING` ahora mira los últimos 30 min de logs (antes 400 líneas, que se
    llenaban con warnings de ventanas expiradas).
  - `reglas cargadas` ahora exige 10 reglas y 0 en `firing` (antes exigía 10 `inactive`,
    lo que fallaba si una regla estaba `pending` de forma transitoria).
