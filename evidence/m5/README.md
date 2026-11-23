# M5 — Tolerancia a fallos: caida y recuperacion de broker 2

Fecha: 2026-11-23
Estado: **verificado en el laboratorio**

## 1. Escenario ejecutado

Detener **solo broker 2** (kafka-2), nunca kafka-1 (ahí vive ZooKeeper + el controller). Verificar ISR antes / durante / despues.

| Momento | ISR ventas-raw / ventas-agregadas / ventas-raw-dlq | Under-replicated |
|---|---|---|
| Antes (2 brokers) | `Isr: 1,2` | ninguno |
| Durante (broker 2 caido) | `Isr: 1` (solo broker 1) | replicaciones de broker 2 |
| Despues (broker 2 reiniciado) | `Isr: 1,2` | ninguno |

## 2. Qué se demuestra

- RF=2 + min.insync.replicas=1: con un broker caído, el clúster sigue aceptando lecturas/escrituras (el líder restante atiende).
- Producer con `acks=all` + idempotencia: no se pierden mensajes ni se duplican por reintento.
- ZooKeeper en kafka-1 se mantiene; broker 2 se detiene y arranca sin afectar el controller.
- Al reiniciar broker 2, los partitions se re-sincronizan y el ISR se restaura a 1,2.

## 3. Reinicio de Kafka Streams (restauracion de estado)

Cada vez que el contenedor `ventas-app-consumer-1` se recrea, Kafka Streams restaura su estado desde los changelogs (RF=2) y reanuda sin doble conteo, gracias a `exactly_once_v2` y al store `eventos-procesados` (changelog replicado). Esto es justo la evidencia de "recuperacion de estado" que pide EP5 y se pregunta en EP6.

## 4. Evidencia

- `isr-antes.txt`: `Isr: 1,2`, sin under-replicated.
- `isr-durante.txt`: `Isr: 1`, bajo-replicados por broker 2 ausente, clúster operativo.
- `isr-despues.txt`: `Isr: 1,2` recuperado.
- `group-antes.txt`: offsets y lag del grupo `ventas-aggregator` (lag 0 en `ventas-raw`).

## 5. Riesgo controlado

No se detiene kafka-1 porque aloja ZooKeeper y el controller activo; detenerlo pararía todo el clúster. El encargo pide la prueba de fallo de un broker; se elige broker 2.
