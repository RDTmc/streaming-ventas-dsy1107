# M1 — Validación, DLQ y tiempo de evento

Fecha: 2026-11-23
Rama: `feature/streaming-resiliente`
Estado: **desplegado y verificado en el laboratorio**

## 1. Qué se implementó

| Componente | Archivo | Función |
|---|---|---|
| Validación pura | `SaleValidator.java` | JSON, campos obligatorios, `eventTime` ISO-8601, rangos y `totalClp = quantity * unitPriceClp` |
| Resultado tipado | `ValidationResult.java` | Separa evento válido de rechazo, conserva motivo, `eventId` y valor original |
| Sobre de DLQ | `DlqEnvelope.java` | `reason`, `source_topic`, `key`, `event_id`, `original`, `detected_at` |
| Tiempo de evento | `EventTimeExtractor.java` | Usa `eventTime` del JSON como timestamp; si no es parseable cae al timestamp del registro |
| Topología | `StreamsApp.java` | `split` lógico válido/inválido, sink a `ventas-raw-dlq`, log contextual |
| Productor | `ProducerApp.java` | Clave `eventId`, timestamp explícito y modo `mixed` con 5 escenarios |

## 2. Decisiones aplicadas

- Clave del productor: **`eventId`**. Dos copias del mismo evento caen en la misma partición.
- Timestamp del registro: **`eventTime`** del evento.
- Evento inválido: se escribe en `ventas-raw-dlq` y **no** se agrega.
- Evento válido pero tardío: se descarta por la ventana (con gracia 0 en esta iteración); **no** va a DLQ.
- El flujo válido y el agregado por tienda siguen funcionando sin cambios de esquema.

## 3. Evidencia de la corrida

Productor `mixed` con `SCENARIO_EVERY=2`, 20 eventos:

| Escenario | Emitidos | Resultado observado |
|---|---|---|
| Venta válida | 11 | Se agrega |
| JSON malformado | 2 | DLQ `JSON_INVALIDO` |
| Campo faltante (`quantity`) | 2 | DLQ `CAMPO_OBLIGATORIO:quantity` |
| Total inconsistente | 2 | DLQ `TOTAL_INCONSISTENTE` |
| Duplicado (`eventId` repetido) | 2 | Se procesa (aún sin dedupe; se corrige en M2) |
| Tardío (`eventTime` -180 s) | 1 | Descartado por ventana, no va a DLQ |

DLQ acumulada: **8 mensajes** (3 `JSON_INVALIDO`, 3 `CAMPO_OBLIGATORIO:quantity`, 2 `TOTAL_INCONSISTENTE`).
Cada sobre conserva el valor original y la clave.

Archivos:

- `dlq-messages.jsonl`: los 8 sobres completos.
- `streams-dlq-y-tardios.log`: logs de la app con el envío a DLQ y el descarte por ventana.
- `offsets-consumer-group.txt`: offsets de `ventas-agregadas` y lag del grupo `ventas-aggregator`.
- `pruebas-mvn.txt`: salida de `mvn test` (9 pruebas en verde).

## 4. Pruebas automatizadas

```
mvn test
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- `SaleValidatorTest` (6): válido, JSON malformado, campo faltante, total inconsistente, `eventTime` inválido, cantidad no positiva.
- `TopologyDlqTest` (2): los inválidos van a DLQ con sobre completo; un válido no pasa por DLQ.
- `TopologyAggregationTest` (1): ventana de 60 s, agregado por tienda, `suppress` al cerrar.

## 5. Limitaciones conocidas de esta iteración

- Sin deduplicación por `eventId` todavía (M2).
- Gracia de 30 s configurada en el código pero el `app.env` del laboratorio aún tiene `WINDOW_GRACE_SECONDS=0` (M3).
- El generador `mixed` se ejecuta como contenedor de una sola vez; los scripts reproducibles llegan en M4.

## 6. Nota operativa: OOM en kafka-1

Durante la línea base, kafka-1 (1,9 GB) mató el broker por OOM 5 veces:
ZooKeeper + Kafka + Kafka UI + Alloy + JMX no caben sin swap.

Mitigación aplicada: **swap persistente** (2 GB en kafka-1, 1 GB en kafka-2 y monitoring;
app ya tenía 2 GB) y `vm.swappiness=10`. Script reproducible: `scripts/bootstrap-swap.sh`.
No se modificó la configuración de Kafka ni de ZooKeeper.
