# M3 — Politica de tardios y gracia de 30 s

Fecha: 2026-11-23
Estado: **desplegado y verificado en el laboratorio**

## 1. Qué se configuró

- `WINDOW_GRACE_SECONDS=30` en `app.env` (y `app.env.example`).
- La topología usa `TimeWindows.ofSizeAndGrace(60s, 30s)` por `eventTime`.
- Dentro de gracia: el evento se agrega a su ventana.
- Fuera de gracia: Kafka Streams lo descarta (`expired window`); se registra y se evidencia con
  `kafka_streams_stream_task_metrics_dropped_records`. **Nunca va a la DLQ.**
- Esto separa claramente "dato inválido" (→ DLQ) de "evento válido pero tardío" (→ descarte por ventana).

## 2. Evidencia

### Manual (clúster real)

Se enviaron dos eventos con `scripts/demo-tardios.sh`:

| Evento | eventTime | Resultado |
|---|---|---|
| `gracia-...` (Tienda Gracia) | ahora - 20 s | Se agrega (ventana dentro de gracia) |
| `tardia-...` (Tienda Tardia) | ahora - 180 s | Descartado por ventana expirada, no va a DLQ |

Log de la app:

```text
KStreamWindowAggregate - Skipping record for expired window. ... streamTime=...
```

DLQ después de los tardíos: sin mensajes `Gracia`/`Tardia`.

### Pruebas automatizadas (`TopologyLateEventTest`)

- `eventoDentroDeGraciaSeAgrega`: eventTime dentro de ventana+gracia → se agrega (count 1).
- `eventoFueraDeGraciaNoSeAgregaYNoVaADLQ`: eventTime en ventana ya cerrada → se descarta, no aparece en salida.

## 3. Pruebas totales (M1 + M2 + M3)

```
mvn test
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Clases: `SaleValidatorTest` (6), `TopologyAggregationTest` (1), `TopologyDlqTest` (2),
`TopologyDedupeTest` (2), `TopologyLateEventTest` (2).

Archivos de evidencia: `streams-tardios.log`, `agregados-tardios.txt`, `dlq-gracia.txt`,
`pruebas-mvn.txt`, `demo-tardios.sh`.

## 4. Para la defensa EP6

Mostrar:
1. Venta válida → `ventas-agregadas`.
2. Mensaje defectuoso → `ventas-raw-dlq` con motivo/clave/original.
3. Duplicado → descarte por `eventId` (no duplica el monto).
4. Tardío → descarte por ventana, métrica `dropped_records`, no entra a DLQ.
5. Consumer group `ventas-aggregator` y lag en Kafka UI.
