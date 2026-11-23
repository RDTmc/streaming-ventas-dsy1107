# M2 — Deduplicación por `eventId`

Fecha: 2026-11-23
Estado: **desplegado y verificado en el laboratorio**

## 1. Qué se implementó

- `DedupeTransformer` con estado persistente `eventos-procesados`.
- Clave del estado: `eventId`; valor: `seenAt|sha256(payload)`.
- Primer evento: se guarda y se agrega.
- Mismo `eventId` y mismo payload: `DUPLICADO`, se descarta (no cuenta).
- Mismo `eventId` con payload distinto: `DUPLICADO_CON_DIFERENCIAS`, se conserva el primero.
- Retención: **7 días** (`DEDUPE_RETENTION_SECONDS=604800`).
- Purga por punctuator cada 10 minutos (`DEDUPE_PURGE_INTERVAL_SECONDS=600`).
- Changelog automático `ventas-aggregator-eventos-procesados-changelog` con **RF=2, ISR 1,2**.
- La clave del productor ya es `eventId`, por lo que dos copias caen en la misma partición.

## 2. Evidencia E2E

Se enviaron **dos veces** el mismo evento (`dedupe-e2e-20261123`, tienda `Tienda Dedupe`,
1000 CLP) usando `scripts/demo-dedupe.sh`.

Log de la app:

```text
DedupeTransformer - DUPLICADO eventId=dedupe-e2e-20261123 store=Tienda Dedupe
```

Agregado emitido:

```json
{"store":"Tienda Dedupe","window_start":"2026-11-23T02:03:00Z","window_end":"2026-11-23T02:04:00Z","sales_count":1,"total_clp":1000,"avg_ticket_clp":1000}
```

Resultado: **1 venta, 1000 CLP**, aunque el evento se produjo dos veces.

Archivos:

- `dedupe-log.txt`
- `agregado-dedupe.txt`
- `pruebas-mvn.txt` (11 pruebas, incluye `TopologyDedupeTest`)

## 3. Pruebas automatizadas

- `elMismoEventoNoSeCuentaDosVeces`: 2 eventos válidos + 1 duplicado → conteo 2.
- `siElPayloadDifiereSeConservaElPrimero`: duplicado con otro monto → conteo 1, monto del primero.

## 4. EOS v2 vs deduplicación de negocio

- `exactly_once_v2` evita duplicados por **reintentos del procesamiento** (transacciones
  de productor y offsets).
- No evita que **el mismo evento de negocio** se produzca dos veces (doble clic, reintento
  del cliente, reproceso manual).
- Por eso se agrega el estado `eventos-procesados`: es deduplicación **de negocio**, idempotente
  por `eventId`.

## 5. Limitación

- Si un productor antiguo usa otra clave (por ejemplo `customerId`), dos copias del mismo
  `eventId` podrían caer en particiones distintas y la dedupe no las vería. El contrato del
  productor es: **clave = `eventId`**.
