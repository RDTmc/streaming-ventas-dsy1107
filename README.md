# streaming-ventas-dsy1107

**DSY1107 — Desarrollo Cloud Native I**
**EP5 · Encargo: Streaming resiliente de ventas + EP6 · Defensa individual**

Flujo de ventas en tiempo real con **Kafka (2 brokers), Kafka Streams en Java, ventanas por tiempo de evento, DLQ, deduplicación de negocio y observabilidad** con Prometheus, Grafana y Loki.

> **Estado:** M1 desplegado (validación, DLQ y tiempo de evento). Deduplicación y política
> de tardíos se incorporan en M2–M3. Ver `evidence/m1/README.md`.

---

## 1. Arquitectura

```text
Productor (Java)
  │  key = eventId, timestamp = eventTime, JSON de venta
  ▼
ventas-raw · 4 particiones · RF=2 · ISR 1,2
  │
  ▼
Kafka Streams
  ├── validación (JSON + campos + total + eventTime)
  │      └── inválido ─────────────► ventas-raw-dlq · 4p · RF=2
  │                                    (reason, key, original, detected_at)
  ├── eventId duplicado ───────────► descarte con log (estado RocksDB, M2)
  └── válido ─► rekey por tienda ─► tumbling 60 s / gracia 30 s
                                     └──► ventas-agregadas · 2p · RF=2
                                                  │
                                                  ▼
                                Prometheus · Grafana · Loki · Kafka UI
```

Infraestructura del laboratorio: 4 EC2 (kafka-1, kafka-2, app, monitoring), ZooKeeper en kafka-1, JMX Exporter en cada proceso Java, Alloy para logs y node_exporter para métricas de host.

> ZooKeeper corre en un solo nodo: es un **punto único de falla** de la arquitectura base.

---

## 2. Estructura del repositorio

```text
.
├── app/app/                  # Maven + Dockerfile (productor y Streams)
├── deploy/
│   ├── kafka-1/              # ZooKeeper + broker 1 + Kafka UI
│   ├── kafka-2/              # broker 2
│   ├── app/                  # producer + consumer
│   ├── jmx/                  # configuración de JMX Exporter
│   └── monitoring/           # Prometheus, Grafana, Loki, Alloy, node_exporter
├── scripts/
│   ├── verify-lab.ps1        # verificación desde Windows (31 comprobaciones)
│   └── bootstrap-swap.sh     # swap persistente para hosts de 2 GB
├── docs/
│   └── plan-ep5-ep6.md       # análisis, riesgos y plan por iteraciones
└── evidence/
    ├── baseline/             # estado del laboratorio antes de los cambios (M0)
    └── m1/                   # validación, DLQ y tiempo de evento (M1)
```

Los archivos `*.env` reales **no se versionan**. Se crean a partir de los `.env.example`.

---

## 3. Prerrequisitos

- 4 instancias Ubuntu con Docker Engine + Compose v2.
- 2 brokers Kafka 7.6.x y ZooKeeper en kafka-1.
- Java 21 (la imagen se construye desde `app/app/Dockerfile`).
- Acceso SSH a los hosts y JMX Exporter 1.7.0 en `/opt/monitoring/jmx`.
- **Swap** en los hosts de 2 GB (`scripts/bootstrap-swap.sh`), para evitar que el OOM killer
  termine el broker cuando conviven ZooKeeper, Kafka, Kafka UI y Alloy.
- Opcional para verificación: AWS CLI, PowerShell 7 y `gh`.

---

## 4. Puesta en marcha

### 4.1 Crear los archivos de entorno

```bash
cp deploy/kafka-1/broker-a.env.example deploy/kafka-1/broker-a.env
cp deploy/kafka-2/broker-b.env.example deploy/kafka-2/broker-b.env
cp deploy/app/app.env.example          deploy/app/app.env
cp deploy/monitoring/monitoring.env.example deploy/monitoring/monitoring.env
cp deploy/monitoring/alloy.env.example deploy/monitoring/alloy.env
cp deploy/monitoring/node-exporter.env.example deploy/monitoring/node-exporter.env
```

Editar IPs privadas y contraseñas. Nunca subir estos archivos.

### 4.2 kafka-1 (ZooKeeper + broker 1)

```bash
cd deploy/kafka-1
docker compose -f docker-compose.broker.yaml --env-file broker-a.env --profile zk up -d
```

### 4.3 kafka-2 (broker 2, sin ZooKeeper)

```bash
cd deploy/kafka-2
docker compose -f docker-compose.broker.yaml --env-file broker-b.env up -d
```

### 4.4 App

```bash
cd deploy/app
docker compose -f docker-compose.app.yaml --env-file app.env build
docker compose -f docker-compose.app.yaml --env-file app.env up -d
```

### 4.5 Monitoring

```bash
cd deploy/monitoring
docker compose --env-file monitoring.env -f docker-compose.monitoring.yaml up -d
```

Alloy y node_exporter se despliegan por host con sus propios compose y env (ver `docs/plan-ep5-ep6.md`).

---

## 5. Verificación

Desde Windows, con la llave SSH fuera del repositorio:

```powershell
$env:LAB_KEY_PATH = "C:\ruta\a\kafka-1.pem"
powershell -ExecutionPolicy Bypass -File .\scripts\verify-lab.ps1
```

Comprueba EC2, health de Kafka/ZooKeeper, JMX, topics, app, Prometheus/Grafana/Loki, targets, reglas y discos. Salida esperada: `PASS: 31 FAIL: 0`.

---

## 6. Decisiones de diseño

| Tema | Decisión |
|---|---|
| Clave del productor | `eventId` (co-particiona duplicados para dedupe stateful) |
| Ventana | Tumbling 60 s por `eventTime` |
| Gracia | 30 s |
| Deduplicación | Estado `eventos-procesados` con retención 7 días |
| Tardo/descarte | Dentro de gracia se agrega; fuera se descarta con métrica y log, nunca DLQ |
| Replicación | RF=2, `min.insync.replicas=1`; no se usa RF=3 en un clúster de 2 brokers |
| Exactly-once | `exactly_once_v2` para procesamiento; el duplicado de negocio exige estado por `eventId` |

---

## 7. Estado del laboratorio (línea base + extensiones)

| Iteración | Qué se añadió | Verificación |
|---|---|---|
| M0 | Línea base, backup de imágenes `:v1-base`, swap, repo limpio | `verify-lab.ps1` 31/31 |
| M1 | Validación, DLQ, `eventTime`, clave `eventId` | DLQ con motivo/clave/original; 9 pruebas |
| M2 | Deduplicación por `eventId`, retención 7 d, changelog RF=2 | Duplicado no duplica monto; 11 pruebas |
| M3 | Gracia 30 s, política de tardíos | Dentro/fuera de gracia; 13 pruebas |
| M5 | Caída y recuperación de broker 2 | ISR 1,2 → 1 → 1,2 |

> El laboratorio estuvo sano (`verify-lab.ps1` 31/31 PASS) al finalizar cada iteración.

## 8. Documentación y entrega

- `docs/plan-ep5-ep6.md`: análisis del encargo, riesgos de ruptura, mitigaciones y plan M0–M7.
- `evidence/baseline/`: estado del laboratorio antes de los cambios (M0).
- `evidence/m1|m2|m3|m5/`: evidencia de cada iteración.
- **No se suben secretos:** `.env`, `.pem` y credenciales quedan fuera del repositorio (`.gitignore`).

## 9. Seguridad

- No se versionan `.env`, llaves `.pem` ni credenciales.
- Las contraseñas de ejemplo son marcadores (`replace-with-a-strong-password`).
- Kafka UI y Grafana no deben exponerse sin autenticación.

