# Ejercicio 1 — ETL Airflow + MinIO + Trino (Polars)

Pipeline ETL que ingesta `data_prueba_tecnica.csv` (10,000 transacciones), lo limpia
con **Polars**, lo materializa como Parquet en MinIO y lo expone para consulta SQL
en **Trino**.

## Arquitectura

```
CSV local ──► [ingest] ──► MinIO bck-landing/data/data_prueba_tecnica.csv
                                       │
                                       ▼
                            [transform_to_bronze (Polars)]
                                       │
                                       ▼
                MinIO bck-bronze/master/data_prueba_tecnica.parquet
                                       │
                                       ▼
              Trino: bronze.prueba.tbl_data  ──► DBeaver / SQL
```

## Servicios (docker-compose)

| Servicio        | Puerto host  | Notas                                             |
|-----------------|--------------|---------------------------------------------------|
| Airflow API     | `8080`       | UI: http://localhost:8080  (user/pass: `airflow`) |
| MinIO           | `9000/9001`  | Console: http://localhost:9001 (`minio/minio1234`)|
| Trino           | `8090`       | JDBC: `jdbc:trino://localhost:8090` user `root`   |
| Hive Metastore  | `9083`       | Backend MySQL                                     |
| MySQL           | `3306`       | Metastore DB                                      |

El CSV se monta de solo-lectura en el contenedor de Airflow:
`./data_prueba_tecnica.csv → /opt/airflow/data/data_prueba_tecnica.csv`.

## Cómo ejecutar

```bash
cd ejercicio1

# 1) Levantar el stack
docker compose up -d

# 2) Esperar a que Airflow esté healthy (~1-2 min)
#    Verificar en http://localhost:8080

# 3) Despausar y ejecutar el DAG
#    Desde la UI: activar 'etl_engineer_challenge' y Trigger DAG
#    O por CLI:
docker compose exec airflow-scheduler airflow dags unpause etl_engineer_challenge
docker compose exec airflow-scheduler airflow dags trigger etl_engineer_challenge
```

## DAG: `etl_engineer_challenge`

| Tarea                | Responsabilidad                                                    |
|----------------------|--------------------------------------------------------------------|
| `setup_buckets`      | Crea `bck-landing` y `bck-bronze` (idempotente).                   |
| `ingest_to_landing`  | Sube el CSV a `bck-landing/data/data_prueba_tecnica.csv`.          |
| `transform_to_bronze`| Lee con Polars, limpia, agrega y escribe el Parquet en bronze.     |
| `expose_in_trino`    | Crea `bronze.prueba` y la tabla `tbl_data` apuntando al Parquet.   |

Dependencia lineal: `setup_buckets → ingest_to_landing → transform_to_bronze → expose_in_trino`.

### Conexión DBeaver

* Driver: **Trino**
* URL: `jdbc:trino://localhost:8090`
* User: `root` (sin password)
* Catálogo: `bronze`, schema: `prueba`, tabla: `tbl_data`

Ejemplos de consulta:

```sql
-- Tope-10 días por monto cobrado
SELECT name, created_at, amount_sum_by_name_day, paid_count_by_name_day
FROM bronze.prueba.tbl_data
GROUP BY name, created_at, amount_sum_by_name_day, paid_count_by_name_day
ORDER BY amount_sum_by_name_day DESC
LIMIT 10;

-- Distribución de status
SELECT status, count(*) AS n
FROM bronze.prueba.tbl_data
GROUP BY status
ORDER BY n DESC;
```

---

## Análisis del dataset y respuestas a las preguntas

Antes de implementar la limpieza se hizo un *profiling* del CSV. Los hallazgos se
resumen aquí (los conteos son exactos sobre las 10,000 filas).

### 1) `id` nulos — ¿qué hacer con ellos?

**Hallazgo:** 3 filas con `id` vacío. El resto cumple SHA-1 (40 hex chars) y no hay duplicados.

**Decisión:** **descartarlas** del dataset master. Justificación:

* `id` es la clave natural de la transacción; sin ella no hay trazabilidad,
  no se puede deduplicar, ni reconciliar con la fuente.
* No es imputable: no existen otros campos que combinados sean únicos
  (mismas combinaciones de `name + company_id + amount + created_at` reaparecen).
* 3/10,000 = 0.03% — impacto despreciable.

**Mitigación recomendada para próximas ingestas:** alertar al área operativa
(generar un *quarantine* en `bck-landing/quarantine/` con esas filas y un job
de Slack/email). En este pipeline el `print` final reporta cuántas se descartaron.

### 2) `name` y `company_id` — inconsistencias y mitigación

**Hallazgos:**

| Patrón                                     | Filas | Ejemplos                              |
|--------------------------------------------|-------|---------------------------------------|
| `name` vacío                               | 3     | `''`                                  |
| `name` con marker corrupto `0x...`         | 2     | `MiPas0xFFFF`, `MiP0xFFFF`            |
| `company_id` vacío                         | 4     | `''`                                  |
| `company_id` corrupto (no SHA-1)           | 1     | `*******`                             |
| `name` con >1 `company_id` asociado        | 1     | `MiPasajefy` apareció con un cid roto |

Sólo hay 2 entidades reales en la fuente: `MiPasajefy` y `Muebles chidos`,
con sus `company_id` SHA-1. Los valores con `0x...` son corrupción de bytes
(misma firma que aparece en `status` y en `amount`); el `*******` parece un
enmascaramiento mal aplicado.

**Mitigación implementada:**

1. **Normalización de strings** (`strip_chars`).
2. **Validación de formato** del `id` y `company_id` con regex `^[0-9a-fA-F]{40}$`;
   lo que no cumpla → `null`.
3. **Detección de marker `0x` en `name`** → `null`.
4. **Imputación cruzada `name ↔ company_id`** usando como diccionario de
   referencia los pares íntegros observados en el propio dataset
   (`{cbf1c8…: MiPasajefy, 8f642d…: Muebles chidos}`). Esto recupera las 5+1
   filas con uno de los dos campos corrupto.
5. Resultado tras limpieza: `name` queda con sólo 2 valores y `company_id`
   también — la integridad referencial entre ambos queda restaurada.

**Mitigación recomendada a futuro:** la tabla `name ↔ company_id` debería
vivir en una **dimensión maestra** (e.g. `bck-bronze/dim/dim_company.parquet`)
poblada desde el CRM/ERP — no inferida del propio fichero transaccional.

### 3) Resto de campos — valores atípicos y procedimiento

**`amount`** (sin nulos en origen, pero hay corrupción severa):

* 5 valores absurdos: `21,312,312,393.19`, `2.13e16`, `2.13e18`, `3.0e34`, `inf`.
  Todos en transacciones `pending_payment` o `voided` (no afectan revenue real).
* p99 = 5,148.21; p99.9 = 42,243.0; máximo legítimo observado = 83,983.0.
* No hay negativos ni ceros.

**Decisión:** parsear a `Float64` y rechazar (→ `null`) los no finitos y
cualquier valor `>= 1e9`. El umbral 1e9 es muy generoso vs. p99.9, pero corta
limpiamente los 5 outliers absurdos. En el master quedan 5 filas con `amount`
nulo pero con su `id` y resto de campos válidos (preservamos la fila).

**`status`** (10 categorías observadas):

* 8 válidas (`paid`, `voided`, `pending_payment`, `refunded`, `charged_back`,
  `pre_authorized`, `expired`, `partially_refunded`).
* 2 corruptas (`p&0x3fid`, `0xFFFF`) → 1 fila cada una.

**Decisión:** las 2 corruptas → `unknown` (no inferimos: aunque `p&0x3fid`
*parece* `paid`, no cambiamos la verdad operativa sin evidencia; sólo lo
flaggeamos para que el área operativa decida).

**`created_at`** (3 con formato no estándar):

* `2019-02-27T00:00:00` (ISO con T)
* `20190516`, `20190121` (compacto sin separadores)

**Decisión:** parser multi-formato con `coalesce` de tres `strptime` (YYYY-MM-DD,
YYYYMMDD, ISO con T). Tras esto, `created_at` queda 100% no nulo y dentro de
`[2019-01-01, 2019-05-20]`.

**`paid_at`** (3,991 nulos):

* Verificación: los `null` corresponden 1:1 con `status ∈ {voided, pending_payment, expired, pre_authorized}` — comportamiento esperado, NO es inconsistencia.
* Adicional: regla de negocio — si `paid_at < created_at`, anular `paid_at`
  (no se observa en este dataset, pero la lógica queda en el DAG para futuras
  cargas).

**Otros chequeos pasados:**

* `id` único (no duplicados) en las filas válidas.
* `paid_at` nunca anterior a `created_at`.

### 4) Mejoras al ETL para próximas versiones

| # | Mejora | Beneficio |
|---|---|---|
| 1 | **Pasar a un patrón Bronze→Silver→Gold (capas medallion)**. Hoy mezclamos limpieza + agregaciones en `master`. | Trazabilidad: la capa Bronze sería la copia fiel del CSV ya tipada; Silver el limpio; Gold los agregados. |
| 2 | **Escritura particionada** (`year=YYYY/month=MM/day=DD`) y formato Iceberg/Delta en lugar de Parquet plano. | Cargas incrementales, time-travel, schema evolution, vacuum. |
| 3 | **Cargas incrementales** vía `data_interval_start/end` o un sensor de objeto en MinIO en vez de full refresh. | Coste, latencia y eliminación del overwrite total. |
| 4 | **Validación con Great Expectations / Soda / pydeequ** como tarea previa al sink. | Frenar el ingreso de datos malos antes de que contaminen Bronze. |
| 5 | **Quarantine bucket** (`bck-quarantine/{batch_id}/...`) para filas rechazadas + métricas de DQ exportadas a Prometheus/StatsD. | Visibilidad: se sabe qué se descartó y por qué. |
| 6 | **Catálogo maestro `dim_company`** desde CRM/ERP, no inferido del transaccional. | Imputación robusta y reproducible. |
| 7 | **Conexiones de Airflow** (`aws_default`, `trino_default`) en vez de credenciales hardcodeadas. | Higiene de secretos, alineado con la respuesta de seguridad del Ej. 2. |
| 8 | **Tests unitarios** sobre las funciones de limpieza (pytest + DataFrames sintéticos). | Confiabilidad ante cambios del schema fuente. |
| 9 | **Sensor de archivo nuevo** en `bck-landing/data/` (S3KeySensor). | Disparo basado en evento, no manual. |
| 10 | **Lineage / OpenLineage** activado en Airflow para trazar columna ↔ origen. | Gobernanza de datos. |
| 11 | **Idempotencia explícita** vía `MERGE` Iceberg sobre `id` en lugar de `DROP/CREATE` Hive. | Reprocesos seguros. |
| 12 | **Métricas de drift** del catálogo `name`/`status` (alerta si aparece un valor nuevo). | Detección temprana de cambios upstream. |

### 5) Captura de pantalla de Trino + DBeaver

Pendiente: una vez levantado el stack, ejecutar el DAG, conectar DBeaver al
endpoint `jdbc:trino://localhost:8090` y guardar la captura como
`ejercicio1/dbeaver_trino.png` (o cualquier nombre `.png/.jpg`) con el
resultado de:

```sql
SELECT * FROM bronze.prueba.tbl_data LIMIT 50;
```

---

---

## Plus — DAG Ecobici (`ecobici_station_status_etl`)

Ingesta el feed GBFS [`station_status`](https://gbfs.mex.lyftbikes.com/gbfs/en/station_status.json)
de Ecobici CDMX (operado por Lyft) **cada 10 minutos**, lo limpia con Polars y
mantiene **un único parquet por día** en MinIO bajo
`bck-bronze/ecobici/station_status/dt=YYYY-MM-DD/data.parquet`. La tabla Trino
expone los datos particionados.

### Tareas del DAG

| Tarea                   | Acción                                                              |
|-------------------------|---------------------------------------------------------------------|
| `ensure_bronze_setup`   | Crea bucket + schema + tabla externa Hive particionada (idempotente).|
| `fetch_station_status`  | Descarga el JSON GBFS.                                              |
| `clean_and_append`      | Lee parquet del día (si existe), une, deduplica por (station_id, last_reported), reescribe **un solo** parquet. |
| `sync_trino_partitions` | `CALL system.sync_partition_metadata` para que Trino vea la partición del día. |

### Respuestas a las preguntas del documento adicional

**¿Qué dataset se seleccionó para tu flujo?**
GBFS *station_status* del sistema Ecobici CDMX. Devuelve, para las ~677
estaciones activas, el número de bicis y docks disponibles/inhabilitados, las
banderas operativas (`is_installed`, `is_renting`, `is_returning`,
`is_charging`) y `last_reported` por estación.

**¿Qué temporalidad se realizará la extracción? Explica por qué se
seleccionó este timing.**
**Cada 10 minutos.** Justificación:

* El feed declara `ttl=10s` y se actualiza con altísima frecuencia, así que
  podría hacerse incluso por minuto, pero 10 min es un buen *trade-off*:
  ~144 snapshots/día (suficiente para análisis de ocupación a nivel
  hora-pico vs. valle) sin saturar la API ni inflar el lake.
* Una estación cambia *de verdad* su estado cada vez que se devuelve o
  toma una bici; en general, un Δ de 10 min captura todos los cambios
  relevantes. La deduplicación por `(station_id, last_reported)` evita
  guardar lecturas idénticas si la estación no cambió.
* No requiere autenticación ni rate-limit conocido; aún así, 10 min es
  conservador y respetuoso del proveedor.

**¿Qué limpieza de datos usaste o crees que necesitaba los datos?**

1. **Cast canónico** de tipos: `station_id` a `varchar`, los counts a
   `int32`, los `is_*` a `boolean` (el feed los emite como 1/0),
   `last_reported` (epoch en segundos) a `timestamp`.
2. **Filtros de validez**: descartar registros con `station_id` o
   `last_reported` nulos, o con conteos negativos (corrupción).
3. **Adición de metadatos**: `snapshot_ts` (cuándo lo trajimos) y `dt`
   (clave de partición YYYY-MM-DD) — separan tiempo de la fuente vs.
   tiempo de ingesta.
4. **Deduplicación** por `(station_id, last_reported)` al hacer el append:
   si el snapshot del día ya tenía esa lectura, no se duplica.

**¿Qué propuesta de partición de ruta elegiste para el guardado de tu
parquet y crees que esta partición afecta a Trino para su disponibilización
automática de datos?**

Partición por día: `bck-bronze/ecobici/station_status/dt=YYYY-MM-DD/data.parquet`.

Sí, **afecta**:

* Con **conector Hive** (el que está configurado), Trino **no** descubre
  particiones nuevas automáticamente. Una carpeta `dt=2026-04-29/` recién
  creada será invisible hasta que se registre en el metastore.
* Por eso el DAG ejecuta al final
  `CALL system.sync_partition_metadata(schema, table, mode => 'ADD')`,
  que añade al metastore las particiones existentes en S3 que aún no
  estaban registradas.
* Si en su lugar usáramos **Iceberg**, no haría falta sync: Iceberg lleva
  el manifiesto de archivos en el catálogo y resuelve particiones por
  *hidden partitioning*. Para producción sería mi recomendación.

**¿De todo el proceso, cuál fue el reto más grande? Explica el por qué.**

El reto fue cumplir simultáneamente con dos requisitos en tensión:

1. **Append continuo** (datos nuevos cada 10 min).
2. **Un único parquet por ruta**.

Parquet no soporta append nativo. La solución fue tratar el parquet del día
como una *tabla pequeña en memoria*: leerla con Polars, concatenar el
snapshot, deduplicar por `(station_id, last_reported)` y reescribir. Esto
mantiene un solo archivo por partición y produce *idempotencia* (correr
dos veces el mismo snapshot no duplica filas), pero requiere atender:

* **Race conditions** si hay corridas concurrentes (mitigado con
  `max_active_runs=1`).
* **Latencia creciente** dentro de un día: el parquet crece a lo largo
  del día y la rescritura cuesta más. A escala, lo correcto sería migrar a
  Iceberg con `MERGE` (compactación periódica), o particionar también por
  hora para acotar el tamaño del archivo a sobrescribir.
* **Eventual consistency** de S3/MinIO: para un solo objeto y un solo
  escritor concurrente no es problema; con múltiples escritores sí.

---

## Estructura del repo (este ejercicio)

```
ejercicio1/
├── Airflow/
│   ├── config/airflow.cfg
│   └── dags/
│       ├── etl_engineer_challenge.py
│       └── ecobici_station_status_etl.py   # Plus
├── Hive/metastore-site.xml
├── Trino/etc/
│   ├── catalog/bronze.properties
│   └── config.properties
├── data_prueba_tecnica.csv
├── docker-compose.yaml
└── README.md
```

## Decisiones técnicas relevantes

* **Polars (Plus del reto)** para todo el cómputo: streaming-friendly, expresiones
  vectorizadas, lazy si crece el dataset.
* **Tabla externa Hive sobre Parquet** (no managed) — el DAG controla el ciclo
  de vida del archivo; Trino sólo lo expone.
* **Un único Parquet** en `bck-bronze/master/` para que la tabla externa apunte
  a la carpeta sin ambigüedad de archivos múltiples.
* **`schema_overrides` + `infer_schema_length=0`**: leemos todo como `Utf8` para
  decidir nosotros las reglas de casteo (evita que Polars rechace todo el CSV
  por un `inf` en `amount`).
