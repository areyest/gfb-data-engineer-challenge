# GFB · Data Engineer Challenge

Solución a los dos ejercicios (más el **Plus de Ecobici**) del Data Engineer
Challenge de Banco BASE / GFB.

## Estructura

```
.
├── ejercicio1/                     # Pipeline ETL Airflow + MinIO + Trino (Polars)
│   ├── Airflow/dags/
│   │   ├── etl_engineer_challenge.py
│   │   └── ecobici_station_status_etl.py     # Plus
│   ├── Hive/metastore-site.xml
│   ├── Trino/etc/...
│   ├── data_prueba_tecnica.csv
│   ├── docker-compose.yaml
│   └── README.md                  # Detalle del pipeline + respuestas a las preguntas
├── ejercicio2/                     # Diseño de arquitectura (sólo propuesta)
│   ├── diagrama.svg
│   └── justificacion_de_tu_arquitectura.pdf
└── README.md                       # (este archivo)
```

## Ejercicio 1 — Resumen

Pipeline ETL que ingesta `data_prueba_tecnica.csv` (10,000 transacciones), lo
limpia con **Polars** (Plus del reto), lo materializa como Parquet en MinIO y
lo expone en **Trino** vía una tabla externa Hive `bronze.prueba.tbl_data`.

Datos clave del *profiling*:

* **3** filas con `id` nulo (descartadas).
* **5** filas con `name` corrupto/vacío y **5** con `company_id` corrupto/vacío
  → imputación cruzada usando los pares íntegros como diccionario.
* **5** valores absurdos en `amount` (`inf`, `3e+34`, `2.13e+18`, …) → `NULL`.
* **2** valores corruptos en `status` (`p&0x3fid`, `0xFFFF`) → `unknown`.
* **3** `created_at` con formatos no estándar (`YYYYMMDD`, `ISO con T`) →
  parser multi-formato.

Resultado: **9,997 filas limpias**, 2 marcas (`MiPasajefy`, `Muebles chidos`),
9 estatus (8 válidos + `unknown`), agregaciones por `(name, created_at)`
embebidas como columnas para consulta directa en Trino.

Detalles, decisiones y respuestas a las **preguntas adicionales** del reto:
ver [`ejercicio1/README.md`](ejercicio1/README.md).

## Ejercicio 2 — Resumen

Arquitectura **Lakehouse Iceberg** sobre object storage, alimentada por:

* **Debezium + Kafka** (CDC) para F2 (SQL Server) y F3 (PostgreSQL).
* **Airbyte** para F1 (CRM propietario) con extracción incremental.

Capas medallion (Bronze → Silver → Gold) con **Spark / dbt-trino** como motor
de transformación, **Trino** para consumo SQL operativo y **Spark / Polars +
Neo4j** para ciencia de datos (clustering / grafos).

Cubre las preguntas I.A–I (subconjuntos, retos, mitigación de impacto, etapas,
herramientas, storage, orquestación, diagrama), II.A (seguridad end-to-end) y
III.A (gobernanza y metadata).

Entregables: [`ejercicio2/diagrama.svg`](ejercicio2/diagrama.svg) y
[`ejercicio2/justificacion_de_tu_arquitectura.pdf`](ejercicio2/justificacion_de_tu_arquitectura.pdf).

## Plus — DAG Ecobici

DAG `ecobici_station_status_etl` que ingesta el feed GBFS
`station_status` de Ecobici CDMX **cada 10 min**, limpia con Polars y mantiene
**un único parquet por día** en
`bck-bronze/ecobici/station_status/dt=YYYY-MM-DD/data.parquet` (append por
deduplicación). La tabla `bronze.ecobici.station_status` queda particionada
por `dt`.

Detalle y respuestas en
[`ejercicio1/README.md`](ejercicio1/README.md#plus--dag-ecobici-ecobici_station_status_etl).

## Cómo ejecutar (resumen)

```bash
cd ejercicio1
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d

# Esperar a que airflow-apiserver esté healthy y entrar a la UI:
#   http://localhost:8080      (airflow / airflow)
#   http://localhost:9001      (minio  / minio1234)
#   jdbc:trino://localhost:8090  (root, sin password)

docker compose exec airflow-scheduler airflow dags unpause etl_engineer_challenge
docker compose exec airflow-scheduler airflow dags trigger etl_engineer_challenge
```

Para más detalle (verificación, troubleshooting y captura DBeaver) ver
[`ejercicio1/README.md`](ejercicio1/README.md).

## Stack utilizado

* **Apache Airflow 3.2** (CeleryExecutor + Postgres + Redis)
* **MinIO** (object storage S3-compatible)
* **Trino 467** + **Hive Metastore 3** + **MySQL 8** (catálogo)
* **Polars 1.x** para procesamiento (Plus del reto)
* **boto3** para operaciones de control plane S3 (creación de buckets, upload)
