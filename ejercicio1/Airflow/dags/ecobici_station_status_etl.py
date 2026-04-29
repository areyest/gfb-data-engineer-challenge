"""
DAG: ecobici_station_status_etl   (Ejercicio adicional - Plus)

Ingesta el feed GBFS de Ecobici CDMX (operado por Lyft) cada 10 minutos,
limpia con Polars y mantiene UN solo parquet por día en MinIO bajo
`bck-bronze/ecobici/station_status/dt=YYYY-MM-DD/data.parquet` (modo append:
cada corrida lee el parquet del día, deduplica por (station_id, last_reported)
y reescribe el archivo único).

La capa de consulta es una tabla Hive externa particionada por `dt`. Como Hive
no descubre particiones nuevas automáticamente, una tarea final llama a
`system.sync_partition_metadata` para que las particiones del día estén
visibles en Trino.

Feed elegido: station_status (estado en tiempo real). Justificación:
  * ttl = 10s, se actualiza con altísima frecuencia.
  * Polling cada 10 min produce ~144 snapshots/día, suficiente para análisis
    de ocupación sin saturar la API ni el lake.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import BytesIO

from airflow.decorators import dag, task

GBFS_STATION_STATUS = "https://gbfs.mex.lyftbikes.com/gbfs/en/station_status.json"

MINIO_ENDPOINT_INTERNAL = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"

BRONZE_BUCKET = "bck-bronze"
BRONZE_PREFIX = "ecobici/station_status"  

TRINO_HOST = "coordinator"
TRINO_PORT = 8080
TRINO_USER = "root"
TRINO_CATALOG = "bronze"
TRINO_SCHEMA = "ecobici"
TRINO_TABLE = "station_status"


def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_INTERNAL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


@dag(
    dag_id="ecobici_station_status_etl",
    description="Ingesta GBFS station_status de Ecobici CDMX cada 10 min",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["plus", "ecobici", "polars", "minio", "trino"],
)
def ecobici_station_status_etl():

    @task
    def ensure_bronze_setup() -> str:
        """Garantiza el bucket bronze, el schema y la tabla externa Hive."""
        from botocore.exceptions import ClientError
        from trino.dbapi import connect

        s3 = _s3_client()
        try:
            s3.head_bucket(Bucket=BRONZE_BUCKET)
        except ClientError:
            s3.create_bucket(Bucket=BRONZE_BUCKET)

        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
        cur = conn.cursor()

        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}
            WITH (location = 's3a://{BRONZE_BUCKET}/ecobici/')
            """
        )
        cur.fetchall()

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} (
                station_id varchar,
                num_bikes_available integer,
                num_bikes_disabled integer,
                num_docks_available integer,
                num_docks_disabled integer,
                is_installed boolean,
                is_renting boolean,
                is_returning boolean,
                is_charging boolean,
                eightd_has_available_keys boolean,
                last_reported timestamp(3),
                snapshot_ts timestamp(3),
                dt varchar
            )
            WITH (
                external_location = 's3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}',
                format = 'PARQUET',
                partitioned_by = ARRAY['dt']
            )
            """
        )
        cur.fetchall()
        return f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}"

    @task
    def fetch_station_status() -> dict:
        """Descarga el JSON crudo y lo devuelve para la siguiente tarea."""
        import urllib.request

        req = urllib.request.Request(
            GBFS_STATION_STATUS,
            headers={"User-Agent": "ecobici-airflow-etl/1.0"},
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            import json

            payload = json.loads(r.read().decode("utf-8"))
        n = len(payload.get("data", {}).get("stations", []))
        if n == 0:
            raise ValueError("Feed GBFS sin estaciones — abortando")
        print(f"Descargadas {n} estaciones, last_updated={payload.get('last_updated')}")
        return payload

    @task
    def clean_and_append(payload: dict) -> str:
        import polars as pl

        snapshot_ts = datetime.fromtimestamp(
            payload["last_updated"], tz=timezone.utc
        ).replace(tzinfo=None)
        dt_part = snapshot_ts.strftime("%Y-%m-%d")
        key = f"{BRONZE_PREFIX}/dt={dt_part}/data.parquet"

        stations = payload["data"]["stations"]
        df_new = pl.DataFrame(stations)

        for col, dtype in {
            "station_id": pl.Utf8,
            "num_bikes_available": pl.Int32,
            "num_bikes_disabled": pl.Int32,
            "num_docks_available": pl.Int32,
            "num_docks_disabled": pl.Int32,
            "last_reported": pl.Int64,
        }.items():
            if col in df_new.columns:
                df_new = df_new.with_columns(pl.col(col).cast(dtype, strict=False))


        for col in ("is_installed", "is_renting", "is_returning"):
            if col in df_new.columns:
                df_new = df_new.with_columns(pl.col(col).cast(pl.Int8, strict=False).cast(pl.Boolean))
        for col in ("is_charging", "eightd_has_available_keys"):
            if col in df_new.columns:
                df_new = df_new.with_columns(pl.col(col).cast(pl.Boolean, strict=False))
            else:
                df_new = df_new.with_columns(pl.lit(None).cast(pl.Boolean).alias(col))

        df_new = df_new.filter(
            pl.col("station_id").is_not_null()
            & pl.col("last_reported").is_not_null()
            & (pl.col("num_bikes_available") >= 0)
            & (pl.col("num_docks_available") >= 0)
        )

        df_new = df_new.with_columns(
            pl.from_epoch(pl.col("last_reported"), time_unit="s")
              .cast(pl.Datetime("ms"))
              .alias("last_reported"),
            pl.lit(snapshot_ts).cast(pl.Datetime("ms")).alias("snapshot_ts"),
            pl.lit(dt_part).alias("dt"),
        )

        wanted_cols = [
            "station_id",
            "num_bikes_available",
            "num_bikes_disabled",
            "num_docks_available",
            "num_docks_disabled",
            "is_installed",
            "is_renting",
            "is_returning",
            "is_charging",
            "eightd_has_available_keys",
            "last_reported",
            "snapshot_ts",
            "dt",
        ]
        df_new = df_new.select([c for c in wanted_cols if c in df_new.columns])

        s3 = _s3_client()
        existing = None
        try:
            obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
            existing = pl.read_parquet(BytesIO(obj["Body"].read()))
        except s3.exceptions.NoSuchKey:
            existing = None
        except Exception as e:
            print(f"WARN: no pude leer el parquet previo ({key}): {e}")
            existing = None

        if existing is not None and existing.height > 0:
            common = [c for c in df_new.columns if c in existing.columns]
            df_combined = pl.concat([existing.select(common), df_new.select(common)])
        else:
            df_combined = df_new

        df_combined = df_combined.unique(
            subset=["station_id", "last_reported"], keep="last"
        ).sort(["last_reported", "station_id"])

        buf = BytesIO()
        df_combined.write_parquet(buf, compression="snappy")
        buf.seek(0)
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )

        print(
            f"Partición dt={dt_part}: existing={0 if existing is None else existing.height} "
            f"+ new={df_new.height} -> total dedup={df_combined.height}"
        )
        return f"s3://{BRONZE_BUCKET}/{key}"

    @task
    def sync_trino_partitions() -> str:

        from trino.dbapi import connect

        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
        cur = conn.cursor()
        cur.execute(
            f"""
            CALL system.sync_partition_metadata(
                schema_name => '{TRINO_SCHEMA}',
                table_name => '{TRINO_TABLE}',
                mode => 'ADD'
            )
            """
        )
        cur.fetchall()
        return "partitions synced"

    setup = ensure_bronze_setup()
    payload = fetch_station_status()
    parquet_path = clean_and_append(payload)
    sync = sync_trino_partitions()

    setup >> payload >> parquet_path >> sync


ecobici_station_status_etl()
