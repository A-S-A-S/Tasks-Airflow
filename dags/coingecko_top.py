import io
import json
import logging
from datetime import timedelta

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.operators.postgres import PostgresOperator


#  Constants 

POSTGRES_CONN_ID = "postgres_default"
POSTGRES_TABLE = "coin_markets"
MINIO_CONN_ID = "minio_conn"

BUCKET_RAW = "raw"
BUCKET_PROCESSED = "processed"

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=volume_desc&per_page=50&page=1"
    "&price_change_percentage=24h"
)

log = logging.getLogger(__name__)


#  Helpers 

def _raw_key(run_id: str) -> str:
    return f"coingecko/{run_id}/raw.json"

def _processed_key(run_id: str) -> str:
    return f"coingecko/{run_id}/processed.parquet"


#  Tasks 

def extract_raw(**context):
    run_id = context["run_id"]
    log.info("Fetching CoinGecko data...")
    resp = requests.get(COINGECKO_URL, timeout=30)
    resp.raise_for_status()
    coins = resp.json()

    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    key = _raw_key(run_id)
    s3.load_bytes(json.dumps(coins, indent=2).encode(), key, BUCKET_RAW)

    log.info("Saved raw data: %s", key)
    context["ti"].xcom_push(key="raw_s3_key", value=key)


def quality_check_raw(**context):
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    key = _raw_key(context["run_id"])
    coins = json.loads(s3.get_key(key, BUCKET_RAW).get()["Body"].read())

    assert len(coins) > 0
    required = {"id", "symbol", "current_price", "total_volume", "price_change_percentage_24h"}
    for coin in coins:
        missing = required - coin.keys()
        assert not missing, f"Missing fields for {coin.get('id')}: {missing}"
    log.info("Raw QC passed: %d coins", len(coins))


def transform(**context):
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    raw_key = context["ti"].xcom_pull(task_ids="extract_raw", key="raw_s3_key")
    df = pd.read_json(io.BytesIO(s3.get_key(raw_key, BUCKET_RAW).get()["Body"].read()))

    df = df[[
        "id", "symbol", "name", "current_price", "total_volume",
        "market_cap", "price_change_percentage_24h", "last_updated"
    ]].copy()

    df.rename(columns={
        "price_change_percentage_24h": "price_change_pct_24h",
        "last_updated": "api_last_updated"
    }, inplace=True)

    df.dropna(subset=["price_change_pct_24h", "current_price"], inplace=True)
    df.sort_values("total_volume", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["volume_rank"] = df.index + 1

    df["etl_run_id"] = context["run_id"]
    df["etl_extracted_at"] = pendulum.now("UTC").isoformat()

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.load_bytes(buffer.getvalue(), _processed_key(context["run_id"]), BUCKET_PROCESSED)

    log.info("Transformed and saved %d records", len(df))


def quality_check_processed(**context):
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    df = pd.read_parquet(io.BytesIO(
        s3.get_key(_processed_key(context["run_id"]), BUCKET_PROCESSED).get()["Body"].read()
    ))

    assert not df.duplicated("id").any()
    assert (df["current_price"] > 0).all()

    expected = set(range(1, len(df) + 1))
    assert expected == set(df["volume_rank"])

    log.info("Processed QC passed: %d records", len(df))


def load_to_postgres(**context):
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    df = pd.read_parquet(io.BytesIO(
        s3.get_key(_processed_key(context["run_id"]), BUCKET_PROCESSED).get()["Body"].read()
    ))

    log.info("Loading %d rows to Postgres...", len(df))

    upsert_sql = f"""
        INSERT INTO {POSTGRES_TABLE} (id, symbol, name, current_price, total_volume, market_cap,
            price_change_pct_24h, api_last_updated, volume_rank, etl_run_id, etl_extracted_at)
        VALUES (%(id)s, %(symbol)s, %(name)s, %(current_price)s, %(total_volume)s,
            %(market_cap)s, %(price_change_pct_24h)s, %(api_last_updated)s,
            %(volume_rank)s, %(etl_run_id)s, %(etl_extracted_at)s)
        ON CONFLICT (id) DO UPDATE SET
            symbol=EXCLUDED.symbol, name=EXCLUDED.name, current_price=EXCLUDED.current_price,
            total_volume=EXCLUDED.total_volume, market_cap=EXCLUDED.market_cap,
            price_change_pct_24h=EXCLUDED.price_change_pct_24h,
            api_last_updated=EXCLUDED.api_last_updated,
            volume_rank=EXCLUDED.volume_rank,
            etl_run_id=EXCLUDED.etl_run_id,
            etl_extracted_at=EXCLUDED.etl_extracted_at;
    """

    records = df.to_dict(orient="records")

    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_conn = psycopg2.connect(
        host=conn.host, port=conn.port or 5432, dbname=conn.schema,
        user=conn.login, password=conn.password
    )
    try:
        with pg_conn.cursor() as cur:
            cur.executemany(upsert_sql, records)
            pg_conn.commit()
        log.info("Upserted %d rows successfully", len(records))
    finally:
        pg_conn.close()


def notify_on_failure(**context):
    log.error("DAG FAILED | run=%s | task=%s", context["run_id"], context["task_instance"].task_id)


#  DAG Definition 

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="coingecko_etl",
    description="CoinGecko Top 50 ETL",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["coingecko"],
) as dag:

    t_extract = PythonOperator(task_id="extract_raw", python_callable=extract_raw)
    t_qc_raw = PythonOperator(task_id="quality_check_raw", python_callable=quality_check_raw)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_qc_processed = PythonOperator(task_id="quality_check_processed", python_callable=quality_check_processed)

    t_create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (...);",  # keep your full CREATE TABLE
    )

    t_load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    t_notify = PythonOperator(
        task_id="notify_on_failure",
        python_callable=notify_on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    t_extract >> t_qc_raw >> t_transform >> t_qc_processed >> t_create_table >> t_load
    [t_extract, t_qc_raw, t_transform, t_qc_processed, t_create_table, t_load] >> t_notify