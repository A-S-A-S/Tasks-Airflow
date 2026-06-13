import datetime
import io
import json
import logging
from datetime import timedelta
from datetime import time

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.python import PythonSensor


#  Constants

POSTGRES_CONN_ID = "postgres_default"
POSTGRES_TABLE = "coin_markets"
MINIO_CONN_ID = "minio_conn"

BUCKET_RAW = "raw"
BUCKET_PROCESSED = "processed"
BUCKET_FAILED = "failed"       

HIGHCAP_THRESHOLD_USD = 1_000_000_000

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=volume_desc&per_page=50&page=1"
    "&price_change_percentage=24h"
)

log = logging.getLogger(__name__)


#  Helpers 

def _raw_key(ds: str) -> str:
    return f"coingecko/{ds}/raw.json"

def _highcap_key(ds: str) -> str:
    return f"coingecko/{ds}/highcap.parquet"

def _lowcap_key(ds: str) -> str:
    return f"coingecko/{ds}/lowcap.parquet"

def _processed_key(ds: str) -> str:
    return f"coingecko/{ds}/processed.parquet"

def _failed_key(ds: str) -> str:
    return f"coingecko/{ds}/quarantine.parquet"

def _base_transform(df: pd.DataFrame, ds: str) -> pd.DataFrame:
    df = df[[
        "id", "symbol", "name", "current_price", "total_volume",
        "market_cap", "price_change_percentage_24h", "last_updated",
    ]].copy()
    df.rename(columns={
        "price_change_percentage_24h": "price_change_pct_24h",
        "last_updated": "api_last_updated",
    }, inplace=True)
    df["api_last_updated"] = (
        pd.to_datetime(df["api_last_updated"], utc=True)
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )
    df.dropna(subset=["price_change_pct_24h", "current_price"], inplace=True)
    df["etl_run_id"] = ds
    df["etl_extracted_at"] = pendulum.now("UTC").isoformat()
    return df

#  Tasks 

def create_table_if_not_exists(**context):
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_conn = psycopg2.connect(
        host=conn.host, port=conn.port or 5432, dbname=conn.schema,
        user=conn.login, password=conn.password,
    )
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
                    id                   VARCHAR(100) PRIMARY KEY,
                    symbol               VARCHAR(20)  NOT NULL,
                    name                 VARCHAR(200) NOT NULL,
                    current_price        NUMERIC(20, 8),
                    total_volume         NUMERIC(30, 2),
                    market_cap           NUMERIC(30, 2),
                    price_change_pct_24h NUMERIC(10, 4),
                    api_last_updated     TIMESTAMPTZ,
                    volume_rank          INTEGER,
                    subset               VARCHAR(10),
                    etl_run_id           TEXT,
                    etl_extracted_at     TIMESTAMPTZ,
                    inserted_at          TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            pg_conn.commit()
        log.info("Table '%s' ensured", POSTGRES_TABLE)
    finally:
        pg_conn.close()


def extract_raw(**context):
    """Task A — fetch top 50 coins from CoinGecko and persist raw JSON to MinIO."""
    ds = context["ds"]
    log.info("Fetching CoinGecko data...")
    resp = requests.get(COINGECKO_URL, timeout=30)
    resp.raise_for_status()
    coins = resp.json()

    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    key = _raw_key(ds)
    s3.load_bytes(json.dumps(coins, indent=2).encode(), key, BUCKET_RAW, replace=True)
    log.info("Saved %d coins → %s", len(coins), key)
    context["ti"].xcom_push(key="raw_s3_key", value=key)


def quality_check_raw(**context):
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    key = _raw_key(context["ds"])
    coins = json.loads(s3.get_key(key, BUCKET_RAW).get()["Body"].read())

    if len(coins) == 0:
        raise ValueError("Raw QC failed: API returned 0 records")
    required = {"id", "symbol", "current_price", "total_volume", "price_change_percentage_24h"}
    for coin in coins:
        missing = required - coin.keys()
        if missing:
            raise ValueError(f"Raw QC failed: missing fields for {coin.get('id')}: {missing}")
    log.info("Raw QC passed: %d coins", len(coins))


def transform_highcap(**context):
    """Task B — coins with market_cap >= HIGHCAP_THRESHOLD_USD."""
    ds = context["ds"]
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    raw = json.loads(s3.get_key(_raw_key(ds), BUCKET_RAW).get()["Body"].read())

    df = pd.DataFrame(raw)
    before = len(df)
    df = df[df["market_cap"] >= HIGHCAP_THRESHOLD_USD]
    df = _base_transform(df, ds)
    df.sort_values("total_volume", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["volume_rank"] = df.index + 1
    df["subset"] = "highcap"

    buf = io.BytesIO()
    df.to_parquet(
        buf,
        index=False,
        engine="pyarrow",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )
    s3.load_bytes(buf.getvalue(), _highcap_key(ds), BUCKET_PROCESSED, replace=True)
    log.info("High-cap transform: %d → %d rows", before, len(df))


def transform_lowcap(**context):
    """Task C — coins with market_cap < HIGHCAP_THRESHOLD_USD."""
    ds = context["ds"]
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    raw = json.loads(s3.get_key(_raw_key(ds), BUCKET_RAW).get()["Body"].read())

    df = pd.DataFrame(raw)
    before = len(df)
    df = df[df["market_cap"] < HIGHCAP_THRESHOLD_USD]
    df.sort_values("total_volume", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["volume_rank"] = df.index + 1
    df["subset"] = "lowcap"
    df = _base_transform(df, ds)

    buf = io.BytesIO()
    df.to_parquet(
        buf,
        index=False,
        engine="pyarrow",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )
    s3.load_bytes(buf.getvalue(), _lowcap_key(ds), BUCKET_PROCESSED, replace=True)
    log.info("Low-cap transform: %d → %d rows", before, len(df))


def quality_check_and_merge(**context):
    """
    Reads both subset parquets, validates, merges into processed.parquet.
    Pushes 'qc_passed' XCom so branch_on_quality can route accordingly.
    Trigger rule: ALL_SUCCESS — only runs if both B and C succeeded.
    """
    ds = context["ds"]
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)

    df_high = pd.read_parquet(io.BytesIO(
        s3.get_key(_highcap_key(ds), BUCKET_PROCESSED).get()["Body"].read()
    ))
    df_low = pd.read_parquet(io.BytesIO(
        s3.get_key(_lowcap_key(ds), BUCKET_PROCESSED).get()["Body"].read()
    ))

    df = pd.concat([df_high, df_low], ignore_index=True)
    issues = []

    if df.duplicated("id").any():
        issues.append(f"{df.duplicated('id').sum()} duplicate coin IDs")
    if not (df["current_price"] > 0).all():
        issues.append("one or more coins have price <= 0")
    null_pct = df["market_cap"].isna().mean()
    if null_pct > 0.10:
        issues.append(f"market_cap null rate {null_pct:.1%} exceeds 10% threshold")

    buf = io.BytesIO()
    df.to_parquet(
        buf,
        index=False,
        engine="pyarrow",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )

    if issues:
        s3.load_bytes(buf.getvalue(), _failed_key(ds), BUCKET_FAILED, replace=True)
        log.warning("Processed QC FAILED: %s — quarantined to %s", issues, BUCKET_FAILED)
        context["ti"].xcom_push(key="qc_passed", value=False)
        context["ti"].xcom_push(key="qc_issues", value=issues)
    else:
        s3.load_bytes(buf.getvalue(), _processed_key(ds), BUCKET_PROCESSED, replace=True)
        log.info("Processed QC passed: %d total records", len(df))
        context["ti"].xcom_push(key="qc_passed", value=True)


def branch_on_quality(**context):
    """
    Task F — BranchPythonOperator.
    Returns the task_id Airflow should follow; the other branch is skipped.
    """
    qc_passed = context["ti"].xcom_pull( 
        task_ids="quality_check_and_merge", key="qc_passed"
    )
    if qc_passed:
        log.info("Branch → load path")
        return "create_table"
    log.warning("Branch → quarantine path")
    return "quarantine_alert"


def load_to_postgres(**context):
    """Task D — loads merged parquet. Only reached via the passing branch."""
    ds = context["ds"]
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    df = pd.read_parquet(io.BytesIO(
        s3.get_key(_processed_key(ds), BUCKET_PROCESSED).get()["Body"].read()
    ))
    log.info("Loading %d rows to Postgres...", len(df))

    df = df.astype(object).where(df.notna(), None)

    upsert_sql = f"""
        INSERT INTO {POSTGRES_TABLE}
            (id, symbol, name, current_price, total_volume, market_cap,
             price_change_pct_24h, api_last_updated, volume_rank, subset,
             etl_run_id, etl_extracted_at)
        VALUES
            (%(id)s, %(symbol)s, %(name)s, %(current_price)s, %(total_volume)s,
             %(market_cap)s, %(price_change_pct_24h)s, %(api_last_updated)s,
             %(volume_rank)s, %(subset)s, %(etl_run_id)s, %(etl_extracted_at)s)
        ON CONFLICT (id) DO UPDATE SET
            symbol               = EXCLUDED.symbol,
            name                 = EXCLUDED.name,
            current_price        = EXCLUDED.current_price,
            total_volume         = EXCLUDED.total_volume,
            market_cap           = EXCLUDED.market_cap,
            price_change_pct_24h = EXCLUDED.price_change_pct_24h,
            api_last_updated     = EXCLUDED.api_last_updated,
            volume_rank          = EXCLUDED.volume_rank,
            subset               = EXCLUDED.subset,
            etl_run_id           = EXCLUDED.etl_run_id,
            etl_extracted_at     = EXCLUDED.etl_extracted_at;
    """
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_conn = psycopg2.connect(
        host=conn.host, port=conn.port or 5432, dbname=conn.schema,
        user=conn.login, password=conn.password,
    )
    try:
        with pg_conn.cursor() as cur:
            cur.executemany(upsert_sql, df.to_dict(orient="records"))
            pg_conn.commit()
        log.info("Upserted %d rows successfully", len(df))
    finally:
        pg_conn.close()


def quarantine_alert(**context):
    issues = context["ti"].xcom_pull(
        task_ids="quality_check_and_merge", key="qc_issues"
    )
    log.error(
        "QUARANTINE | run=%s | data written to '%s' bucket | issues=%s",
        context["ds"], BUCKET_FAILED, issues,
    )


def audit_log(**context):
    """Task E — always runs (ALL_DONE). Summarises the run outcome."""
    ti = context["ti"]
    qc_passed = ti.xcom_pull(task_ids="quality_check_and_merge", key="qc_passed")
    issues = ti.xcom_pull(task_ids="quality_check_and_merge", key="qc_issues") or []
    outcome = "LOADED" if qc_passed else "QUARANTINED"
    log.info(
        "AUDIT | run=%s | outcome=%s | issues=%s",
        context["ds"], outcome, issues,
    )

def wait_for_market_settle(**context):
    """Wait until 17:55 UTC on scheduled runs. Skip immediately on manual runs."""
    dag_run = context.get("dag_run")
    
    if not dag_run or dag_run.run_type != "scheduled":
        logging.info("Manual / test run detected → skipping market settle wait")
        return True

    try:
        now = pendulum.now("UTC")
        current_time = now.time()
        target_time = datetime.time(17, 55)

        if current_time >= target_time:
            logging.info(f"Market settle time reached ({current_time}). Proceeding.")
            return True
        else:
            logging.info(f"Still waiting... Current time: {current_time}, target: {target_time}")
            return False

    except Exception as e:  # Safety net
        logging.error(f"Error in wait_for_market_settle: {e}")
        raise 

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
    schedule="0 18 * * 5",   # Every Friday at 18:00 UTC
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["coingecko"],
) as dag:

    t_wait_market = PythonSensor(
        task_id="wait_for_market_settle",
        python_callable=wait_for_market_settle,
        mode="reschedule",
        timeout=3600,                # 1 hour max
        poke_interval=60,
    )

    # Task A
    t_extract = PythonOperator(
        task_id="extract_raw",
        python_callable=extract_raw,
    )
    t_qc_raw = PythonOperator(
        task_id="quality_check_raw",
        python_callable=quality_check_raw,
    )

    # Tasks B + C — parallel inside TaskGroup
    with TaskGroup("transform_group") as tg_transform:
        t_transform_high = PythonOperator(
            task_id="transform_highcap",
            python_callable=transform_highcap,
        )
        t_transform_low = PythonOperator(
            task_id="transform_lowcap",
            python_callable=transform_lowcap,
        )

    # QC + merge — ALL_SUCCESS gate (both B and C must succeed)
    t_qc_merge = PythonOperator(
        task_id="quality_check_and_merge",
        python_callable=quality_check_and_merge,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task F — branch on QC result
    t_branch = BranchPythonOperator(
        task_id="branch_on_quality",
        python_callable=branch_on_quality,
    )

    # Happy path: create table → load (Task D)
    t_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_if_not_exists,
    )
    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Quarantine path
    t_quarantine = PythonOperator(
        task_id="quarantine_alert",
        python_callable=quarantine_alert,
    )

    # Task E — always runs regardless of which branch was taken
    t_audit = PythonOperator(
        task_id="audit_log",
        python_callable=audit_log,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_wait_market >> t_extract >> t_qc_raw >> tg_transform >> t_qc_merge >> t_branch
    t_branch >> t_create_table >> t_load >> t_audit
    t_branch >> t_quarantine >> t_audit