import logging
from datetime import timedelta, datetime, UTC
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

def alert_on_failure(**context):
        logging.error("Spark job failed - sending notification")
        print("Spark job failed - sending notification")

with DAG(
    dag_id="spark_orchestration",
    description="Task 3 - Orchestrating Spark Jobs",
    schedule=None,
    start_date = datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    default_args=default_args,
    tags=["spark"],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="/opt/airflow/spark/coingecko_spark_job.py",
        conn_id="spark_default",
        application_args=[
            "s3a://processed/coingecko/{{ ds }}/processed.parquet", 
            "s3a://processed/coingecko/{{ ds }}/spark_summary.parquet"
        ],
        conf={
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
    )

    failure_alert = PythonOperator(
        task_id="failure_alert",
        python_callable=alert_on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

submit_spark_job >> failure_alert
