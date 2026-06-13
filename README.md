# Task 1: Build a Multi-Operator ETL Pipeline
Requirements:
> Create a DAG that implements a complete ETL workflow:
> Extract data from a REST API  <br/>

Using api from coingecko.com with a 30s timeout and raise_for_status(). Raw JSON is immediately persisted to MinIO before any processing, so re-runs don't re-hit the API unnecessarily<br/>

> Transform data using PythonOperator (clean, filter, aggregate) <br/>

See `transform` task <br/>

> Load data into a database (use appropriate Operator)

See `load_to_postgres` task. Upsert pattern (ON CONFLICT ... DO UPDATE) for idempotent reruns<br/>

> Implement proper task dependencies

The linear chain extract was on the first iteration, see task 2 for changes

> Use appropriate schedule_interval 

schedule="0 18 * * 5", just because

> Add retry logic and timeout configuration

retries: 2, retry_delay: timedelta(minutes=5) and execution_timeout: timedelta(minutes=10) are set in default_args and apply to all tasks

> Implement data quality checks as separate tasks

Two dedicated QC tasks exist: quality_check_raw and quality_check_processed 

# Task 2: Implement Advanced Task Dependencies
Requirements:
> Create a DAG with complex dependency patterns:
> Branching: Use BranchPythonOperator to conditionally execute tasks

BranchPythonOperator (branch_on_quality)

> Trigger Rules: Implement tasks with different trigger rules (all_success, one_failed, all_done)

ALL_SUCCESS, ALL_DONE — done

> Task Groups: Organize related tasks into TaskGroups

See `transform_group`

> SubDAGs or Sensors: Wait for external conditions

See `t_wait_market` PythonSensor. It would make more sense to trigger coingecko_etl in spark_orchestration, but I'll leave it as is

> Implement a scenario where:
> Task A extracts data
> Task B and C process different subsets in parallel
> Task D only runs if both B and C succeed
> Task E runs regardless of B/C outcome 
> Task F branches based on data quality results

# Task 3: Orchestrating Spark Jobs
Requirements:
> Create DAGs that orchestrate Spark jobs
> Submit Spark jobs to local cluster

`spark_orchestration_dag.py` uses `SparkSubmitOperator` to submit `spark/coingecko_spark_job.py`

> Monitor job execution and handle failures

The Spark job reads `s3a://processed/coingecko/{{ ds }}/processed.parquet` and writes summary output to `s3a://processed/coingecko/{{ ds }}/spark_summary.parquet`. Failure monitoring is implemented with a follow-up `failure_alert` task

> Manage Spark application resources
Implement:
- SparkSubmitOperator for job submission
- Sensor to wait for job completion
- Process large datasets (>1GB)
- Optimize Spark job configuration from Airflow

# Note
Need to create following connections in Airflow UI:
- Conn Id: minio_conn; Conn Type Amazon Web Services; Host empty; Login (AWS Access Key ID) from .env Password (AWS Secret Access Key) from .env; extra: {  "endpoint_url": "http://minio:9000" }
- Conn Id: postgres_default; Conn Type Postgres; Host postgres; Database airflow; Login from .env Password from .env; port 5432; Extra: { "endpoint_url": "http://minio:9000" }
- Conn Id: spark_default; Conn Type Spark; Host spark://spark-master; port 7077;