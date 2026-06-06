# Task 1: Build a Multi-Operator ETL Pipeline
Requirements:
> Create a DAG that implements a complete ETL workflow:

> Extract data from a REST API  <br/>

Using api from coingecko.com <br/>

> Transform data using PythonOperator (clean, filter, aggregate) <br/>

See transform task <br/>

> Load data into a database (use appropriate Operator)

See load_to_postgres task

> Implement proper task dependencies

> Use appropriate schedule_interval 

> Add retry logic and timeout configuration

> Implement data quality checks as separate tasks

# Task 2: Implement Advanced Task Dependencies
Requirements:
- Create a DAG with complex dependency patterns:
- Branching: Use BranchPythonOperator to conditionally execute tasks
- Trigger Rules: Implement tasks with different trigger rules (all_success, one_failed, all_done)
- Task Groups: Organize related tasks into TaskGroups
- SubDAGs or Sensors: Wait for external conditions
Implement a scenario where:
- Task A extracts data
- Task B and C process different subsets in parallel
- Task D only runs if both B and C succeed
- Task E runs regardless of B/C outcome 
- Task F branches based on data quality results

# Task 3: Orchestrating Spark Jobs
Requirements:
- Create DAGs that orchestrate Spark jobs: 
- Submit Spark jobs to local cluster 
- Monitor job execution and handle failures
- Manage Spark application resources
Implement:
- SparkSubmitOperator for job submission
- Sensor to wait for job completion
- Process large datasets (>1GB)
- Optimize Spark job configuration from Airflow

6 tasks:
extract_raw -> quality_check_raw -> transform -> quality_check_processed -> load -> notify_on_failure

# Note
Need to create following connections in Airflow UI:
- Conn Id: minio_conn; Conn Type Amazon Web Services; Host empty; Login (AWS Access Key ID) from .env Password (AWS Secret Access Key) from .env;
- Conn Id: postgres_default; Conn Type Postgres; Host postgres; Database airflow; Login from .env Password from .env; port 5432
