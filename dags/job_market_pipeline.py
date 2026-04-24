"""
Main Airflow DAG: Job Market Intelligence Pipeline
Schedule: daily at 6am UTC
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "grace",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="job_market_pipeline",
    default_args=default_args,
    description="ETL: Kaggle/Adzuna → Snowflake → dbt → analytics",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["job-market", "etl"],
) as dag:

    # ── Task 1: Download fresh data (Kaggle static or Adzuna API) ──────────
    download_data = BashOperator(
        task_id="download_data",
        bash_command="python /opt/airflow/scripts/download_data.py",
    )

    # ── Task 2: Upload raw CSVs to Snowflake RAW schema ───────────────────
    upload_to_snowflake = BashOperator(
        task_id="upload_to_snowflake",
        bash_command="python /opt/airflow/scripts/upload_to_snowflake.py",
    )

    # ── Task 3: Run dbt transformations (staging + marts) ─────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --profiles-dir /opt/airflow/dbt_project --target prod"
        ),
    )

    # ── Task 4: Run dbt tests ──────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir /opt/airflow/dbt_project --target prod"
        ),
    )

    # ── Task 5: Refresh skill embeddings for RAG (Phase 4) ────────────────
    # Disabled until Phase 4; kept as placeholder
    # refresh_embeddings = BashOperator(
    #     task_id="refresh_embeddings",
    #     bash_command="python /opt/airflow/scripts/refresh_embeddings.py",
    # )

    download_data >> upload_to_snowflake >> dbt_run >> dbt_test
