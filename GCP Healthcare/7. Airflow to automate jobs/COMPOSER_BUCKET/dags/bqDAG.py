from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Production Constants
PROJECT_ID = "avd-databricks-demo"
LOCATION = "US"  # BigQuery dataset location (multi-region US)

# Default configuration applied across all operators
DEFAULT_ARGS = {
    "owner": "DEBABRATA ADAK",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Industry Best Practice: Define template_searchpath to load SQL files safely via Jinja.
# Airflow automatically scans this path relative to your environment's dags folder.
with DAG(
    dag_id="bigquery_medallion_pipeline",
    schedule_interval=None,
    description="Orchestrates Bronze, Silver, and Gold BigQuery layers sequentially.",
    default_args=DEFAULT_ARGS,
    catchup=False,
    template_searchpath=["/home/airflow/gcs/data/BQ/"], 
    tags=["gcp", "bigquery", "etl", "medallion"]
) as dag:

    # 1. Bronze Layer Task
    run_bronze_layer = BigQueryInsertJobOperator(
        task_id="run_bronze_layer",
        configuration={
            "query": {
                "query": "bronze.sql",  # Loaded natively via Jinja template engine
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location=LOCATION
    )
			
    # 2. Silver Layer Task
    run_silver_layer = BigQueryInsertJobOperator(
        task_id="run_silver_layer",
        configuration={
            "query": {
                "query": "silver.sql",  # Loaded natively via Jinja template engine
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location=LOCATION
    )
			
    # 3. Gold Layer Task
    run_gold_layer = BigQueryInsertJobOperator(
        task_id="run_gold_layer",
        configuration={
            "query": {
                "query": "gold.sql",  # Loaded natively via Jinja template engine
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location=LOCATION
    )

    # Clean, explicit downstream dependency pipeline execution
    run_bronze_layer >> run_silver_layer >> run_gold_layer
