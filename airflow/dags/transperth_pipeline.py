"""
airflow/dags/transperth_pipeline_dag.py
----------------------------------------
Orchestrates the full Transperth GTFS pipeline:
  1. Ingest raw GTFS data from Transperth (main.py)
  2. Upload CSVs to BigQuery bronze dataset
  3. Run dbt models (bronze → silver → gold)
  4. Run dbt tests
  5. Notify on success

Schedule: daily at 6am Perth time (UTC+8 = UTC 22:00 previous day)
"""

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from dbt import task
from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parents[2]          # transperth_pipeline/
DBT_PROJECT  = PROJECT_ROOT / "dbt_transperth"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,                     # each run is independent
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,                                 # retry once on failure
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="transperth_pipeline",
    description="End-to-end Transperth GTFS pipeline: ingest → BigQuery → dbt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 22 * * *",                      # 6am Perth time (UTC+8)
    catchup=False,
    tags=["transperth", "gtfs", "dbt", "bigquery"],
) as dag:

    # -----------------------------------------------------------------------
    # TASK 1 — Ingest raw GTFS data
    # -----------------------------------------------------------------------
    def run_ingestion(**context):
        logger = logging.getLogger(__name__)
        logger.info(f"Running ingestion from project root: {PROJECT_ROOT}")

        import sys
        sys.path.insert(0, str(PROJECT_ROOT))

        # Import and run your existing pipeline functions
        import json
        from utils import (
            clean_all,
            download_gtfs_zip,
            extract_gtfs_files,
            print_summary,
            save_to_raw,
            setup_logging,
        )

        config_path = PROJECT_ROOT / "config.json"
        with open(config_path) as f:
            config = json.load(f)

        pipeline_logger = setup_logging(config["log_dir"])
        zip_bytes   = download_gtfs_zip(config["gtfs_url"], config["request_timeout_seconds"], pipeline_logger)
        dataframes  = extract_gtfs_files(zip_bytes, config["target_files"], pipeline_logger)
        dataframes  = clean_all(dataframes, pipeline_logger)
        manifest    = save_to_raw(dataframes, config["output_dir"], pipeline_logger)
        print_summary(manifest, pipeline_logger)

        # Push manifest to XCom so downstream tasks can reference it
        context["ti"].xcom_push(key="manifest", value=manifest)
        logger.info("Ingestion complete ✓")

    task_ingest = PythonOperator(
        task_id="ingest_gtfs_data",
        python_callable=run_ingestion,
    )

    load_dotenv()  # Load environment variables from .env file
    # -----------------------------------------------------------------------
    # TASK 2 — Upload CSVs to BigQuery bronze dataset
    # -----------------------------------------------------------------------
    def upload_to_bigquery(**context):
        from google.cloud import bigquery
        from google.oauth2 import service_account
        import pandas as pd

        logger = logging.getLogger(__name__)

        project_id = os.environ.get("GCP_PROJECT_ID")
        key_path   = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable not set")
        if not key_path:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")

        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        client   = bigquery.Client(project=project_id, credentials=credentials)
        dataset  = "bronze"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite each run
            autodetect=True,
        )

        csv_files = list(RAW_DATA_DIR.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {RAW_DATA_DIR}. Did ingestion run?")

        for csv_path in csv_files:
            table_name = csv_path.stem                # e.g. routes.csv → routes
            table_ref  = f"{project_id}.{dataset}.{table_name}"

            logger.info(f"Uploading {csv_path.name} → {table_ref}")
            df = pd.read_csv(csv_path)

            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()                             # wait for job to complete

            logger.info(f"Uploaded {len(df):,} rows to {table_ref} ✓")

        logger.info("All tables uploaded to BigQuery ✓")

    task_upload = PythonOperator(
        task_id="upload_to_bigquery",
        python_callable=upload_to_bigquery,
    )

    # -----------------------------------------------------------------------
    # TASK 3 — Run dbt models
    # Executes dbt run inside the dbt project directory
    # -----------------------------------------------------------------------
    def run_dbt_models(**context):
        logger = logging.getLogger(__name__)
        logger.info(f"Running dbt from: {DBT_PROJECT}")

        result = subprocess.run(
            ["dbt", "run", "--target", "prod"],
            cwd=DBT_PROJECT,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", ""),
                "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
            },
        )

        logger.info(result.stdout)

        if result.returncode != 0:
            logger.error(result.stderr)
            raise Exception(f"dbt run failed:\n{result.stderr}")

        logger.info("dbt run complete ✓")

    task_dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_models,
    )

    # -----------------------------------------------------------------------
    # TASK 4 — Run dbt tests
    # Executes dbt test to validate data quality across all models
    # -----------------------------------------------------------------------
    def run_dbt_tests(**context):
        logger = logging.getLogger(__name__)
        logger.info("Running dbt tests...")

        result = subprocess.run(
            ["dbt", "test", "--target", "prod"],
            cwd=DBT_PROJECT,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", ""),
            },
        )

        logger.info(result.stdout)

        if result.returncode != 0:
            logger.error(result.stderr)
            raise Exception(f"dbt test failed:\n{result.stderr}")

        logger.info("dbt tests passed ✓")

    task_dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_tests,
    )

    # -----------------------------------------------------------------------
    # TASK 5 — Notify success
    # Logs a summary of the completed pipeline run.
    # In production you'd replace this with a Slack or email notification.
    # -----------------------------------------------------------------------
    def notify_success(**context):
        logger   = logging.getLogger(__name__)
        manifest = context["ti"].xcom_pull(key="manifest", task_ids="ingest_gtfs_data")

        logger.info("=" * 55)
        logger.info("PIPELINE RUN COMPLETE")
        logger.info(f"  DAG run ID : {context['run_id']}")
        logger.info(f"  Exec date  : {context['logical_date']}")
        if manifest:
            logger.info("  Tables loaded:")
            for table, meta in manifest["tables"].items():
                logger.info(f"    {table:<20} {meta['rows']:>8,} rows")
        logger.info("=" * 55)

    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # -----------------------------------------------------------------------
    # Task dependencies — defines the execution order
    # -----------------------------------------------------------------------
    task_ingest >> task_upload >> task_dbt_run >> task_dbt_test >> task_notify