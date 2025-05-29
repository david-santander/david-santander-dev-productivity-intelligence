"""SonarQube ETL Backfill DAG v2 - Enhanced with security review and quality gate metrics.

This enhanced backfill DAG includes:
- Security review metrics (security_hotspots_reviewed, security_review_rating)
- Quality gate status tracking
- Corrected metric key mappings
- Additional issue state metrics
- Improved data transformation aligned with official SonarQube API

This module implements a dedicated Airflow DAG for backfilling historical SonarQube
metrics data with the enhanced metric set. It's designed to handle large date ranges 
efficiently with batch processing and comprehensive validation.

Example:
    Backfill year-to-date with enhanced metrics::

        airflow dags trigger sonarqube_etl_backfill_v2

    Backfill specific date range::

        airflow dags trigger sonarqube_etl_backfill_v2 --conf '{
            "start_date": "2025-01-01",
            "end_date": "2025-03-31"
        }'

    Backfill with batch processing::

        airflow dags trigger sonarqube_etl_backfill_v2 --conf '{
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "batch_size": 30
        }'
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.datasets import Dataset
import logging
import os
from typing import Dict, List, Any, Optional, Tuple
import json
from dateutil import parser

# Import the enhanced SonarQube client v2
from sonarqube_client_v2 import SonarQubeClient

# Define dataset for data-aware scheduling
SONARQUBE_METRICS_DATASET = Dataset("postgres://sonarqube_metrics_db/daily_project_metrics")

# Import functions from enhanced ETL DAG v2
from sonarqube_etl_dag_v2 import (
    get_sonarqube_config,
    fetch_projects as fetch_projects_main,
    transform_metric_data,
    insert_metric,
    insert_carried_forward_metric,
)

# Default args for the backfill DAG
default_args = {
    "owner": "devops-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# Enhanced Backfill DAG definition
dag = DAG(
    "sonarqube_etl_backfill_v2",
    default_args=default_args,
    description="Enhanced backfill with security review and quality gate metrics",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ## SonarQube ETL Backfill DAG v2 - Enhanced
    
    This enhanced DAG backfills historical SonarQube metrics with additional features:
    - Security review metrics and ratings
    - Quality gate status tracking
    - Issue acceptance status
    - Corrected technical debt metrics
    
    ### Usage
    
    1. **Backfill from January 2025 to today** (default):
    ```json
    {}
    ```
    
    2. **Backfill specific date range**:
    ```json
    {
        "start_date": "2025-01-01",
        "end_date": "2025-05-31"
    }
    ```
    
    3. **Backfill last N days**:
    ```json
    {
        "backfill_days": 30
    }
    ```
    
    4. **Backfill with specific batch size** (for large date ranges):
    ```json
    {
        "start_date": "2025-01-01",
        "end_date": "2025-05-31",
        "batch_size": 7
    }
    ```
    
    ### Enhanced Features
    
    - Uses enhanced SonarQube client v2 with corrected metric keys
    - Extracts security review metrics and ratings
    - Tracks quality gate status
    - Handles missing data with carry-forward logic
    - Processes data in batches to avoid timeouts
    - Provides detailed progress logging
    - Idempotent: safe to run multiple times
    
    ### CLI Examples
    
    ```bash
    # Backfill year-to-date with enhanced metrics
    airflow dags trigger sonarqube_etl_backfill_v2
    
    # Backfill specific range
    airflow dags trigger sonarqube_etl_backfill_v2 --conf '{"start_date": "2025-01-01", "end_date": "2025-03-31"}'
    
    # Backfill last 90 days
    airflow dags trigger sonarqube_etl_backfill_v2 --conf '{"backfill_days": 90}'
    
    # Backfill with weekly batches
    airflow dags trigger sonarqube_etl_backfill_v2 --conf '{"start_date": "2025-01-01", "end_date": "2025-05-31", "batch_size": 7}'
    ```
    """,
)


def fetch_projects(**context) -> List[Dict[str, Any]]:
    """Wrapper around main fetch_projects function.

    Delegates to the enhanced ETL DAG v2's fetch_projects function to maintain
    consistency in project retrieval logic.
    """
    return fetch_projects_main(**context)


def extract_historical_metrics(**context) -> int:
    """Extract enhanced historical metrics for all projects within date range.

    Enhanced backfill function that processes historical data with new metrics:
    - Security review metrics
    - Quality gate status
    - Additional issue states
    - Corrected metric key mappings
    
    Uses the enhanced SonarQube client v2 for accurate data collection.
    """
    projects = context["task_instance"].xcom_pull(
        task_ids="fetch_projects", key="projects"
    )
    config = get_sonarqube_config()

    # Parse date range from DAG run configuration
    dag_run_conf = context["dag_run"].conf or {}
    execution_date = context["execution_date"]

    # Determine date range
    if "start_date" in dag_run_conf and "end_date" in dag_run_conf:
        start_date = parser.parse(dag_run_conf["start_date"]).date()
        end_date = parser.parse(dag_run_conf["end_date"]).date()
    else:
        # Default behavior
        end_date = execution_date.date()
        backfill_days = dag_run_conf.get("backfill_days", None)

        if backfill_days:
            start_date = end_date - timedelta(days=int(backfill_days))
        else:
            # Default to beginning of current year
            start_date = datetime(2025, 1, 1).date()

    # Get batch size for processing
    batch_size = dag_run_conf.get("batch_size", 30)  # Default 30 days per batch

    logging.info(f"Enhanced Backfill Configuration:")
    logging.info(f"  Start Date: {start_date}")
    logging.info(f"  End Date: {end_date}")
    logging.info(f"  Total Days: {(end_date - start_date).days + 1}")
    logging.info(f"  Batch Size: {batch_size} days")
    logging.info(f"  Projects: {len(projects)}")
    logging.info(f"  Enhanced Features: Security review, Quality gate, Corrected metrics")

    # Generate list of dates to process
    dates_to_process = []
    current_date = start_date
    while current_date <= end_date:
        dates_to_process.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    # Process in batches
    all_metrics = []
    total_batches = (len(dates_to_process) + batch_size - 1) // batch_size
    
    # Statistics tracking
    success_count = 0
    error_count = 0
    metrics_with_security_review = 0
    metrics_with_quality_gate = 0

    for batch_num in range(total_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, len(dates_to_process))
        batch_dates = dates_to_process[batch_start:batch_end]

        logging.info(
            f"Processing enhanced batch {batch_num + 1}/{total_batches} ({len(batch_dates)} days)"
        )

        for project in projects:
            project_key = project["key"]
            logging.info(f"  Extracting enhanced metrics for project: {project_key}")

            for metric_date in batch_dates:
                try:
                    # Create enhanced client and always use historical API for backfill
                    client = SonarQubeClient(config)
                    metrics_data = client.fetch_project_metrics(
                        project_key, metric_date, use_history=True
                    )
                    
                    # Track enhanced metrics availability
                    if metrics_data.get('metrics'):
                        if metrics_data['metrics'].get('security_hotspots_reviewed') is not None:
                            metrics_with_security_review += 1
                        if metrics_data['metrics'].get('alert_status') is not None:
                            metrics_with_quality_gate += 1
                    
                    all_metrics.append(metrics_data)
                    success_count += 1
                    
                except Exception as e:
                    logging.error(
                        f"Failed to fetch enhanced metrics for {project_key} on {metric_date}: {str(e)}"
                    )
                    error_count += 1
                    # Add empty record for carry-forward
                    all_metrics.append(
                        {
                            "project_key": project_key,
                            "metric_date": metric_date,
                            "metrics": None,
                            "issues_breakdown": None,
                            "new_code_issues_breakdown": None,
                            "error": str(e),
                        }
                    )

        # Log batch progress
        logging.info(
            f"  Batch {batch_num + 1} completed: "
            f"{len(batch_dates) * len(projects)} total attempts"
        )

    # Log final statistics
    total_attempts = len(dates_to_process) * len(projects)
    logging.info(f"Enhanced Backfill Statistics:")
    logging.info(f"  Total Attempts: {total_attempts}")
    logging.info(f"  Successful Extractions: {success_count}")
    logging.info(f"  Failed Extractions: {error_count}")
    logging.info(f"  Success Rate: {(success_count/total_attempts)*100:.1f}%")
    logging.info(f"  Metrics with Security Review: {metrics_with_security_review}")
    logging.info(f"  Metrics with Quality Gate: {metrics_with_quality_gate}")

    context["task_instance"].xcom_push(key="raw_metrics", value=all_metrics)
    return len(all_metrics)


def transform_and_load_historical_metrics(**context) -> None:
    """Transform and load enhanced historical metrics into PostgreSQL.

    Enhanced version that handles:
    - Security review metrics transformation
    - Quality gate status processing
    - Corrected metric key mappings
    - Additional issue state metrics
    
    Uses the enhanced transform_metric_data function from v2 DAG.
    """
    raw_metrics = context["task_instance"].xcom_pull(
        task_ids="extract_historical_metrics", key="raw_metrics"
    )
    projects = context["task_instance"].xcom_pull(
        task_ids="fetch_projects", key="projects"
    )

    pg_hook = PostgresHook(postgres_conn_id="sonarqube_metrics_db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create project mapping
    project_map = {p["key"]: p["name"] for p in projects}

    # Statistics tracking
    records_inserted = 0
    records_carried_forward = 0
    records_with_security_review = 0
    records_with_quality_gate = 0

    try:
        # First, ensure all projects exist in the database
        for project in projects:
            project_key = project["key"]
            project_name = project["name"]
            last_analysis_date = project.get("lastAnalysisDate")

            cursor.execute(
                """
                INSERT INTO sonarqube_metrics.sq_projects (sonarqube_project_key, project_name, last_analysis_date_from_sq)
                VALUES (%s, %s, %s)
                ON CONFLICT (sonarqube_project_key) DO UPDATE
                SET project_name = EXCLUDED.project_name,
                    last_analysis_date_from_sq = EXCLUDED.last_analysis_date_from_sq,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING project_id
            """,
                (project_key, project_name, last_analysis_date),
            )

        conn.commit()

        # Get project IDs
        cursor.execute(
            "SELECT project_id, sonarqube_project_key FROM sonarqube_metrics.sq_projects"
        )
        project_id_map = {row[1]: row[0] for row in cursor.fetchall()}

        # Group metrics by project for carry-forward logic
        metrics_by_project = {}
        for metric in raw_metrics:
            project_key = metric["project_key"]
            if project_key not in metrics_by_project:
                metrics_by_project[project_key] = []
            metrics_by_project[project_key].append(metric)

        # Process each project's metrics
        for project_key, project_metrics in metrics_by_project.items():
            project_id = project_id_map[project_key]

            # Sort by date for proper carry-forward logic
            project_metrics.sort(key=lambda x: x["metric_date"])

            # Get last known values for carry-forward
            cursor.execute(
                """
                SELECT * FROM sonarqube_metrics.daily_project_metrics
                WHERE project_id = %s
                ORDER BY metric_date DESC
                LIMIT 1
            """,
                (project_id,),
            )

            last_known = cursor.fetchone()
            last_known_dict = {}
            if last_known:
                columns = [desc[0] for desc in cursor.description]
                last_known_dict = dict(zip(columns, last_known))

            logging.info(f"Processing {len(project_metrics)} records for {project_key}")

            for metric_data in project_metrics:
                if metric_data.get("error") or not metric_data.get("metrics"):
                    # Use carry-forward logic
                    if last_known_dict:
                        insert_carried_forward_metric(
                            cursor, project_id, metric_data["metric_date"], last_known_dict
                        )
                        records_carried_forward += 1
                else:
                    # Transform and insert actual enhanced metrics
                    transformed = transform_metric_data(metric_data)
                    insert_metric(cursor, project_id, transformed)
                    records_inserted += 1
                    
                    # Track enhanced metrics
                    if transformed.get('security_hotspots_reviewed', 0) > 0:
                        records_with_security_review += 1
                    if transformed.get('alert_status'):
                        records_with_quality_gate += 1
                    
                    # Update last known values
                    last_known_dict = transformed

        conn.commit()
        
        # Log final statistics
        total_records = records_inserted + records_carried_forward
        logging.info(f"Enhanced Backfill Load Statistics:")
        logging.info(f"  Total Records Processed: {total_records}")
        logging.info(f"  New Records Inserted: {records_inserted}")
        logging.info(f"  Carried Forward Records: {records_carried_forward}")
        logging.info(f"  Records with Security Review: {records_with_security_review}")
        logging.info(f"  Records with Quality Gate: {records_with_quality_gate}")
        logging.info(f"Successfully loaded all enhanced metrics to database")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading enhanced metrics to database: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


# Define enhanced backfill tasks
fetch_projects_task = PythonOperator(
    task_id="fetch_projects",
    python_callable=fetch_projects,
    dag=dag,
)

extract_historical_metrics_task = PythonOperator(
    task_id="extract_historical_metrics",
    python_callable=extract_historical_metrics,
    dag=dag,
)

transform_load_historical_task = PythonOperator(
    task_id="transform_and_load_historical",
    python_callable=transform_and_load_historical_metrics,
    outlets=[SONARQUBE_METRICS_DATASET],
    dag=dag,
)

# Set enhanced task dependencies
fetch_projects_task >> extract_historical_metrics_task >> transform_load_historical_task