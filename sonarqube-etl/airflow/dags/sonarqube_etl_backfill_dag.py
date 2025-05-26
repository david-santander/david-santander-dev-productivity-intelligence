"""SonarQube ETL Backfill DAG for historical data population.

This module implements a dedicated Airflow DAG for backfilling historical SonarQube
metrics data. It's designed to handle large date ranges efficiently with batch
processing and comprehensive validation.

The backfill DAG is separate from the daily ETL to:
- Allow independent scheduling and resource management
- Optimize for bulk historical data processing
- Provide specialized configuration options
- Avoid interference with daily operations

Example:
    Backfill year-to-date with default settings::

        airflow dags trigger sonarqube_etl_backfill

    Backfill specific date range::

        airflow dags trigger sonarqube_etl_backfill --conf '{
            "start_date": "2025-01-01",
            "end_date": "2025-03-31"
        }'

    Backfill with batch processing::

        airflow dags trigger sonarqube_etl_backfill --conf '{
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "batch_size": 30
        }'

Configuration Options:
    start_date (str): Beginning of backfill range (YYYY-MM-DD)
    end_date (str): End of backfill range (YYYY-MM-DD)
    backfill_days (int): Alternative to date range - backfill N days from today
    batch_size (int): Number of days to process per batch (default: 30)

Note:
    This DAG always uses the historical API endpoints to ensure consistent
    data retrieval regardless of when the backfill is run.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import os
from typing import Dict, List, Any, Optional, Tuple
import json
from dateutil import parser

# Import the shared SonarQube client
from sonarqube_client import SonarQubeClient

# Import functions from main ETL DAG that aren't API-related
from sonarqube_etl_dag import (
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

# Backfill DAG definition
dag = DAG(
    "sonarqube_etl_backfill",
    default_args=default_args,
    description="Backfill historical SonarQube metrics data",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ## SonarQube ETL Backfill DAG
    
    This DAG is specifically designed for backfilling historical SonarQube metrics data.
    It should be triggered manually when you need to populate historical data.
    
    ### Usage
    
    Trigger this DAG with configuration to specify the date range:
    
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
    
    ### Features
    
    - Uses SonarQube's historical data API
    - Handles missing data with carry-forward logic
    - Processes data in batches to avoid timeouts
    - Provides detailed progress logging
    - Idempotent: safe to run multiple times
    
    ### CLI Examples
    
    ```bash
    # Backfill year-to-date
    airflow dags trigger sonarqube_etl_backfill
    
    # Backfill specific range
    airflow dags trigger sonarqube_etl_backfill --conf '{"start_date": "2025-01-01", "end_date": "2025-03-31"}'
    
    # Backfill last 90 days
    airflow dags trigger sonarqube_etl_backfill --conf '{"backfill_days": 90}'
    
    # Backfill with weekly batches
    airflow dags trigger sonarqube_etl_backfill --conf '{"start_date": "2025-01-01", "end_date": "2025-05-31", "batch_size": 7}'
    ```
    """,
)


def fetch_projects(**context) -> List[Dict[str, Any]]:
    """Wrapper around main fetch_projects function.

    Delegates to the main ETL DAG's fetch_projects function to maintain
    consistency in project retrieval logic.

    Args:
        **context: Airflow context dictionary

    Returns:
        List[Dict[str, Any]]: List of project dictionaries

    See Also:
        sonarqube_etl_dag.fetch_projects: Main implementation
    """
    return fetch_projects_main(**context)


def extract_historical_metrics(**context) -> int:
    """Extract historical metrics for all projects within date range.

    Core backfill function that processes historical data in configurable batches.
    Handles large date ranges efficiently by breaking them into smaller chunks
    to avoid timeouts and memory issues.

    Args:
        **context: Airflow context dictionary containing:
            - task_instance: For XCom communication
            - execution_date: Current execution date
            - dag_run: Contains configuration parameters

    Returns:
        int: Total number of metric records extracted

    Configuration:
        The function reads configuration from dag_run.conf:
        - start_date/end_date: Explicit date range
        - backfill_days: Alternative to date range
        - batch_size: Days per processing batch (default: 30)

    Process:
        1. Determines date range from configuration
        2. Generates list of dates to process
        3. Processes dates in batches for efficiency
        4. Handles failures gracefully with error tracking
        5. Provides detailed progress logging

    Note:
        - Always uses historical API (use_history=True)
        - Failed extractions create placeholder records for carry-forward
        - Logs summary statistics upon completion

    Example:
        When triggered with configuration::

            {
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "batch_size": 7
            }

        Will process January 2025 in weekly batches.
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

    logging.info(f"Backfill Configuration:")
    logging.info(f"  Start Date: {start_date}")
    logging.info(f"  End Date: {end_date}")
    logging.info(f"  Total Days: {(end_date - start_date).days + 1}")
    logging.info(f"  Batch Size: {batch_size} days")
    logging.info(f"  Projects: {len(projects)}")

    # Generate list of dates to process
    dates_to_process = []
    current_date = start_date
    while current_date <= end_date:
        dates_to_process.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    # Process in batches
    all_metrics = []
    total_batches = (len(dates_to_process) + batch_size - 1) // batch_size

    for batch_num in range(total_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, len(dates_to_process))
        batch_dates = dates_to_process[batch_start:batch_end]

        logging.info(
            f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_dates)} days)"
        )

        for project in projects:
            project_key = project["key"]
            logging.info(f"  Extracting metrics for project: {project_key}")

            for metric_date in batch_dates:
                try:
                    # Create client and always use historical API for backfill
                    client = SonarQubeClient(config)
                    metrics_data = client.fetch_project_metrics(
                        project_key, metric_date, use_history=True
                    )
                    all_metrics.append(metrics_data)
                except Exception as e:
                    logging.error(
                        f"Failed to fetch metrics for {project_key} on {metric_date}: {str(e)}"
                    )
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

        logging.info(
            f"Batch {batch_num + 1} complete. Total records so far: {len(all_metrics)}"
        )

    # Push metrics to XCom
    context["task_instance"].xcom_push(key="raw_metrics", value=all_metrics)

    # Summary statistics
    successful = sum(1 for m in all_metrics if m.get("metrics") is not None)
    failed = len(all_metrics) - successful

    logging.info(f"Backfill Extraction Complete:")
    logging.info(f"  Total Records: {len(all_metrics)}")
    logging.info(f"  Successful: {successful}")
    logging.info(f"  Failed: {failed}")
    logging.info(f"  Success Rate: {(successful/len(all_metrics)*100):.1f}%")

    return len(all_metrics)


def load_historical_metrics(**context) -> int:
    """Load historical metrics into PostgreSQL with optimized batch processing.

    Processes the extracted historical metrics and loads them into the database.
    Implements intelligent update logic to handle existing data and carry-forward
    scenarios.

    Args:
        **context: Airflow context dictionary for retrieving XCom data

    Returns:
        int: Total number of records inserted or updated

    Process:
        1. Groups metrics by project for efficient processing
        2. Ensures all projects exist in database
        3. Retrieves existing data to optimize updates
        4. Applies carry-forward logic for missing data
        5. Only updates existing carried-forward records with real data
        6. Commits all changes in a single transaction

    Optimization:
        - Batches database operations by project
        - Only updates when necessary (avoids redundant writes)
        - Uses single transaction for consistency
        - Tracks last known values for carry-forward

    Note:
        - Prioritizes real data over carried-forward data
        - Maintains data integrity with transaction rollback on error
        - Logs detailed statistics upon completion

    Example:
        For a project with gaps in data::

            Day 1: Real data (inserted)
            Day 2: No data (carry-forward from Day 1)
            Day 3: Real data (inserted)
            Day 4: No data (carry-forward from Day 3)
    """
    raw_metrics = context["task_instance"].xcom_pull(
        task_ids="extract_historical_metrics", key="raw_metrics"
    )
    projects = context["task_instance"].xcom_pull(
        task_ids="fetch_projects", key="projects"
    )

    if not raw_metrics:
        logging.warning("No metrics to load")
        return 0

    pg_hook = PostgresHook(postgres_conn_id="sonarqube_metrics_db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create project mapping
    project_map = {p["key"]: p["name"] for p in projects}

    try:
        # First, ensure all projects exist in the database
        for project_key, project_name in project_map.items():
            cursor.execute(
                """
                INSERT INTO sonarqube_metrics.sq_projects (sonarqube_project_key, project_name)
                VALUES (%s, %s)
                ON CONFLICT (sonarqube_project_key) DO UPDATE
                SET project_name = EXCLUDED.project_name,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING project_id
            """,
                (project_key, project_name),
            )

        conn.commit()

        # Get project IDs
        cursor.execute(
            "SELECT project_id, sonarqube_project_key FROM sonarqube_metrics.sq_projects"
        )
        project_id_map = {row[1]: row[0] for row in cursor.fetchall()}

        # Group metrics by project for efficient processing
        metrics_by_project = {}
        for metric in raw_metrics:
            project_key = metric["project_key"]
            if project_key not in metrics_by_project:
                metrics_by_project[project_key] = []
            metrics_by_project[project_key].append(metric)

        # Process each project's metrics
        total_inserted = 0
        total_carried_forward = 0

        for project_key, project_metrics in metrics_by_project.items():
            project_id = project_id_map[project_key]

            # Sort by date for proper carry-forward
            project_metrics.sort(key=lambda x: x["metric_date"])

            # Get existing data for this project to optimize updates
            cursor.execute(
                """
                SELECT metric_date, data_source_timestamp 
                FROM sonarqube_metrics.daily_project_metrics
                WHERE project_id = %s
                ORDER BY metric_date
            """,
                (project_id,),
            )

            existing_dates = {str(row[0]): row[1] for row in cursor.fetchall()}

            # Track last known good values for carry-forward
            last_known_dict = None

            for metric_data in project_metrics:
                metric_date = metric_data["metric_date"]

                if metric_data.get("error") or not metric_data.get("metrics"):
                    # Use carry-forward logic
                    if last_known_dict:
                        # Only insert if we don't have data for this date
                        if metric_date not in existing_dates:
                            insert_carried_forward_metric(
                                cursor, project_id, metric_date, last_known_dict
                            )
                            total_carried_forward += 1
                else:
                    # Transform and insert actual metrics
                    transformed = transform_metric_data(metric_data)

                    # Check if we should update existing data
                    if metric_date in existing_dates:
                        # Only update if this is newer data or if existing is carried forward
                        cursor.execute(
                            """
                            SELECT is_carried_forward 
                            FROM sonarqube_metrics.daily_project_metrics
                            WHERE project_id = %s AND metric_date = %s
                        """,
                            (project_id, metric_date),
                        )

                        result = cursor.fetchone()
                        if (
                            result and result[0]
                        ):  # If existing is carried forward, update with real data
                            insert_metric(cursor, project_id, transformed)
                            total_inserted += 1
                    else:
                        # No existing data, insert new
                        insert_metric(cursor, project_id, transformed)
                        total_inserted += 1

                    # Update last known values
                    last_known_dict = transformed

        conn.commit()

        logging.info(f"Backfill Load Complete:")
        logging.info(f"  Total Inserted/Updated: {total_inserted}")
        logging.info(f"  Total Carried Forward: {total_carried_forward}")
        logging.info(f"  Projects Processed: {len(metrics_by_project)}")

        return total_inserted + total_carried_forward

    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading historical metrics: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def validate_backfill(**context) -> int:
    """Validate the backfilled data and generate summary report.

    Final task that validates the backfilled data and generates a comprehensive
    report showing data quality, coverage, and potential issues.

    Args:
        **context: Airflow context dictionary containing dag_run configuration

    Returns:
        int: Number of projects validated

    Report Sections:
        1. Summary Statistics:
           - Date range processed
           - Projects analyzed
           - Data coverage percentages

        2. Per-Project Analysis:
           - Days with data
           - Actual vs carried-forward ratio
           - Average metric values
           - Date range coverage

        3. Data Quality Checks:
           - Gap detection
           - Missing data identification
           - Coverage analysis

    Output:
        Generates formatted report in Airflow logs with clear sections
        and visual separators for easy reading.

    Note:
        - Report is only written to logs (not persisted)
        - Uses SQL aggregations for efficient analysis
        - Provides actionable insights for data quality

    Example Output::

        ================================================================================
        BACKFILL VALIDATION REPORT
        ================================================================================
        Date Range: 2025-01-01 to 2025-01-31

        Project: my-java-app
          Days with data: 31
          Date range: 2025-01-01 to 2025-01-31
          Actual data days: 28
          Carried forward days: 3
          Coverage: 90.3% actual data
          Average metrics: Bugs=12, Vulnerabilities=3, Code Smells=47

        ✓ No data gaps detected - all days have data
        ================================================================================
    """
    dag_run_conf = context["dag_run"].conf or {}

    # Get date range from configuration
    if "start_date" in dag_run_conf and "end_date" in dag_run_conf:
        start_date = dag_run_conf["start_date"]
        end_date = dag_run_conf["end_date"]
    else:
        execution_date = context["execution_date"]
        end_date = execution_date.strftime("%Y-%m-%d")

        backfill_days = dag_run_conf.get("backfill_days", None)
        if backfill_days:
            start_date = (execution_date - timedelta(days=int(backfill_days))).strftime(
                "%Y-%m-%d"
            )
        else:
            start_date = "2025-01-01"

    pg_hook = PostgresHook(postgres_conn_id="sonarqube_metrics_db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Generate validation report
        cursor.execute(
            """
            SELECT 
                p.project_name,
                COUNT(DISTINCT m.metric_date) as days_with_data,
                MIN(m.metric_date) as earliest_date,
                MAX(m.metric_date) as latest_date,
                COUNT(CASE WHEN m.is_carried_forward = true THEN 1 END) as carried_forward_days,
                COUNT(CASE WHEN m.is_carried_forward = false THEN 1 END) as actual_data_days,
                ROUND(AVG(m.bugs_total), 2) as avg_bugs,
                ROUND(AVG(m.vulnerabilities_total), 2) as avg_vulnerabilities,
                ROUND(AVG(m.code_smells_total), 2) as avg_code_smells
            FROM sonarqube_metrics.daily_project_metrics m
            JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
            WHERE m.metric_date BETWEEN %s AND %s
            GROUP BY p.project_name
            ORDER BY p.project_name
        """,
            (start_date, end_date),
        )

        results = cursor.fetchall()

        logging.info("=" * 80)
        logging.info("BACKFILL VALIDATION REPORT")
        logging.info("=" * 80)
        logging.info(f"Date Range: {start_date} to {end_date}")
        logging.info("")

        for row in results:
            logging.info(f"Project: {row[0]}")
            logging.info(f"  Days with data: {row[1]}")
            logging.info(f"  Date range: {row[2]} to {row[3]}")
            logging.info(f"  Actual data days: {row[5]}")
            logging.info(f"  Carried forward days: {row[4]}")
            logging.info(f"  Coverage: {(row[5]/row[1]*100):.1f}% actual data")
            logging.info(
                f"  Average metrics: Bugs={row[6]}, Vulnerabilities={row[7]}, Code Smells={row[8]}"
            )
            logging.info("")

        # Check for gaps in data
        cursor.execute(
            """
            WITH date_series AS (
                SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date AS expected_date
            ),
            project_dates AS (
                SELECT DISTINCT project_id, metric_date
                FROM sonarqube_metrics.daily_project_metrics
                WHERE metric_date BETWEEN %s AND %s
            )
            SELECT 
                p.project_name,
                COUNT(ds.expected_date) - COUNT(pd.metric_date) as missing_days
            FROM date_series ds
            CROSS JOIN sonarqube_metrics.sq_projects p
            LEFT JOIN project_dates pd ON pd.project_id = p.project_id AND pd.metric_date = ds.expected_date
            GROUP BY p.project_name
            HAVING COUNT(ds.expected_date) - COUNT(pd.metric_date) > 0
        """,
            (start_date, end_date, start_date, end_date),
        )

        gaps = cursor.fetchall()
        if gaps:
            logging.warning("Data gaps detected:")
            for project, missing in gaps:
                logging.warning(f"  {project}: {missing} missing days")
        else:
            logging.info("✓ No data gaps detected - all days have data")

        logging.info("=" * 80)

        return len(results)

    finally:
        cursor.close()
        conn.close()


# Define tasks
fetch_projects_task = PythonOperator(
    task_id="fetch_projects", python_callable=fetch_projects, dag=dag
)

extract_historical_metrics_task = PythonOperator(
    task_id="extract_historical_metrics",
    python_callable=extract_historical_metrics,
    dag=dag,
)

load_historical_metrics_task = PythonOperator(
    task_id="load_historical_metrics", python_callable=load_historical_metrics, dag=dag
)

validate_backfill_task = PythonOperator(
    task_id="validate_backfill", python_callable=validate_backfill, dag=dag
)

# Set task dependencies
(
    fetch_projects_task
    >> extract_historical_metrics_task
    >> load_historical_metrics_task
    >> validate_backfill_task
)
