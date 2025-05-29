"""SonarQube ETL Data Quality Report DAG.

This module implements an Airflow DAG that validates the data quality and health of the
SonarQube ETL process by comparing metrics stored in PostgreSQL against live data from
the SonarQube server. The DAG runs after ETL processes complete and produces reports
in both JSON and Markdown formats.

The data quality validation includes:
- ETL accuracy rate and data integrity checks
- Data freshness and carry-forward detection
- Missing data identification
- Sync status between source and destination
- ETL pipeline health recommendations

Example:
    Manual trigger after ETL completion::

        airflow dags trigger sonarqube_comparison_report

Note:
    This DAG depends on either sonarqube_etl or sonarqube_etl_backfill DAGs
    to ensure fresh data is available for comparison.

Attributes:
    METRIC_FIELDS (List[str]): Core metrics to compare between systems
    TIMEZONE_MAPPING (Dict[str, str]): Timezone configurations for different systems
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from dateutil import parser as date_parser

# Import the shared SonarQube client
from sonarqube_client import SonarQubeClient

# Define datasets for data-aware scheduling
SONARQUBE_METRICS_DATASET = Dataset(
    "postgres://sonarqube_metrics_db/daily_project_metrics"
)

# Timezone configurations
TIMEZONE_MAPPING = {
    "sonarqube_server": "UTC",  # SonarQube typically stores in UTC
    "airflow": "UTC",  # Airflow configured timezone
    "mexico_city": "America/Mexico_City",  # Local reporting timezone
    "postgres": "UTC",  # Database timezone
}

# Metrics to compare
METRIC_FIELDS = [
    # Bug metrics
    "bugs_total",
    "bugs_blocker",
    "bugs_critical",
    "bugs_major",
    "bugs_minor",
    "bugs_info",
    "bugs_open",
    "bugs_confirmed",
    "bugs_reopened",
    "bugs_resolved",
    "bugs_closed",
    "bugs_false_positive",
    "bugs_wontfix",
    # Vulnerability metrics
    "vulnerabilities_total",
    "vulnerabilities_blocker",
    "vulnerabilities_critical",
    "vulnerabilities_high",
    "vulnerabilities_medium",
    "vulnerabilities_low",
    "vulnerabilities_open",
    "vulnerabilities_confirmed",
    "vulnerabilities_reopened",
    "vulnerabilities_resolved",
    "vulnerabilities_closed",
    "vulnerabilities_false_positive",
    "vulnerabilities_wontfix",
    # Code smell metrics
    "code_smells_total",
    "code_smells_blocker",
    "code_smells_critical",
    "code_smells_major",
    "code_smells_minor",
    "code_smells_info",
    "code_smells_open",
    "code_smells_confirmed",
    "code_smells_reopened",
    "code_smells_resolved",
    "code_smells_closed",
    "code_smells_false_positive",
    "code_smells_wontfix",
    # Security hotspot metrics
    "security_hotspots_total",
    "security_hotspots_high",
    "security_hotspots_medium",
    "security_hotspots_low",
    "security_hotspots_to_review",
    "security_hotspots_acknowledged",
    "security_hotspots_fixed",
    "security_hotspots_safe",
    # Quality metrics
    "coverage_percentage",
    "duplicated_lines_density",
    # New code bug metrics
    "new_code_bugs_total",
    "new_code_bugs_blocker",
    "new_code_bugs_critical",
    "new_code_bugs_major",
    "new_code_bugs_minor",
    "new_code_bugs_info",
    # New code vulnerability metrics
    "new_code_vulnerabilities_total",
    "new_code_vulnerabilities_blocker",
    "new_code_vulnerabilities_critical",
    "new_code_vulnerabilities_high",
    "new_code_vulnerabilities_medium",
    "new_code_vulnerabilities_low",
    # New code code smell metrics
    "new_code_code_smells_total",
    "new_code_code_smells_blocker",
    "new_code_code_smells_critical",
    "new_code_code_smells_major",
    "new_code_code_smells_minor",
    "new_code_code_smells_info",
    # New code security hotspot metrics
    "new_code_security_hotspots_total",
    "new_code_security_hotspots_high",
    "new_code_security_hotspots_medium",
    "new_code_security_hotspots_low",
    "new_code_security_hotspots_to_review",
    # New code quality metrics
    "new_code_coverage_percentage",
    "new_code_duplicated_lines_density",
    "new_code_lines",
]

# Default args for the DAG
default_args = {
    "owner": "devops-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "sonarqube_etl_data_quality_report",
    default_args=default_args,
    description="Validate ETL data quality by comparing PostgreSQL and SonarQube metrics",
    schedule=[SONARQUBE_METRICS_DATASET],  # Data-aware scheduling
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ## SonarQube ETL Data Quality Report DAG
    
    This DAG validates the health and quality of the SonarQube ETL process by comparing
    metrics stored in PostgreSQL against live data from the SonarQube server. It focuses
    on ETL pipeline health rather than code quality metrics.
    
    ### Features
    - Compares latest PostgreSQL snapshot with current SonarQube data
    - Handles timezone conversions between systems
    - Generates reports in JSON and Markdown formats
    - Detects and reports data discrepancies
    - Validates data freshness
    
    ### Data-Aware Scheduling
    This DAG uses data-aware scheduling and is triggered automatically when the
    SonarQube metrics dataset is updated by either:
    - `sonarqube_etl` daily runs
    - `sonarqube_etl_backfill` completion
    
    The main ETL DAGs are NOT dependent on this report DAG, ensuring they can
    complete successfully even if report generation fails.
    
    ### Output
    Reports are saved to:
    - `/opt/airflow/reports/comparison_report_YYYYMMDD_HHMMSS.json`
    - `/opt/airflow/reports/comparison_report_YYYYMMDD_HHMMSS.md`
    """,
)


def get_sonarqube_config() -> Dict[str, Any]:
    """Get SonarQube configuration from environment variables.

    Returns:
        Dict[str, Any]: Configuration dictionary with connection details
    """
    token = os.environ.get("AIRFLOW_VAR_SONARQUBE_TOKEN")
    if not token:
        logging.error("SONARQUBE_TOKEN not found in environment!")
        raise KeyError("SONARQUBE_TOKEN not configured in environment")

    base_url = os.environ.get("AIRFLOW_VAR_SONARQUBE_BASE_URL", "http://sonarqube:9000")

    return {
        "base_url": base_url,
        "token": token,
        "auth": (token, ""),
    }


def convert_timezone(timestamp: datetime, from_tz: str, to_tz: str) -> datetime:
    """Convert timestamp between timezones.

    Args:
        timestamp: Datetime object to convert
        from_tz: Source timezone name
        to_tz: Target timezone name

    Returns:
        Timezone-aware datetime in target timezone
    """
    if timestamp.tzinfo is None:
        # Assume the timestamp is in from_tz if naive
        source_tz = pytz.timezone(TIMEZONE_MAPPING.get(from_tz, from_tz))
        timestamp = source_tz.localize(timestamp)

    target_tz = pytz.timezone(TIMEZONE_MAPPING.get(to_tz, to_tz))
    return timestamp.astimezone(target_tz)


def fetch_postgres_latest_snapshot(**context) -> Dict[str, Any]:
    """Fetch the latest metrics snapshot from PostgreSQL.

    Retrieves the most recent metrics for all projects from the database,
    including metadata about data freshness and carry-forward status.

    Args:
        **context: Airflow context

    Returns:
        Dict containing project metrics and metadata
    """
    logging.info("Fetching latest snapshot from PostgreSQL")

    pg_hook = PostgresHook(postgres_conn_id="sonarqube_metrics_db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Get latest metrics for each project
        cursor.execute(
            """
            SELECT 
                p.sonarqube_project_key,
                p.project_name,
                m.metric_date,
                m.data_source_timestamp,
                m.is_carried_forward,
                m.*
            FROM sonarqube_metrics.sq_projects p
            JOIN LATERAL (
                SELECT *
                FROM sonarqube_metrics.daily_project_metrics dpm
                WHERE dpm.project_id = p.project_id
                ORDER BY dpm.metric_date DESC
                LIMIT 1
            ) m ON true
            ORDER BY p.sonarqube_project_key
        """
        )

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        postgres_data = {"snapshot_timestamp": datetime.now(pytz.UTC), "projects": {}}

        for row in rows:
            data = dict(zip(columns, row))
            project_key = data["sonarqube_project_key"]

            # Convert timestamps to UTC
            if data["data_source_timestamp"]:
                data["data_source_timestamp"] = convert_timezone(
                    data["data_source_timestamp"], "postgres", "sonarqube_server"
                )

            postgres_data["projects"][project_key] = data

        # Get database statistics
        cursor.execute(
            """
            SELECT 
                COUNT(DISTINCT project_id) as total_projects,
                MAX(metric_date) as latest_metric_date,
                MIN(metric_date) as earliest_metric_date,
                COUNT(*) as total_records
            FROM sonarqube_metrics.daily_project_metrics
        """
        )

        stats_row = cursor.fetchone()
        postgres_data["statistics"] = {
            "total_projects": stats_row[0],
            "latest_metric_date": str(stats_row[1]),
            "earliest_metric_date": str(stats_row[2]),
            "total_records": stats_row[3],
        }

        context["task_instance"].xcom_push(key="postgres_snapshot", value=postgres_data)
        logging.info(
            f"Retrieved data for {len(postgres_data['projects'])} projects from PostgreSQL"
        )

        return postgres_data

    finally:
        cursor.close()
        conn.close()


def fetch_sonarqube_current_data(**context) -> Dict[str, Any]:
    """Fetch current metrics directly from SonarQube server.

    Retrieves live metrics for all projects to compare against PostgreSQL data.

    Args:
        **context: Airflow context

    Returns:
        Dict containing current SonarQube metrics
    """
    logging.info("Fetching current data from SonarQube server")

    config = get_sonarqube_config()
    client = SonarQubeClient(config)

    # Get all projects
    projects = client.fetch_all_projects()

    sonarqube_data = {"fetch_timestamp": datetime.now(pytz.UTC), "projects": {}}

    for project in projects:
        project_key = project["key"]
        logging.info(f"Fetching current metrics for {project_key}")

        try:
            # Get current metrics
            metrics = client.fetch_current_metrics(project_key)

            # Get issue breakdowns
            issues_breakdown = client.fetch_issue_breakdown(
                project_key, is_new_code=False
            )
            new_code_issues = client.fetch_issue_breakdown(
                project_key, is_new_code=True
            )

            # Transform to match database structure
            transformed_data = transform_sonarqube_to_db_format(
                metrics, issues_breakdown, new_code_issues
            )

            sonarqube_data["projects"][project_key] = {
                "project_name": project["name"],
                "raw_metrics": metrics,
                "transformed_metrics": transformed_data,
                "fetch_time": datetime.now(pytz.UTC),
            }

        except Exception as e:
            logging.error(f"Failed to fetch metrics for {project_key}: {str(e)}")
            sonarqube_data["projects"][project_key] = {
                "project_name": project["name"],
                "error": str(e),
                "fetch_time": datetime.now(pytz.UTC),
            }

    context["task_instance"].xcom_push(key="sonarqube_current", value=sonarqube_data)
    logging.info(
        f"Retrieved data for {len(sonarqube_data['projects'])} projects from SonarQube"
    )

    return sonarqube_data


def transform_sonarqube_to_db_format(
    metrics: Dict[str, Any], issues: Dict[str, int], new_code_issues: Dict[str, int]
) -> Dict[str, Any]:
    """Transform SonarQube API data to match database schema format.

    Args:
        metrics: Raw metrics from SonarQube API
        issues: Issue breakdown by type/severity/status
        new_code_issues: New code issue breakdown

    Returns:
        Transformed data matching database column names
    """
    return {
        # Bug metrics
        "bugs_total": int(metrics.get("bugs", 0)),
        "bugs_blocker": issues.get("bug_blocker", 0),
        "bugs_critical": issues.get("bug_critical", 0),
        "bugs_major": issues.get("bug_major", 0),
        "bugs_minor": issues.get("bug_minor", 0),
        "bugs_info": issues.get("bug_info", 0),
        "bugs_open": issues.get("bug_open", 0),
        "bugs_confirmed": issues.get("bug_confirmed", 0),
        "bugs_reopened": issues.get("bug_reopened", 0),
        "bugs_resolved": issues.get("bug_resolved", 0),
        "bugs_closed": issues.get("bug_closed", 0),
        "bugs_false_positive": issues.get("bug_false-positive", 0),
        "bugs_wontfix": issues.get("bug_wontfix", 0),
        # Vulnerability metrics
        "vulnerabilities_total": int(metrics.get("vulnerabilities", 0)),
        "vulnerabilities_blocker": issues.get("vulnerability_blocker", 0),
        "vulnerabilities_critical": issues.get("vulnerability_critical", 0),
        "vulnerabilities_high": issues.get("vulnerability_major", 0),
        "vulnerabilities_medium": issues.get("vulnerability_minor", 0),
        "vulnerabilities_low": issues.get("vulnerability_info", 0),
        "vulnerabilities_open": issues.get("vulnerability_open", 0),
        "vulnerabilities_confirmed": issues.get("vulnerability_confirmed", 0),
        "vulnerabilities_reopened": issues.get("vulnerability_reopened", 0),
        "vulnerabilities_resolved": issues.get("vulnerability_resolved", 0),
        "vulnerabilities_closed": issues.get("vulnerability_closed", 0),
        "vulnerabilities_false_positive": issues.get("vulnerability_false-positive", 0),
        "vulnerabilities_wontfix": issues.get("vulnerability_wontfix", 0),
        # Code smell metrics
        "code_smells_total": int(metrics.get("code_smells", 0)),
        "code_smells_blocker": issues.get("code_smell_blocker", 0),
        "code_smells_critical": issues.get("code_smell_critical", 0),
        "code_smells_major": issues.get("code_smell_major", 0),
        "code_smells_minor": issues.get("code_smell_minor", 0),
        "code_smells_info": issues.get("code_smell_info", 0),
        "code_smells_open": issues.get("code_smell_open", 0),
        "code_smells_confirmed": issues.get("code_smell_confirmed", 0),
        "code_smells_reopened": issues.get("code_smell_reopened", 0),
        "code_smells_resolved": issues.get("code_smell_resolved", 0),
        "code_smells_closed": issues.get("code_smell_closed", 0),
        "code_smells_false_positive": issues.get("code_smell_false-positive", 0),
        "code_smells_wontfix": issues.get("code_smell_wontfix", 0),
        # Security hotspot metrics
        "security_hotspots_total": int(metrics.get("security_hotspots", 0)),
        "security_hotspots_high": issues.get("security_hotspot_high", 0),
        "security_hotspots_medium": issues.get("security_hotspot_medium", 0),
        "security_hotspots_low": issues.get("security_hotspot_low", 0),
        "security_hotspots_to_review": issues.get("security_hotspot_to_review", 0),
        "security_hotspots_acknowledged": issues.get("security_hotspot_acknowledged", 0),
        "security_hotspots_fixed": issues.get("security_hotspot_fixed", 0),
        "security_hotspots_safe": issues.get("security_hotspot_safe", 0),
        # Quality metrics
        "coverage_percentage": float(metrics.get("coverage", 0)),
        "duplicated_lines_density": float(metrics.get("duplicated_lines_density", 0)),
        # New code bug metrics
        "new_code_bugs_total": int(metrics.get("new_bugs", 0)),
        "new_code_bugs_blocker": new_code_issues.get("new_code_bug_blocker", 0),
        "new_code_bugs_critical": new_code_issues.get("new_code_bug_critical", 0),
        "new_code_bugs_major": new_code_issues.get("new_code_bug_major", 0),
        "new_code_bugs_minor": new_code_issues.get("new_code_bug_minor", 0),
        "new_code_bugs_info": new_code_issues.get("new_code_bug_info", 0),
        # New code vulnerability metrics
        "new_code_vulnerabilities_total": int(metrics.get("new_vulnerabilities", 0)),
        "new_code_vulnerabilities_blocker": new_code_issues.get("new_code_vulnerability_blocker", 0),
        "new_code_vulnerabilities_critical": new_code_issues.get("new_code_vulnerability_critical", 0),
        "new_code_vulnerabilities_high": new_code_issues.get("new_code_vulnerability_major", 0),
        "new_code_vulnerabilities_medium": new_code_issues.get("new_code_vulnerability_minor", 0),
        "new_code_vulnerabilities_low": new_code_issues.get("new_code_vulnerability_info", 0),
        # New code code smell metrics
        "new_code_code_smells_total": int(metrics.get("new_code_smells", 0)),
        "new_code_code_smells_blocker": new_code_issues.get("new_code_code_smell_blocker", 0),
        "new_code_code_smells_critical": new_code_issues.get("new_code_code_smell_critical", 0),
        "new_code_code_smells_major": new_code_issues.get("new_code_code_smell_major", 0),
        "new_code_code_smells_minor": new_code_issues.get("new_code_code_smell_minor", 0),
        "new_code_code_smells_info": new_code_issues.get("new_code_code_smell_info", 0),
        # New code security hotspot metrics
        "new_code_security_hotspots_total": int(metrics.get("new_security_hotspots", 0)),
        "new_code_security_hotspots_high": new_code_issues.get("new_code_security_hotspot_high", 0),
        "new_code_security_hotspots_medium": new_code_issues.get("new_code_security_hotspot_medium", 0),
        "new_code_security_hotspots_low": new_code_issues.get("new_code_security_hotspot_low", 0),
        "new_code_security_hotspots_to_review": new_code_issues.get("new_code_security_hotspot_to_review", 0),
        # New code quality metrics
        "new_code_coverage_percentage": float(metrics.get("new_coverage", 0)),
        "new_code_duplicated_lines_density": float(metrics.get("new_duplicated_lines_density", 0)),
        "new_code_lines": int(metrics.get("new_lines", 0)),
    }


def compare_metrics(
    postgres_data: Dict[str, Any], sonarqube_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Compare metrics between PostgreSQL and SonarQube.

    Performs detailed comparison of metrics, identifying discrepancies and
    calculating variance statistics.

    Args:
        postgres_data: Latest snapshot from PostgreSQL
        sonarqube_data: Current data from SonarQube

    Returns:
        Comparison results with discrepancies and statistics
    """
    comparison_results = {
        "comparison_timestamp": datetime.now(pytz.UTC).isoformat(),
        "postgres_snapshot_time": postgres_data["snapshot_timestamp"].isoformat(),
        "sonarqube_fetch_time": sonarqube_data["fetch_timestamp"].isoformat(),
        "projects": {},
        "summary": {
            "total_projects_postgres": len(postgres_data["projects"]),
            "total_projects_sonarqube": len(sonarqube_data["projects"]),
            "projects_with_discrepancies": 0,
            "total_discrepancies": 0,
            "metrics_compared": len(METRIC_FIELDS),
        },
        "health_status": {
            "overall_status": "healthy",  # Will be updated based on checks
            "data_freshness": {},
            "data_completeness": {},
            "sync_status": {},
            "critical_issues": [],
        },
    }

    # Compare each project
    all_project_keys = set(postgres_data["projects"].keys()) | set(
        sonarqube_data["projects"].keys()
    )

    for project_key in all_project_keys:
        project_comparison = {
            "project_key": project_key,
            "in_postgres": project_key in postgres_data["projects"],
            "in_sonarqube": project_key in sonarqube_data["projects"],
            "discrepancies": [],
            "metrics": {},
        }

        if not project_comparison["in_postgres"]:
            project_comparison["status"] = "missing_in_postgres"
            comparison_results["projects"][project_key] = project_comparison
            continue

        if not project_comparison["in_sonarqube"]:
            project_comparison["status"] = "missing_in_sonarqube"
            comparison_results["projects"][project_key] = project_comparison
            continue

        # Both have the project - compare metrics
        pg_project = postgres_data["projects"][project_key]
        sq_project = sonarqube_data["projects"][project_key]

        if "error" in sq_project:
            project_comparison["status"] = "sonarqube_fetch_error"
            project_comparison["error"] = sq_project["error"]
            comparison_results["projects"][project_key] = project_comparison
            continue

        # Add metadata
        project_comparison["postgres_metric_date"] = str(pg_project["metric_date"])
        project_comparison["postgres_is_carried_forward"] = pg_project[
            "is_carried_forward"
        ]
        project_comparison["data_source_timestamp"] = pg_project[
            "data_source_timestamp"
        ].isoformat()

        # Compare each metric
        sq_metrics = sq_project["transformed_metrics"]
        has_discrepancy = False

        for metric_field in METRIC_FIELDS:
            pg_value = pg_project.get(metric_field, 0)
            sq_value = sq_metrics.get(metric_field, 0)

            # Handle None values
            if pg_value is None:
                pg_value = 0
            if sq_value is None:
                sq_value = 0

            # Calculate difference
            if isinstance(pg_value, (int, float)) and isinstance(
                sq_value, (int, float)
            ):
                difference = sq_value - pg_value
                if pg_value != 0:
                    percentage_diff = (difference / pg_value) * 100
                else:
                    percentage_diff = 100 if sq_value != 0 else 0
            else:
                difference = None
                percentage_diff = None

            metric_comparison = {
                "postgres_value": pg_value,
                "sonarqube_value": sq_value,
                "difference": difference,
                "percentage_difference": percentage_diff,
                "match": pg_value == sq_value,
            }

            project_comparison["metrics"][metric_field] = metric_comparison

            if not metric_comparison["match"]:
                has_discrepancy = True
                discrepancy = {
                    "metric": metric_field,
                    "postgres_value": pg_value,
                    "sonarqube_value": sq_value,
                    "difference": difference,
                    "percentage_difference": percentage_diff,
                }
                project_comparison["discrepancies"].append(discrepancy)
                comparison_results["summary"]["total_discrepancies"] += 1

        project_comparison["has_discrepancies"] = has_discrepancy
        project_comparison["discrepancy_count"] = len(
            project_comparison["discrepancies"]
        )

        if has_discrepancy:
            comparison_results["summary"]["projects_with_discrepancies"] += 1

        comparison_results["projects"][project_key] = project_comparison

    # Calculate health status
    comparison_results["health_status"] = calculate_health_status(
        comparison_results, postgres_data, sonarqube_data
    )

    return comparison_results


def generate_reports(**context) -> None:
    """Generate comparison reports in JSON and Markdown formats.

    Creates detailed reports showing the comparison results, including
    summary statistics, project-level details, and identified discrepancies.

    Args:
        **context: Airflow context containing XCom data
    """
    postgres_data = context["task_instance"].xcom_pull(
        task_ids="fetch_postgres_snapshot", key="postgres_snapshot"
    )
    sonarqube_data = context["task_instance"].xcom_pull(
        task_ids="fetch_sonarqube_current", key="sonarqube_current"
    )

    # Perform comparison
    comparison_results = compare_metrics(postgres_data, sonarqube_data)

    # Add additional context
    comparison_results["postgres_statistics"] = postgres_data.get("statistics", {})
    comparison_results["execution_date"] = context["execution_date"].isoformat()
    comparison_results["dag_run_id"] = context["dag_run"].run_id

    # Generate timestamp for filenames
    report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Ensure reports directory exists
    reports_dir = "/opt/airflow/reports"
    os.makedirs(reports_dir, exist_ok=True)

    # Generate JSON report
    json_filename = f"{reports_dir}/comparison_report_{report_timestamp}.json"
    with open(json_filename, "w") as f:
        json.dump(comparison_results, f, indent=2, default=str)
    logging.info(f"JSON report saved to {json_filename}")

    # Generate Markdown report
    md_filename = f"{reports_dir}/comparison_report_{report_timestamp}.md"
    md_content = generate_markdown_report(comparison_results)
    with open(md_filename, "w") as f:
        f.write(md_content)
    logging.info(f"Markdown report saved to {md_filename}")

    # Push report paths to XCom
    context["task_instance"].xcom_push(
        key="report_paths", value={"json": json_filename, "markdown": md_filename}
    )

    # Log summary
    summary = comparison_results["summary"]
    logging.info(
        f"""
    Comparison Report Summary:
    - Total Projects in PostgreSQL: {summary['total_projects_postgres']}
    - Total Projects in SonarQube: {summary['total_projects_sonarqube']}
    - Projects with Discrepancies: {summary['projects_with_discrepancies']}
    - Total Discrepancies Found: {summary['total_discrepancies']}
    """
    )


def calculate_health_status(
    comparison_results: Dict[str, Any],
    postgres_data: Dict[str, Any],
    sonarqube_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Calculate comprehensive health status of the data pipeline.

    Args:
        comparison_results: Comparison results
        postgres_data: Latest snapshot from PostgreSQL
        sonarqube_data: Current data from SonarQube

    Returns:
        Health status dictionary
    """
    health_status = {
        "overall_status": "healthy",
        "data_freshness": {},
        "data_completeness": {},
        "sync_status": {},
        "critical_issues": [],
        "warnings": [],
        "checks_performed": [],
    }

    # Data Freshness Check
    current_time = datetime.now(pytz.UTC)

    # Check PostgreSQL data freshness
    if postgres_data.get("statistics"):
        latest_metric_date = datetime.strptime(
            postgres_data["statistics"]["latest_metric_date"], "%Y-%m-%d"
        ).date()
        days_old = (current_time.date() - latest_metric_date).days

        health_status["data_freshness"]["postgres_latest_date"] = str(
            latest_metric_date
        )
        health_status["data_freshness"]["postgres_data_age_days"] = days_old
        health_status["data_freshness"]["postgres_status"] = (
            "healthy" if days_old <= 1 else "warning" if days_old <= 3 else "critical"
        )

        if days_old > 1:
            issue_level = "critical" if days_old > 3 else "warning"
            message = (
                f"PostgreSQL data is {days_old} days old (latest: {latest_metric_date})"
            )
            if issue_level == "critical":
                health_status["critical_issues"].append(message)
            else:
                health_status["warnings"].append(message)

    # Check carried forward data
    carried_forward_count = sum(
        1
        for p in postgres_data["projects"].values()
        if p.get("is_carried_forward", False)
    )
    carried_forward_percentage = (
        (carried_forward_count / len(postgres_data["projects"]) * 100)
        if postgres_data["projects"]
        else 0
    )

    health_status["data_freshness"]["carried_forward_projects"] = carried_forward_count
    health_status["data_freshness"]["carried_forward_percentage"] = round(
        carried_forward_percentage, 2
    )

    if carried_forward_percentage > 50:
        health_status["critical_issues"].append(
            f"{carried_forward_count} projects ({carried_forward_percentage:.1f}%) have carried forward data"
        )
    elif carried_forward_percentage > 20:
        health_status["warnings"].append(
            f"{carried_forward_count} projects ({carried_forward_percentage:.1f}%) have carried forward data"
        )

    # Data Completeness Check
    total_projects = max(
        len(postgres_data["projects"]), len(sonarqube_data["projects"])
    )
    missing_in_postgres = [
        k for k in sonarqube_data["projects"] if k not in postgres_data["projects"]
    ]
    missing_in_sonarqube = [
        k for k in postgres_data["projects"] if k not in sonarqube_data["projects"]
    ]

    health_status["data_completeness"]["total_unique_projects"] = total_projects
    health_status["data_completeness"]["missing_in_postgres_count"] = len(
        missing_in_postgres
    )
    health_status["data_completeness"]["missing_in_sonarqube_count"] = len(
        missing_in_sonarqube
    )
    health_status["data_completeness"]["completeness_percentage"] = (
        round(
            (total_projects - len(missing_in_postgres) - len(missing_in_sonarqube))
            / total_projects
            * 100,
            2,
        )
        if total_projects > 0
        else 100
    )

    if missing_in_postgres:
        health_status["critical_issues"].append(
            f"{len(missing_in_postgres)} projects missing in PostgreSQL"
        )
    if missing_in_sonarqube:
        health_status["warnings"].append(
            f"{len(missing_in_sonarqube)} projects missing in SonarQube"
        )

    # Sync Status Check
    projects_with_discrepancies = comparison_results["summary"][
        "projects_with_discrepancies"
    ]
    total_discrepancies = comparison_results["summary"]["total_discrepancies"]
    sync_percentage = (
        round(
            (1 - projects_with_discrepancies / len(comparison_results["projects"]))
            * 100,
            2,
        )
        if comparison_results["projects"]
        else 100
    )

    health_status["sync_status"]["projects_in_sync"] = (
        len(comparison_results["projects"]) - projects_with_discrepancies
    )
    health_status["sync_status"][
        "projects_with_discrepancies"
    ] = projects_with_discrepancies
    health_status["sync_status"]["total_discrepancies"] = total_discrepancies
    health_status["sync_status"]["sync_percentage"] = sync_percentage

    if sync_percentage < 80:
        health_status["critical_issues"].append(
            f"Only {sync_percentage}% of projects are in sync"
        )
    elif sync_percentage < 95:
        health_status["warnings"].append(
            f"{projects_with_discrepancies} projects have discrepancies"
        )

    # Fetch Error Check
    fetch_errors = [
        k
        for k, v in comparison_results["projects"].items()
        if v.get("status") == "sonarqube_fetch_error"
    ]
    if fetch_errors:
        health_status["critical_issues"].append(
            f"{len(fetch_errors)} projects failed to fetch from SonarQube"
        )

    # Determine overall status
    if health_status["critical_issues"]:
        health_status["overall_status"] = "critical"
    elif health_status["warnings"]:
        health_status["overall_status"] = "warning"
    else:
        health_status["overall_status"] = "healthy"

    # Add checks performed
    health_status["checks_performed"] = [
        "Data freshness (PostgreSQL latest date)",
        "Carried forward data percentage",
        "Data completeness (missing projects)",
        "Sync status (discrepancy analysis)",
        "SonarQube fetch errors",
    ]

    return health_status


def analyze_discrepancy_patterns(discrepancies: List[Dict[str, Any]]) -> Dict[str, str]:
    """Analyze patterns in discrepancies to identify common issues.
    
    Args:
        discrepancies: List of discrepancy dictionaries
        
    Returns:
        Dictionary of pattern descriptions
    """
    patterns = {}
    
    # Group discrepancies by type
    severity_discrepancies = []
    status_discrepancies = []
    total_discrepancies = []
    new_code_discrepancies = []
    
    for disc in discrepancies:
        metric = disc["metric"]
        
        if any(sev in metric for sev in ["blocker", "critical", "major", "minor", "info", "high", "medium", "low"]):
            severity_discrepancies.append(metric)
        elif any(status in metric for status in ["open", "confirmed", "reopened", "resolved", "closed", "false_positive", "wontfix", "to_review", "acknowledged", "fixed", "safe"]):
            status_discrepancies.append(metric)
        elif metric.endswith("_total"):
            total_discrepancies.append(metric)
        elif metric.startswith("new_code_"):
            new_code_discrepancies.append(metric)
    
    # Report patterns
    if severity_discrepancies:
        patterns["Severity breakdown mismatch"] = f"{len(severity_discrepancies)} metrics affected"
    if status_discrepancies:
        patterns["Status tracking discrepancy"] = f"{len(status_discrepancies)} metrics affected"
    if total_discrepancies:
        patterns["Total count mismatch"] = f"{len(total_discrepancies)} metrics affected"
    if new_code_discrepancies:
        patterns["New code metrics variance"] = f"{len(new_code_discrepancies)} metrics affected"
        
    return patterns


def validate_new_metrics(comparison_results: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Validate that new metrics are being captured correctly.
    
    Args:
        comparison_results: Comparison data structure
        
    Returns:
        Validation results by category
    """
    validation_results = {}
    
    # Check if any project has data
    has_data = len(comparison_results.get("projects", {})) > 0
    
    if not has_data:
        return {
            "Overall": {
                "status": "error",
                "details": "No projects found for validation"
            }
        }
    
    # Define validation checks
    validation_checks = {
        "Vulnerabilities BLOCKER Severity": {
            "metrics": ["vulnerabilities_blocker", "new_code_vulnerabilities_blocker"],
            "description": "BLOCKER severity for vulnerabilities"
        },
        "Issues FALSE-POSITIVE Status": {
            "metrics": ["bugs_false_positive", "vulnerabilities_false_positive", "code_smells_false_positive"],
            "description": "FALSE-POSITIVE status for all issue types"
        },
        "Issues WONTFIX Status": {
            "metrics": ["bugs_wontfix", "vulnerabilities_wontfix", "code_smells_wontfix"],
            "description": "WONTFIX status for all issue types"
        },
        "Code Smells Status Tracking": {
            "metrics": ["code_smells_open", "code_smells_confirmed", "code_smells_reopened", 
                       "code_smells_resolved", "code_smells_closed"],
            "description": "Complete status tracking for code smells"
        },
        "Security Hotspots SAFE Status": {
            "metrics": ["security_hotspots_safe"],
            "description": "SAFE status for security hotspots"
        },
        "New Code Severity Breakdown": {
            "metrics": ["new_code_bugs_blocker", "new_code_vulnerabilities_blocker", 
                       "new_code_code_smells_blocker"],
            "description": "Complete severity breakdown for new code"
        }
    }
    
    # Perform validation checks
    for check_name, check_config in validation_checks.items():
        metrics_found = 0
        metrics_with_data = 0
        
        for project_data in comparison_results["projects"].values():
            if "metrics" not in project_data:
                continue
                
            for metric in check_config["metrics"]:
                if metric in project_data["metrics"]:
                    metrics_found += 1
                    # Check if either PostgreSQL or SonarQube has non-zero data
                    metric_data = project_data["metrics"][metric]
                    if (metric_data.get("postgres_value", 0) != 0 or 
                        metric_data.get("sonarqube_value", 0) != 0):
                        metrics_with_data += 1
        
        if metrics_found == 0:
            status = "error"
            details = f"Metrics not found in comparison data"
        elif metrics_with_data == 0:
            status = "warning"
            details = f"Metrics present but no data captured yet"
        else:
            status = "valid"
            details = f"{metrics_with_data}/{metrics_found} metrics have data"
            
        validation_results[check_name] = {
            "status": status,
            "details": details
        }
    
    return validation_results


def generate_markdown_report(comparison_results: Dict[str, Any]) -> str:
    """Generate a Markdown formatted comparison report.

    Args:
        comparison_results: Comparison data structure

    Returns:
        Markdown formatted report string
    """
    report_time = datetime.now(pytz.UTC)

    md_lines = [
        f"# SonarQube ETL Data Quality Report",
        f"",
        f"**Generated:** {report_time.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"**Execution Date:** {comparison_results['execution_date']}",
        f"**DAG Run ID:** {comparison_results['dag_run_id']}",
        f"",
        f"## Health Status",
        f"",
    ]

    # Add health status section
    health_status = comparison_results.get("health_status", {})
    overall_status = health_status.get("overall_status", "unknown").upper()
    status_emoji = {
        "HEALTHY": "PASS",
        "WARNING": "WARN",
        "CRITICAL": "FAIL",
        "UNKNOWN": "????",
    }.get(overall_status, "????")

    md_lines.extend(
        [
            f"### Overall Status: {status_emoji} {overall_status}",
            f"",
        ]
    )

    # Critical Issues
    if health_status.get("critical_issues"):
        md_lines.extend(
            [
                f"#### Critical Issues",
                f"",
            ]
        )
        for issue in health_status["critical_issues"]:
            md_lines.append(f"- {issue}")
        md_lines.append("")

    # Warnings
    if health_status.get("warnings"):
        md_lines.extend(
            [
                f"#### Warnings",
                f"",
            ]
        )
        for warning in health_status["warnings"]:
            md_lines.append(f"- {warning}")
        md_lines.append("")

    # Health Metrics Table
    md_lines.extend(
        [
            f"### Health Metrics",
            f"",
            f"| Category | Metric | Value | Status |",
            f"|----------|--------|-------|--------|",
        ]
    )

    # Data Freshness
    freshness = health_status.get("data_freshness", {})
    if freshness:
        md_lines.extend(
            [
                f"| Data Freshness | Latest PostgreSQL Date | {freshness.get('postgres_latest_date', 'N/A')} | {freshness.get('postgres_status', 'N/A')} |",
                f"| Data Freshness | Data Age (days) | {freshness.get('postgres_data_age_days', 'N/A')} | - |",
                f"| Data Freshness | Carried Forward Projects | {freshness.get('carried_forward_projects', 0)} ({freshness.get('carried_forward_percentage', 0)}%) | - |",
            ]
        )

    # Data Completeness
    completeness = health_status.get("data_completeness", {})
    if completeness:
        md_lines.extend(
            [
                f"| Data Completeness | Total Unique Projects | {completeness.get('total_unique_projects', 0)} | - |",
                f"| Data Completeness | Missing in PostgreSQL | {completeness.get('missing_in_postgres_count', 0)} | - |",
                f"| Data Completeness | Missing in SonarQube | {completeness.get('missing_in_sonarqube_count', 0)} | - |",
                f"| Data Completeness | Completeness % | {completeness.get('completeness_percentage', 0)}% | - |",
            ]
        )

    # Sync Status
    sync_status = health_status.get("sync_status", {})
    if sync_status:
        md_lines.extend(
            [
                f"| Sync Status | Projects in Sync | {sync_status.get('projects_in_sync', 0)} | - |",
                f"| Sync Status | Projects with Discrepancies | {sync_status.get('projects_with_discrepancies', 0)} | - |",
                f"| Sync Status | Total Discrepancies | {sync_status.get('total_discrepancies', 0)} | - |",
                f"| Sync Status | Sync Percentage | {sync_status.get('sync_percentage', 0)}% | - |",
            ]
        )

    md_lines.extend(
        [
            f"",
            f"## Summary",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Projects in PostgreSQL | {comparison_results['summary']['total_projects_postgres']} |",
            f"| Total Projects in SonarQube | {comparison_results['summary']['total_projects_sonarqube']} |",
            f"| Projects with Discrepancies | {comparison_results['summary']['projects_with_discrepancies']} |",
            f"| Total Discrepancies | {comparison_results['summary']['total_discrepancies']} |",
            f"| Metrics Compared | {comparison_results['summary']['metrics_compared']} |",
            f"",
            f"## PostgreSQL Database Statistics",
            f"",
            f"| Statistic | Value |",
            f"|-----------|-------|",
        ]
    )

    # Add database statistics
    db_stats = comparison_results.get("postgres_statistics", {})
    for stat_key, stat_value in db_stats.items():
        md_lines.append(f"| {stat_key.replace('_', ' ').title()} | {stat_value} |")

    # Add Data Quality Metrics
    md_lines.extend(
        [
            f"",
            f"## Data Quality Metrics",
            f"",
            f"### Metric Coverage Analysis",
            f"",
            f"| Metric Type | Total Fields | Fields with Data | Coverage % |",
            f"|-------------|--------------|------------------|------------|",
        ]
    )

    # Calculate metric coverage
    metric_coverage = {}
    for project_data in comparison_results["projects"].values():
        if "metrics" in project_data:
            for metric, data in project_data["metrics"].items():
                if metric not in metric_coverage:
                    metric_coverage[metric] = {"total": 0, "non_zero": 0}
                metric_coverage[metric]["total"] += 1
                if data["postgres_value"] != 0 or data["sonarqube_value"] != 0:
                    metric_coverage[metric]["non_zero"] += 1

    # Group metrics by type
    metric_types = {
        "Bugs": [m for m in METRIC_FIELDS if m.startswith("bugs_") and not m.startswith("new_code_")],
        "Vulnerabilities": [
            m for m in METRIC_FIELDS if m.startswith("vulnerabilities_") and not m.startswith("new_code_")
        ],
        "Code Smells": [m for m in METRIC_FIELDS if m.startswith("code_smells_") and not m.startswith("new_code_")],
        "Security Hotspots": [
            m for m in METRIC_FIELDS if m.startswith("security_hotspots_") and not m.startswith("new_code_")
        ],
        "Coverage": [m for m in METRIC_FIELDS if "coverage" in m and not m.startswith("new_code_")],
        "Duplication": [m for m in METRIC_FIELDS if "duplicated" in m and not m.startswith("new_code_")],
        "New Code - Bugs": [m for m in METRIC_FIELDS if m.startswith("new_code_bugs_")],
        "New Code - Vulnerabilities": [m for m in METRIC_FIELDS if m.startswith("new_code_vulnerabilities_")],
        "New Code - Code Smells": [m for m in METRIC_FIELDS if m.startswith("new_code_code_smells_")],
        "New Code - Security Hotspots": [m for m in METRIC_FIELDS if m.startswith("new_code_security_hotspots_")],
        "New Code - Quality": [m for m in METRIC_FIELDS if m.startswith("new_code_") and any(q in m for q in ["coverage", "duplicated", "lines"])],
    }

    for type_name, metrics in metric_types.items():
        total_fields = len(metrics)
        fields_with_data = sum(
            1
            for m in metrics
            if m in metric_coverage and metric_coverage[m]["non_zero"] > 0
        )
        coverage_pct = (
            round(fields_with_data / total_fields * 100, 1) if total_fields > 0 else 0
        )
        md_lines.append(
            f"| {type_name} | {total_fields} | {fields_with_data} | {coverage_pct}% |"
        )

    # Add Metric Validation Section
    md_lines.extend(
        [
            f"",
            f"## Metric Validation Report",
            f"",
            f"### New Metrics Validation",
            f"",
            f"| Metric Category | Status | Details |",
            f"|-----------------|--------|---------|",
        ]
    )
    
    # Validate new metrics are being captured
    validation_results = validate_new_metrics(comparison_results)
    
    for category, result in validation_results.items():
        status_icon = "✅" if result["status"] == "valid" else "⚠️"
        md_lines.append(
            f"| {category} | {status_icon} {result['status']} | {result['details']} |"
        )
    
    # Add ETL Process Health
    md_lines.extend(
        [
            f"",
            f"## ETL Process Health",
            f"",
        ]
    )

    # Calculate ETL-specific metrics
    total_metrics = len(METRIC_FIELDS) * len(comparison_results["projects"])
    total_discrepancies = comparison_results["summary"]["total_discrepancies"]
    accuracy_rate = round((1 - total_discrepancies / total_metrics) * 100, 2) if total_metrics > 0 else 100
    
    md_lines.extend(
        [
            f"| ETL Metric | Value |",
            f"|------------|-------|",
            f"| Data Accuracy Rate | {accuracy_rate}% |",
            f"| Total Metrics Processed | {total_metrics} |",
            f"| Metrics with Discrepancies | {total_discrepancies} |",
            f"| Average Discrepancies per Project | {round(total_discrepancies / len(comparison_results['projects']), 2) if comparison_results['projects'] else 0} |",
        ]
    )
    
    # Add data pipeline recommendations
    md_lines.extend(
        [
            f"",
            f"### ETL Health Recommendations",
            f"",
        ]
    )
    
    recommendations = []
    
    # Check sync status
    sync_percentage = health_status.get("sync_status", {}).get("sync_percentage", 100)
    if sync_percentage < 100:
        recommendations.append(f"- Data sync is at {sync_percentage}%. Investigate ETL pipeline for data transformation issues.")
    
    # Check carried forward data
    carried_forward_pct = health_status.get("data_freshness", {}).get("carried_forward_percentage", 0)
    if carried_forward_pct > 0:
        recommendations.append(f"- {carried_forward_pct}% of projects have carried forward data. Check SonarQube API connectivity.")
    
    # Check for missing projects
    missing_count = health_status.get("data_completeness", {}).get("missing_in_postgres_count", 0)
    if missing_count > 0:
        recommendations.append(f"- {missing_count} projects are missing from PostgreSQL. Review ETL project discovery logic.")
    
    if not recommendations:
        recommendations.append("- ETL pipeline is operating normally with 100% data accuracy.")
    
    md_lines.extend(recommendations)

    # Add project details
    md_lines.extend([f"", f"## Project Comparison Details", f""])

    # Sort projects by discrepancy count
    projects_with_discrepancies = [
        (key, data)
        for key, data in comparison_results["projects"].items()
        if data.get("has_discrepancies", False)
    ]
    projects_with_discrepancies.sort(
        key=lambda x: x[1].get("discrepancy_count", 0), reverse=True
    )

    # Show summary even when no discrepancies
    if not projects_with_discrepancies:
        md_lines.extend([
            f"### All Projects in Sync ✅",
            f"",
            f"Great news! All {len(comparison_results['projects'])} projects have matching metrics between PostgreSQL and SonarQube.",
            f"",
            f"| Project | Last Updated | Data Status |",
            f"|---------|--------------|-------------|",
        ])
        
        for project_key, project_data in comparison_results['projects'].items():
            last_updated = project_data.get('postgres_metric_date', 'N/A')
            is_carried = project_data.get('postgres_is_carried_forward', False)
            status = "⚠️ Carried Forward" if is_carried else "✅ Fresh Data"
            md_lines.append(f"| {project_key} | {last_updated} | {status} |")
        
        md_lines.append("")

    if projects_with_discrepancies:
        md_lines.extend([f"### Projects with Discrepancies", f""])

        for project_key, project_data in projects_with_discrepancies:
            md_lines.extend(
                [
                    f"#### {project_key}",
                    f"",
                    f"- **PostgreSQL Metric Date:** {project_data.get('postgres_metric_date', 'N/A')}",
                    f"- **Data Carried Forward:** {project_data.get('postgres_is_carried_forward', False)}",
                    f"- **Discrepancy Count:** {project_data.get('discrepancy_count', 0)}",
                    f"",
                    f"| Metric | PostgreSQL | SonarQube | Difference | % Difference |",
                    f"|--------|------------|-----------|------------|--------------|",
                ]
            )

            # Show top discrepancies
            for discrepancy in project_data["discrepancies"][:10]:  # Limit to top 10
                metric = discrepancy["metric"]
                pg_val = discrepancy["postgres_value"]
                sq_val = discrepancy["sonarqube_value"]
                diff = discrepancy["difference"]
                pct_diff = discrepancy["percentage_difference"]

                if pct_diff is not None:
                    pct_str = f"{pct_diff:+.1f}%"
                else:
                    pct_str = "N/A"

                md_lines.append(
                    f"| {metric} | {pg_val} | {sq_val} | {diff:+d} | {pct_str} |"
                )

            if project_data["discrepancy_count"] > 10:
                md_lines.append(
                    f"| ... and {project_data['discrepancy_count'] - 10} more ... |"
                )

            md_lines.append("")
            
            # Add severity/status breakdown for discrepancies
            discrepancy_breakdown = analyze_discrepancy_patterns(project_data["discrepancies"])
            if discrepancy_breakdown:
                md_lines.extend([
                    f"",
                    f"**Discrepancy Analysis:**",
                    f"",
                ])
                for pattern, details in discrepancy_breakdown.items():
                    md_lines.append(f"- {pattern}: {details}")
                md_lines.append("")

    # Add projects with issues
    missing_in_postgres = [
        key
        for key, data in comparison_results["projects"].items()
        if data.get("status") == "missing_in_postgres"
    ]

    missing_in_sonarqube = [
        key
        for key, data in comparison_results["projects"].items()
        if data.get("status") == "missing_in_sonarqube"
    ]

    fetch_errors = [
        key
        for key, data in comparison_results["projects"].items()
        if data.get("status") == "sonarqube_fetch_error"
    ]

    if missing_in_postgres:
        md_lines.extend(
            [
                f"### Projects Missing in PostgreSQL",
                f"",
                f"The following projects exist in SonarQube but not in PostgreSQL:",
                f"",
            ]
        )
        for project in missing_in_postgres:
            md_lines.append(f"- {project}")
        md_lines.append("")

    if missing_in_sonarqube:
        md_lines.extend(
            [
                f"### Projects Missing in SonarQube",
                f"",
                f"The following projects exist in PostgreSQL but not in SonarQube:",
                f"",
            ]
        )
        for project in missing_in_sonarqube:
            md_lines.append(f"- {project}")
        md_lines.append("")

    if fetch_errors:
        md_lines.extend(
            [
                f"### Projects with Fetch Errors",
                f"",
                f"The following projects encountered errors during SonarQube data fetch:",
                f"",
            ]
        )
        for project in fetch_errors:
            error = comparison_results["projects"][project].get(
                "error", "Unknown error"
            )
            md_lines.append(f"- {project}: {error}")
        md_lines.append("")

    # Add footer
    md_lines.extend(
        [f"", f"---", f"*Report generated by SonarQube ETL Data Quality Report DAG*"]
    )

    return "\n".join(md_lines)


# Define main tasks
fetch_postgres_task = PythonOperator(
    task_id="fetch_postgres_snapshot",
    python_callable=fetch_postgres_latest_snapshot,
    dag=dag,
)

fetch_sonarqube_task = PythonOperator(
    task_id="fetch_sonarqube_current",
    python_callable=fetch_sonarqube_current_data,
    dag=dag,
)

generate_reports_task = PythonOperator(
    task_id="generate_reports", python_callable=generate_reports, dag=dag
)

# Set task dependencies
[fetch_postgres_task, fetch_sonarqube_task] >> generate_reports_task
