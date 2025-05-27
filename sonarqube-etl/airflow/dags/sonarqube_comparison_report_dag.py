"""SonarQube Comparison Report DAG.

This module implements an Airflow DAG that generates comprehensive comparison reports
between SonarQube metrics stored in PostgreSQL and live data from the SonarQube server.
The DAG runs after ETL processes complete and produces reports in both JSON and Markdown formats.

The comparison includes:
- Latest snapshot data from PostgreSQL vs current SonarQube data
- Metric-by-metric comparison with variance analysis
- Timezone-aware timestamp handling
- Data freshness validation
- Discrepancy detection and reporting

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
SONARQUBE_METRICS_DATASET = Dataset("postgres://sonarqube_metrics_db/daily_project_metrics")

# Timezone configurations
TIMEZONE_MAPPING = {
    'sonarqube_server': 'UTC',  # SonarQube typically stores in UTC
    'airflow': 'UTC',  # Airflow configured timezone
    'mexico_city': 'America/Mexico_City',  # Local reporting timezone
    'postgres': 'UTC'  # Database timezone
}

# Metrics to compare
METRIC_FIELDS = [
    # Bug metrics
    'bugs_total', 'bugs_blocker', 'bugs_critical', 'bugs_major', 'bugs_minor', 'bugs_info',
    'bugs_open', 'bugs_confirmed', 'bugs_reopened', 'bugs_resolved', 'bugs_closed',
    
    # Vulnerability metrics
    'vulnerabilities_total', 'vulnerabilities_critical', 'vulnerabilities_high',
    'vulnerabilities_medium', 'vulnerabilities_low',
    'vulnerabilities_open', 'vulnerabilities_confirmed', 'vulnerabilities_reopened',
    'vulnerabilities_resolved', 'vulnerabilities_closed',
    
    # Code smell metrics
    'code_smells_total', 'code_smells_blocker', 'code_smells_critical',
    'code_smells_major', 'code_smells_minor', 'code_smells_info',
    
    # Security hotspot metrics
    'security_hotspots_total', 'security_hotspots_high', 'security_hotspots_medium',
    'security_hotspots_low', 'security_hotspots_to_review', 'security_hotspots_reviewed',
    'security_hotspots_acknowledged', 'security_hotspots_fixed',
    
    # Quality metrics
    'coverage_percentage', 'duplicated_lines_density',
    
    # New code metrics
    'new_code_bugs_total', 'new_code_vulnerabilities_total', 'new_code_code_smells_total',
    'new_code_security_hotspots_total', 'new_code_coverage_percentage',
    'new_code_duplicated_lines_density', 'new_code_lines'
]

# Default args for the DAG
default_args = {
    'owner': 'devops-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'sonarqube_comparison_report',
    default_args=default_args,
    description='Generate comparison reports between PostgreSQL and SonarQube data',
    schedule=[SONARQUBE_METRICS_DATASET],  # Data-aware scheduling
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ## SonarQube Comparison Report DAG
    
    This DAG generates comprehensive comparison reports between metrics stored in PostgreSQL
    and live data from SonarQube server. It validates data consistency and identifies
    any discrepancies.
    
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
    """
)


def get_sonarqube_config() -> Dict[str, Any]:
    """Get SonarQube configuration from environment variables.
    
    Returns:
        Dict[str, Any]: Configuration dictionary with connection details
    """
    token = os.environ.get('AIRFLOW_VAR_SONARQUBE_TOKEN')
    if not token:
        logging.error("SONARQUBE_TOKEN not found in environment!")
        raise KeyError("SONARQUBE_TOKEN not configured in environment")
    
    base_url = os.environ.get('AIRFLOW_VAR_SONARQUBE_BASE_URL', 'http://sonarqube:9000')
    
    return {
        'base_url': base_url,
        'token': token,
        'auth': (token, ''),
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
    
    pg_hook = PostgresHook(postgres_conn_id='sonarqube_metrics_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Get latest metrics for each project
        cursor.execute("""
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
        """)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        postgres_data = {
            'snapshot_timestamp': datetime.now(pytz.UTC),
            'projects': {}
        }
        
        for row in rows:
            data = dict(zip(columns, row))
            project_key = data['sonarqube_project_key']
            
            # Convert timestamps to UTC
            if data['data_source_timestamp']:
                data['data_source_timestamp'] = convert_timezone(
                    data['data_source_timestamp'], 'postgres', 'sonarqube_server'
                )
            
            postgres_data['projects'][project_key] = data
        
        # Get database statistics
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT project_id) as total_projects,
                MAX(metric_date) as latest_metric_date,
                MIN(metric_date) as earliest_metric_date,
                COUNT(*) as total_records
            FROM sonarqube_metrics.daily_project_metrics
        """)
        
        stats_row = cursor.fetchone()
        postgres_data['statistics'] = {
            'total_projects': stats_row[0],
            'latest_metric_date': str(stats_row[1]),
            'earliest_metric_date': str(stats_row[2]),
            'total_records': stats_row[3]
        }
        
        context['task_instance'].xcom_push(key='postgres_snapshot', value=postgres_data)
        logging.info(f"Retrieved data for {len(postgres_data['projects'])} projects from PostgreSQL")
        
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
    
    sonarqube_data = {
        'fetch_timestamp': datetime.now(pytz.UTC),
        'projects': {}
    }
    
    for project in projects:
        project_key = project['key']
        logging.info(f"Fetching current metrics for {project_key}")
        
        try:
            # Get current metrics
            metrics = client.fetch_current_metrics(project_key)
            
            # Get issue breakdowns
            issues_breakdown = client.fetch_issue_breakdown(project_key, is_new_code=False)
            new_code_issues = client.fetch_issue_breakdown(project_key, is_new_code=True)
            
            # Transform to match database structure
            transformed_data = transform_sonarqube_to_db_format(
                metrics, issues_breakdown, new_code_issues
            )
            
            sonarqube_data['projects'][project_key] = {
                'project_name': project['name'],
                'raw_metrics': metrics,
                'transformed_metrics': transformed_data,
                'fetch_time': datetime.now(pytz.UTC)
            }
            
        except Exception as e:
            logging.error(f"Failed to fetch metrics for {project_key}: {str(e)}")
            sonarqube_data['projects'][project_key] = {
                'project_name': project['name'],
                'error': str(e),
                'fetch_time': datetime.now(pytz.UTC)
            }
    
    context['task_instance'].xcom_push(key='sonarqube_current', value=sonarqube_data)
    logging.info(f"Retrieved data for {len(sonarqube_data['projects'])} projects from SonarQube")
    
    return sonarqube_data


def transform_sonarqube_to_db_format(metrics: Dict[str, Any], 
                                   issues: Dict[str, int], 
                                   new_code_issues: Dict[str, int]) -> Dict[str, Any]:
    """Transform SonarQube API data to match database schema format.
    
    Args:
        metrics: Raw metrics from SonarQube API
        issues: Issue breakdown by type/severity/status
        new_code_issues: New code issue breakdown
        
    Returns:
        Transformed data matching database column names
    """
    return {
        'bugs_total': int(metrics.get('bugs', 0)),
        'bugs_blocker': issues.get('bug_blocker', 0),
        'bugs_critical': issues.get('bug_critical', 0),
        'bugs_major': issues.get('bug_major', 0),
        'bugs_minor': issues.get('bug_minor', 0),
        'bugs_info': issues.get('bug_info', 0),
        'bugs_open': issues.get('bug_open', 0),
        'bugs_confirmed': issues.get('bug_confirmed', 0),
        'bugs_reopened': issues.get('bug_reopened', 0),
        'bugs_resolved': issues.get('bug_resolved', 0),
        'bugs_closed': issues.get('bug_closed', 0),
        
        'vulnerabilities_total': int(metrics.get('vulnerabilities', 0)),
        'vulnerabilities_critical': issues.get('vulnerability_critical', 0),
        'vulnerabilities_high': issues.get('vulnerability_major', 0),
        'vulnerabilities_medium': issues.get('vulnerability_minor', 0),
        'vulnerabilities_low': issues.get('vulnerability_info', 0),
        'vulnerabilities_open': issues.get('vulnerability_open', 0),
        'vulnerabilities_confirmed': issues.get('vulnerability_confirmed', 0),
        'vulnerabilities_reopened': issues.get('vulnerability_reopened', 0),
        'vulnerabilities_resolved': issues.get('vulnerability_resolved', 0),
        'vulnerabilities_closed': issues.get('vulnerability_closed', 0),
        
        'code_smells_total': int(metrics.get('code_smells', 0)),
        'code_smells_blocker': issues.get('code_smell_blocker', 0),
        'code_smells_critical': issues.get('code_smell_critical', 0),
        'code_smells_major': issues.get('code_smell_major', 0),
        'code_smells_minor': issues.get('code_smell_minor', 0),
        'code_smells_info': issues.get('code_smell_info', 0),
        
        'security_hotspots_total': int(metrics.get('security_hotspots', 0)),
        'security_hotspots_high': issues.get('security_hotspot_high', 0),
        'security_hotspots_medium': issues.get('security_hotspot_medium', 0),
        'security_hotspots_low': issues.get('security_hotspot_low', 0),
        'security_hotspots_to_review': issues.get('security_hotspot_to_review', 0),
        'security_hotspots_reviewed': issues.get('security_hotspot_reviewed', 0),
        'security_hotspots_acknowledged': issues.get('security_hotspot_acknowledged', 0),
        'security_hotspots_fixed': issues.get('security_hotspot_fixed', 0),
        
        'coverage_percentage': float(metrics.get('coverage', 0)),
        'duplicated_lines_density': float(metrics.get('duplicated_lines_density', 0)),
        
        'new_code_bugs_total': int(metrics.get('new_bugs', 0)),
        'new_code_vulnerabilities_total': int(metrics.get('new_vulnerabilities', 0)),
        'new_code_code_smells_total': int(metrics.get('new_code_smells', 0)),
        'new_code_security_hotspots_total': int(metrics.get('new_security_hotspots', 0)),
        'new_code_coverage_percentage': float(metrics.get('new_coverage', 0)),
        'new_code_duplicated_lines_density': float(metrics.get('new_duplicated_lines_density', 0)),
        'new_code_lines': int(metrics.get('new_lines', 0))
    }


def compare_metrics(postgres_data: Dict[str, Any], sonarqube_data: Dict[str, Any]) -> Dict[str, Any]:
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
        'comparison_timestamp': datetime.now(pytz.UTC).isoformat(),
        'postgres_snapshot_time': postgres_data['snapshot_timestamp'].isoformat(),
        'sonarqube_fetch_time': sonarqube_data['fetch_timestamp'].isoformat(),
        'timezone_info': TIMEZONE_MAPPING,
        'projects': {},
        'summary': {
            'total_projects_postgres': len(postgres_data['projects']),
            'total_projects_sonarqube': len(sonarqube_data['projects']),
            'projects_with_discrepancies': 0,
            'total_discrepancies': 0,
            'metrics_compared': len(METRIC_FIELDS)
        }
    }
    
    # Compare each project
    all_project_keys = set(postgres_data['projects'].keys()) | set(sonarqube_data['projects'].keys())
    
    for project_key in all_project_keys:
        project_comparison = {
            'project_key': project_key,
            'in_postgres': project_key in postgres_data['projects'],
            'in_sonarqube': project_key in sonarqube_data['projects'],
            'discrepancies': [],
            'metrics': {}
        }
        
        if not project_comparison['in_postgres']:
            project_comparison['status'] = 'missing_in_postgres'
            comparison_results['projects'][project_key] = project_comparison
            continue
            
        if not project_comparison['in_sonarqube']:
            project_comparison['status'] = 'missing_in_sonarqube'
            comparison_results['projects'][project_key] = project_comparison
            continue
        
        # Both have the project - compare metrics
        pg_project = postgres_data['projects'][project_key]
        sq_project = sonarqube_data['projects'][project_key]
        
        if 'error' in sq_project:
            project_comparison['status'] = 'sonarqube_fetch_error'
            project_comparison['error'] = sq_project['error']
            comparison_results['projects'][project_key] = project_comparison
            continue
        
        # Add metadata
        project_comparison['postgres_metric_date'] = str(pg_project['metric_date'])
        project_comparison['postgres_is_carried_forward'] = pg_project['is_carried_forward']
        project_comparison['data_source_timestamp'] = pg_project['data_source_timestamp'].isoformat()
        
        # Compare each metric
        sq_metrics = sq_project['transformed_metrics']
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
            if isinstance(pg_value, (int, float)) and isinstance(sq_value, (int, float)):
                difference = sq_value - pg_value
                if pg_value != 0:
                    percentage_diff = (difference / pg_value) * 100
                else:
                    percentage_diff = 100 if sq_value != 0 else 0
            else:
                difference = None
                percentage_diff = None
            
            metric_comparison = {
                'postgres_value': pg_value,
                'sonarqube_value': sq_value,
                'difference': difference,
                'percentage_difference': percentage_diff,
                'match': pg_value == sq_value
            }
            
            project_comparison['metrics'][metric_field] = metric_comparison
            
            if not metric_comparison['match']:
                has_discrepancy = True
                discrepancy = {
                    'metric': metric_field,
                    'postgres_value': pg_value,
                    'sonarqube_value': sq_value,
                    'difference': difference,
                    'percentage_difference': percentage_diff
                }
                project_comparison['discrepancies'].append(discrepancy)
                comparison_results['summary']['total_discrepancies'] += 1
        
        project_comparison['has_discrepancies'] = has_discrepancy
        project_comparison['discrepancy_count'] = len(project_comparison['discrepancies'])
        
        if has_discrepancy:
            comparison_results['summary']['projects_with_discrepancies'] += 1
        
        comparison_results['projects'][project_key] = project_comparison
    
    return comparison_results


def generate_reports(**context) -> None:
    """Generate comparison reports in JSON and Markdown formats.
    
    Creates detailed reports showing the comparison results, including
    summary statistics, project-level details, and identified discrepancies.
    
    Args:
        **context: Airflow context containing XCom data
    """
    postgres_data = context['task_instance'].xcom_pull(task_ids='fetch_postgres_snapshot', 
                                                      key='postgres_snapshot')
    sonarqube_data = context['task_instance'].xcom_pull(task_ids='fetch_sonarqube_current', 
                                                        key='sonarqube_current')
    
    # Perform comparison
    comparison_results = compare_metrics(postgres_data, sonarqube_data)
    
    # Add additional context
    comparison_results['postgres_statistics'] = postgres_data.get('statistics', {})
    comparison_results['execution_date'] = context['execution_date'].isoformat()
    comparison_results['dag_run_id'] = context['dag_run'].run_id
    
    # Generate timestamp for filenames
    report_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Ensure reports directory exists
    reports_dir = '/opt/airflow/reports'
    os.makedirs(reports_dir, exist_ok=True)
    
    # Generate JSON report
    json_filename = f"{reports_dir}/comparison_report_{report_timestamp}.json"
    with open(json_filename, 'w') as f:
        json.dump(comparison_results, f, indent=2, default=str)
    logging.info(f"JSON report saved to {json_filename}")
    
    # Generate Markdown report
    md_filename = f"{reports_dir}/comparison_report_{report_timestamp}.md"
    md_content = generate_markdown_report(comparison_results)
    with open(md_filename, 'w') as f:
        f.write(md_content)
    logging.info(f"Markdown report saved to {md_filename}")
    
    # Push report paths to XCom
    context['task_instance'].xcom_push(key='report_paths', value={
        'json': json_filename,
        'markdown': md_filename
    })
    
    # Log summary
    summary = comparison_results['summary']
    logging.info(f"""
    Comparison Report Summary:
    - Total Projects in PostgreSQL: {summary['total_projects_postgres']}
    - Total Projects in SonarQube: {summary['total_projects_sonarqube']}
    - Projects with Discrepancies: {summary['projects_with_discrepancies']}
    - Total Discrepancies Found: {summary['total_discrepancies']}
    """)


def generate_markdown_report(comparison_results: Dict[str, Any]) -> str:
    """Generate a Markdown formatted comparison report.
    
    Args:
        comparison_results: Comparison data structure
        
    Returns:
        Markdown formatted report string
    """
    # Convert timezone info to Mexico City for report
    mexico_tz = pytz.timezone(TIMEZONE_MAPPING['mexico_city'])
    report_time = datetime.now(mexico_tz)
    
    md_lines = [
        f"# SonarQube Metrics Comparison Report",
        f"",
        f"**Generated:** {report_time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"**Execution Date:** {comparison_results['execution_date']}",
        f"**DAG Run ID:** {comparison_results['dag_run_id']}",
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
    
    # Add database statistics
    db_stats = comparison_results.get('postgres_statistics', {})
    for stat_key, stat_value in db_stats.items():
        md_lines.append(f"| {stat_key.replace('_', ' ').title()} | {stat_value} |")
    
    md_lines.extend([
        f"",
        f"## Timezone Information",
        f"",
        f"| System | Timezone |",
        f"|--------|----------|",
    ])
    
    # Add timezone info
    for system, tz in TIMEZONE_MAPPING.items():
        md_lines.append(f"| {system.replace('_', ' ').title()} | {tz} |")
    
    # Add project details
    md_lines.extend([
        f"",
        f"## Project Comparison Details",
        f""
    ])
    
    # Sort projects by discrepancy count
    projects_with_discrepancies = [
        (key, data) for key, data in comparison_results['projects'].items()
        if data.get('has_discrepancies', False)
    ]
    projects_with_discrepancies.sort(key=lambda x: x[1].get('discrepancy_count', 0), reverse=True)
    
    if projects_with_discrepancies:
        md_lines.extend([
            f"### Projects with Discrepancies",
            f""
        ])
        
        for project_key, project_data in projects_with_discrepancies:
            md_lines.extend([
                f"#### {project_key}",
                f"",
                f"- **PostgreSQL Metric Date:** {project_data.get('postgres_metric_date', 'N/A')}",
                f"- **Data Carried Forward:** {project_data.get('postgres_is_carried_forward', False)}",
                f"- **Discrepancy Count:** {project_data.get('discrepancy_count', 0)}",
                f"",
                f"| Metric | PostgreSQL | SonarQube | Difference | % Difference |",
                f"|--------|------------|-----------|------------|--------------|",
            ])
            
            # Show top discrepancies
            for discrepancy in project_data['discrepancies'][:10]:  # Limit to top 10
                metric = discrepancy['metric']
                pg_val = discrepancy['postgres_value']
                sq_val = discrepancy['sonarqube_value']
                diff = discrepancy['difference']
                pct_diff = discrepancy['percentage_difference']
                
                if pct_diff is not None:
                    pct_str = f"{pct_diff:+.1f}%"
                else:
                    pct_str = "N/A"
                
                md_lines.append(
                    f"| {metric} | {pg_val} | {sq_val} | {diff:+d} | {pct_str} |"
                )
            
            if project_data['discrepancy_count'] > 10:
                md_lines.append(f"| ... and {project_data['discrepancy_count'] - 10} more ... |")
            
            md_lines.append("")
    
    # Add projects with issues
    missing_in_postgres = [
        key for key, data in comparison_results['projects'].items()
        if data.get('status') == 'missing_in_postgres'
    ]
    
    missing_in_sonarqube = [
        key for key, data in comparison_results['projects'].items()
        if data.get('status') == 'missing_in_sonarqube'
    ]
    
    fetch_errors = [
        key for key, data in comparison_results['projects'].items()
        if data.get('status') == 'sonarqube_fetch_error'
    ]
    
    if missing_in_postgres:
        md_lines.extend([
            f"### Projects Missing in PostgreSQL",
            f"",
            f"The following projects exist in SonarQube but not in PostgreSQL:",
            f""
        ])
        for project in missing_in_postgres:
            md_lines.append(f"- {project}")
        md_lines.append("")
    
    if missing_in_sonarqube:
        md_lines.extend([
            f"### Projects Missing in SonarQube",
            f"",
            f"The following projects exist in PostgreSQL but not in SonarQube:",
            f""
        ])
        for project in missing_in_sonarqube:
            md_lines.append(f"- {project}")
        md_lines.append("")
    
    if fetch_errors:
        md_lines.extend([
            f"### Projects with Fetch Errors",
            f"",
            f"The following projects encountered errors during SonarQube data fetch:",
            f""
        ])
        for project in fetch_errors:
            error = comparison_results['projects'][project].get('error', 'Unknown error')
            md_lines.append(f"- {project}: {error}")
        md_lines.append("")
    
    # Add footer
    md_lines.extend([
        f"",
        f"---",
        f"*Report generated by SonarQube Comparison Report DAG*"
    ])
    
    return "\n".join(md_lines)


# Define main tasks
fetch_postgres_task = PythonOperator(
    task_id='fetch_postgres_snapshot',
    python_callable=fetch_postgres_latest_snapshot,
    dag=dag
)

fetch_sonarqube_task = PythonOperator(
    task_id='fetch_sonarqube_current',
    python_callable=fetch_sonarqube_current_data,
    dag=dag
)

generate_reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag
)

# Set task dependencies
[fetch_postgres_task, fetch_sonarqube_task] >> generate_reports_task