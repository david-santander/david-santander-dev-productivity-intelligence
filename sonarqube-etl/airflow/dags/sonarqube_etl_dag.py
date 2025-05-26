"""SonarQube ETL DAG for daily metrics extraction.

This module implements an Airflow DAG that extracts code quality metrics from SonarQube
and loads them into PostgreSQL for dashboard visualization and trend analysis.

The DAG runs daily at 2 AM and collects metrics for the previous day, including:
- Code issues (bugs, vulnerabilities, code smells, security hotspots)
- Code coverage and duplication metrics
- Quality ratings
- New code period metrics

Example:
    To manually trigger the DAG::
    
        airflow dags trigger sonarqube_etl

Note:
    For historical data backfill, use the dedicated sonarqube_etl_backfill DAG.

Attributes:
    SONARQUBE_METRICS (List[str]): Core metrics to extract from SonarQube
    NEW_CODE_METRICS (List[str]): Metrics specific to new code period
    ISSUE_TYPES (List[str]): Types of issues to track
    SEVERITIES (List[str]): Issue severity levels
    STATUSES (List[str]): Issue status values
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

# Default args for the DAG
default_args = {
    'owner': 'devops-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

# DAG definition
dag = DAG(
    'sonarqube_etl',
    default_args=default_args,
    description='ETL process to extract SonarQube metrics and load into PostgreSQL',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    catchup=False,  # Disable catchup - use dedicated backfill DAG instead
    max_active_runs=1,
    doc_md="""
    ## SonarQube ETL DAG
    
    This DAG extracts metrics from SonarQube and loads them into PostgreSQL for dashboard visualization.
    
    ### Regular Execution
    - Runs daily at 2 AM
    - Extracts metrics for the previous day
    - Handles carry-forward for missing data
    
    ### Features
    - Extracts both overall and new code metrics
    - Tracks issue severity breakdowns
    - Monitors code quality trends
    - Supports multiple projects
    
    ### For Historical Data Backfill
    
    Use the dedicated `sonarqube_etl_backfill` DAG for backfilling historical data.
    
    ```bash
    # Backfill year-to-date
    airflow dags trigger sonarqube_etl_backfill
    
    # Backfill specific range
    airflow dags trigger sonarqube_etl_backfill --conf '{"start_date": "2025-01-01", "end_date": "2025-03-31"}'
    ```
    """
)

def get_sonarqube_config() -> Dict[str, Any]:
    """Get SonarQube configuration from Airflow Variables or Environment.
    
    Retrieves the SonarQube API token and base URL from environment variables.
    The token is required for authentication with the SonarQube API.
    
    Returns:
        Dict[str, Any]: Configuration dictionary containing:
            - base_url (str): SonarQube server URL
            - token (str): API authentication token
            - auth (Tuple[str, str]): Authentication tuple for requests
            
    Raises:
        KeyError: If SONARQUBE_TOKEN environment variable is not set
        
    Example:
        >>> config = get_sonarqube_config()
        >>> response = requests.get(
        ...     f"{config['base_url']}/api/projects/search",
        ...     auth=config['auth']
        ... )
    """
    # Force using environment variable for now
    token = os.environ.get('AIRFLOW_VAR_SONARQUBE_TOKEN')
    if not token:
        logging.error("SONARQUBE_TOKEN not found in environment!")
        raise KeyError("SONARQUBE_TOKEN not configured in environment")
    logging.info("Using SONARQUBE_TOKEN from environment variable")
    
    # Get base URL from environment
    base_url = os.environ.get('AIRFLOW_VAR_SONARQUBE_BASE_URL', 'http://sonarqube:9000')
    
    return {
        'base_url': base_url,
        'token': token,
        'auth': (token, ''),
    }

def fetch_projects(**context) -> List[Dict[str, Any]]:
    """Fetch all projects from SonarQube.
    
    Retrieves the complete list of projects from SonarQube using pagination.
    This task is the entry point for the ETL process and provides the project
    list to subsequent tasks.
    
    Args:
        **context: Airflow context dictionary containing task instance and
                  execution date information
                  
    Returns:
        List[Dict[str, Any]]: List of project dictionaries containing:
            - key (str): Unique project identifier
            - name (str): Project display name
            - qualifier (str): Project type qualifier
            - visibility (str): Project visibility setting
            
    Raises:
        requests.HTTPError: If SonarQube API returns an error
        KeyError: If authentication token is not configured
        
    Note:
        Results are pushed to XCom with key 'projects' for downstream tasks.
        
    Example:
        The function is typically called by Airflow's PythonOperator::
        
            fetch_task = PythonOperator(
                task_id='fetch_projects',
                python_callable=fetch_projects
            )
    """
    config = get_sonarqube_config()
    client = SonarQubeClient(config)
    
    projects = client.fetch_all_projects()
    
    context['task_instance'].xcom_push(key='projects', value=projects)
    return projects

def fetch_project_metrics(project_key: str, metric_date: str, config: Dict[str, Any], 
                         use_history: bool = False) -> Dict[str, Any]:
    """Fetch metrics for a specific project, optionally from historical data.
    
    This function now delegates to the SonarQubeClient for all API interactions.
    It's maintained for backward compatibility with the existing DAG structure.
    
    Args:
        project_key (str): SonarQube project key identifier
        metric_date (str): Date to fetch metrics for (YYYY-MM-DD format)
        config (Dict[str, Any]): SonarQube configuration dictionary from get_sonarqube_config()
        use_history (bool, optional): If True, uses historical API for past data.
                                     If False, uses smart selection based on date.
                                     Defaults to False.
                                     
    Returns:
        Dict[str, Any]: Dictionary containing:
            - project_key (str): The project identifier
            - metric_date (str): The date of the metrics
            - metrics (Dict[str, Any] or None): Core metrics dictionary or None if unavailable
            - issues_breakdown (Dict[str, int]): Issue counts by type/severity/status
            - new_code_issues_breakdown (Dict[str, int]): New code issue counts
            - error (str, optional): Error message if metrics fetch failed
            
    Note:
        This function is maintained for backward compatibility. The actual
        implementation is now in SonarQubeClient.
        
    Example:
        >>> config = get_sonarqube_config()
        >>> metrics = fetch_project_metrics(
        ...     'my-project', 
        ...     '2025-01-15', 
        ...     config,
        ...     use_history=True
        ... )
    """
    client = SonarQubeClient(config)
    return client.fetch_project_metrics(project_key, metric_date, use_history)

def extract_metrics_for_project(**context) -> int:
    """Extract metrics for all projects for the previous day.
    
    Main extraction task that iterates through all projects and fetches their
    metrics for yesterday's date. This ensures we capture metrics after daily
    analysis runs have completed.
    
    Args:
        **context: Airflow context dictionary containing:
            - task_instance: For XCom communication
            - execution_date: Current execution date
            
    Returns:
        int: Number of metric records extracted
        
    Note:
        - Always extracts yesterday's data relative to execution_date
        - Handles failures gracefully by recording error states
        - Results are pushed to XCom with key 'raw_metrics'
        - Now uses SonarQubeClient for all API operations
        
    Example:
        This function is designed to be called by Airflow's PythonOperator
        and relies on the output of fetch_projects task::
        
            extract_task = PythonOperator(
                task_id='extract_metrics',
                python_callable=extract_metrics_for_project
            )
    """
    projects = context['task_instance'].xcom_pull(task_ids='fetch_projects', key='projects')
    config = get_sonarqube_config()
    client = SonarQubeClient(config)
    
    # Always extract yesterday's data for daily runs
    execution_date = context['execution_date']
    metric_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logging.info(f"Extracting metrics for date: {metric_date}")
    
    all_metrics = []
    
    for project in projects:
        project_key = project['key']
        logging.info(f"Extracting metrics for project: {project_key}")
        
        try:
            # Use the smart fetch method which automatically selects the right API
            metrics_data = client.fetch_metrics_smart(project_key, metric_date)
            all_metrics.append(metrics_data)
        except Exception as e:
            logging.error(f"Failed to fetch metrics for {project_key} on {metric_date}: {str(e)}")
            # For missing data, we'll handle carry-forward logic in the transform step
            all_metrics.append({
                'project_key': project_key,
                'metric_date': metric_date,
                'metrics': None,
                'issues_breakdown': None,
                'new_code_issues_breakdown': None,
                'error': str(e)
            })
    
    context['task_instance'].xcom_push(key='raw_metrics', value=all_metrics)
    logging.info(f"Extracted {len(all_metrics)} metric records for {metric_date}")
    return len(all_metrics)

def transform_and_load_metrics(**context) -> None:
    """Transform and load metrics into PostgreSQL.
    
    Final task in the ETL pipeline that transforms raw metrics into the database
    schema format and loads them into PostgreSQL. Implements carry-forward logic
    for missing data points.
    
    Args:
        **context: Airflow context dictionary containing:
            - task_instance: For retrieving XCom data from previous tasks
            
    Raises:
        Exception: If database operations fail (transaction is rolled back)
        
    Note:
        - Uses database transactions for data integrity
        - Implements upsert logic to handle re-runs
        - Carries forward last known values when current data unavailable
        
    Process:
        1. Retrieves raw metrics and projects from XCom
        2. Ensures all projects exist in database
        3. Transforms metrics to database format
        4. Handles carry-forward for missing data
        5. Commits all changes atomically
    """
    raw_metrics = context['task_instance'].xcom_pull(task_ids='extract_metrics', key='raw_metrics')
    projects = context['task_instance'].xcom_pull(task_ids='fetch_projects', key='projects')
    
    pg_hook = PostgresHook(postgres_conn_id='sonarqube_metrics_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Create project mapping
    project_map = {p['key']: p['name'] for p in projects}
    
    try:
        # First, ensure all projects exist in the database
        for project_key, project_name in project_map.items():
            cursor.execute("""
                INSERT INTO sonarqube_metrics.sq_projects (sonarqube_project_key, project_name)
                VALUES (%s, %s)
                ON CONFLICT (sonarqube_project_key) DO UPDATE
                SET project_name = EXCLUDED.project_name,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING project_id
            """, (project_key, project_name))
            
        conn.commit()
        
        # Get project IDs
        cursor.execute("SELECT project_id, sonarqube_project_key FROM sonarqube_metrics.sq_projects")
        project_id_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Group metrics by project for carry-forward logic
        metrics_by_project = {}
        for metric in raw_metrics:
            project_key = metric['project_key']
            if project_key not in metrics_by_project:
                metrics_by_project[project_key] = []
            metrics_by_project[project_key].append(metric)
        
        # Process each project's metrics
        for project_key, project_metrics in metrics_by_project.items():
            project_id = project_id_map[project_key]
            
            # Sort by date
            project_metrics.sort(key=lambda x: x['metric_date'])
            
            # Get last known values for carry-forward
            cursor.execute("""
                SELECT * FROM sonarqube_metrics.daily_project_metrics
                WHERE project_id = %s
                ORDER BY metric_date DESC
                LIMIT 1
            """, (project_id,))
            
            last_known = cursor.fetchone()
            last_known_dict = {}
            if last_known:
                columns = [desc[0] for desc in cursor.description]
                last_known_dict = dict(zip(columns, last_known))
            
            for metric_data in project_metrics:
                if metric_data.get('error') or not metric_data.get('metrics'):
                    # Use carry-forward logic
                    if last_known_dict:
                        insert_carried_forward_metric(cursor, project_id, metric_data['metric_date'], last_known_dict)
                else:
                    # Transform and insert actual metrics
                    transformed = transform_metric_data(metric_data)
                    insert_metric(cursor, project_id, transformed)
                    # Update last known values
                    last_known_dict = transformed
            
        conn.commit()
        logging.info("Successfully loaded all metrics to database")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading metrics to database: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def transform_metric_data(metric_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw metric data into database format.
    
    Converts the raw metric data from SonarQube API format into the flat
    structure required by the database schema. Handles missing values and
    type conversions.
    
    Args:
        metric_data (Dict[str, Any]): Raw metric data containing:
            - metrics: Core metric values
            - issues_breakdown: Issue counts by type/severity/status
            - new_code_issues_breakdown: New code issue counts
            - metric_date: Date of the metrics
            
    Returns:
        Dict[str, Any]: Transformed data matching database schema with all
                       required fields populated (using defaults for missing values)
                       
    Note:
        - Converts string values to appropriate numeric types
        - Handles missing values with sensible defaults (0 for counts)
        - Parses new code period date if available
        - Sets is_carried_forward to False for actual data
        
    Example:
        >>> raw_data = {
        ...     'metric_date': '2025-01-15',
        ...     'metrics': {'bugs': '5', 'coverage': '85.3'},
        ...     'issues_breakdown': {'bug_blocker': 1},
        ...     'new_code_issues_breakdown': {}
        ... }
        >>> transformed = transform_metric_data(raw_data)
        >>> print(transformed['bugs_total'])  # Returns integer
        5
    """
    metrics = metric_data['metrics']
    issues = metric_data['issues_breakdown']
    new_code_issues = metric_data.get('new_code_issues_breakdown', {})
    
    # Parse new code period date if available
    new_code_period_date = None
    if 'new_code_period_date' in metrics and metrics['new_code_period_date']:
        try:
            new_code_period_date = parser.parse(metrics['new_code_period_date']).date()
        except:
            new_code_period_date = None
    
    return {
        'metric_date': metric_data['metric_date'],
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
        'data_source_timestamp': datetime.now(),
        'is_carried_forward': False,
        # New code metrics
        'new_code_bugs_total': int(metrics.get('new_bugs', 0)),
        'new_code_bugs_blocker': new_code_issues.get('new_code_bug_blocker', 0),
        'new_code_bugs_critical': new_code_issues.get('new_code_bug_critical', 0),
        'new_code_bugs_major': new_code_issues.get('new_code_bug_major', 0),
        'new_code_bugs_minor': new_code_issues.get('new_code_bug_minor', 0),
        'new_code_bugs_info': new_code_issues.get('new_code_bug_info', 0),
        'new_code_vulnerabilities_total': int(metrics.get('new_vulnerabilities', 0)),
        'new_code_vulnerabilities_critical': new_code_issues.get('new_code_vulnerability_critical', 0),
        'new_code_vulnerabilities_high': new_code_issues.get('new_code_vulnerability_major', 0),
        'new_code_vulnerabilities_medium': new_code_issues.get('new_code_vulnerability_minor', 0),
        'new_code_vulnerabilities_low': new_code_issues.get('new_code_vulnerability_info', 0),
        'new_code_code_smells_total': int(metrics.get('new_code_smells', 0)),
        'new_code_code_smells_blocker': new_code_issues.get('new_code_code_smell_blocker', 0),
        'new_code_code_smells_critical': new_code_issues.get('new_code_code_smell_critical', 0),
        'new_code_code_smells_major': new_code_issues.get('new_code_code_smell_major', 0),
        'new_code_code_smells_minor': new_code_issues.get('new_code_code_smell_minor', 0),
        'new_code_code_smells_info': new_code_issues.get('new_code_code_smell_info', 0),
        'new_code_security_hotspots_total': int(metrics.get('new_security_hotspots', 0)),
        'new_code_security_hotspots_high': new_code_issues.get('new_code_security_hotspot_high', 0),
        'new_code_security_hotspots_medium': new_code_issues.get('new_code_security_hotspot_medium', 0),
        'new_code_security_hotspots_low': new_code_issues.get('new_code_security_hotspot_low', 0),
        'new_code_security_hotspots_to_review': new_code_issues.get('new_code_security_hotspot_to_review', 0),
        'new_code_security_hotspots_reviewed': new_code_issues.get('new_code_security_hotspot_reviewed', 0),
        'new_code_coverage_percentage': float(metrics.get('new_coverage', 0)),
        'new_code_duplicated_lines_density': float(metrics.get('new_duplicated_lines_density', 0)),
        'new_code_lines': int(metrics.get('new_lines', 0)),
        'new_code_period_date': new_code_period_date
    }

def insert_metric(cursor, project_id: int, metric_data: Dict[str, Any]) -> None:
    """Insert metric data into database.
    
    Executes an upsert operation to insert new metric data or update existing
    records. Uses PostgreSQL's ON CONFLICT clause for idempotent operations.
    
    Args:
        cursor: PostgreSQL database cursor
        project_id (int): Database ID of the project
        metric_data (Dict[str, Any]): Transformed metric data from transform_metric_data()
                                     containing all required database fields
                                     
    Note:
        - Uses parameterized queries to prevent SQL injection
        - Implements full upsert (INSERT ... ON CONFLICT DO UPDATE)
        - Updates all fields on conflict to ensure data consistency
        
    Example:
        >>> conn = pg_hook.get_conn()
        >>> cursor = conn.cursor()
        >>> transformed_data = transform_metric_data(raw_metrics)
        >>> insert_metric(cursor, project_id=1, metric_data=transformed_data)
        >>> conn.commit()
    """
    insert_query = """
        INSERT INTO sonarqube_metrics.daily_project_metrics (
            project_id, metric_date,
            bugs_total, bugs_blocker, bugs_critical, bugs_major, bugs_minor, bugs_info,
            bugs_open, bugs_confirmed, bugs_reopened, bugs_resolved, bugs_closed,
            vulnerabilities_total, vulnerabilities_critical, vulnerabilities_high,
            vulnerabilities_medium, vulnerabilities_low,
            vulnerabilities_open, vulnerabilities_confirmed, vulnerabilities_reopened,
            vulnerabilities_resolved, vulnerabilities_closed,
            code_smells_total, code_smells_blocker, code_smells_critical,
            code_smells_major, code_smells_minor, code_smells_info,
            security_hotspots_total, security_hotspots_high, security_hotspots_medium,
            security_hotspots_low, security_hotspots_to_review, security_hotspots_reviewed,
            security_hotspots_acknowledged, security_hotspots_fixed,
            coverage_percentage, duplicated_lines_density,
            data_source_timestamp, is_carried_forward,
            new_code_bugs_total, new_code_bugs_blocker, new_code_bugs_critical,
            new_code_bugs_major, new_code_bugs_minor, new_code_bugs_info,
            new_code_vulnerabilities_total, new_code_vulnerabilities_critical,
            new_code_vulnerabilities_high, new_code_vulnerabilities_medium,
            new_code_vulnerabilities_low, new_code_code_smells_total,
            new_code_code_smells_blocker, new_code_code_smells_critical,
            new_code_code_smells_major, new_code_code_smells_minor,
            new_code_code_smells_info, new_code_security_hotspots_total,
            new_code_security_hotspots_high, new_code_security_hotspots_medium,
            new_code_security_hotspots_low, new_code_security_hotspots_to_review,
            new_code_security_hotspots_reviewed, new_code_coverage_percentage,
            new_code_duplicated_lines_density, new_code_lines, new_code_period_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (project_id, metric_date) DO UPDATE SET
            bugs_total = EXCLUDED.bugs_total,
            bugs_blocker = EXCLUDED.bugs_blocker,
            bugs_critical = EXCLUDED.bugs_critical,
            bugs_major = EXCLUDED.bugs_major,
            bugs_minor = EXCLUDED.bugs_minor,
            bugs_info = EXCLUDED.bugs_info,
            bugs_open = EXCLUDED.bugs_open,
            bugs_confirmed = EXCLUDED.bugs_confirmed,
            bugs_reopened = EXCLUDED.bugs_reopened,
            bugs_resolved = EXCLUDED.bugs_resolved,
            bugs_closed = EXCLUDED.bugs_closed,
            vulnerabilities_total = EXCLUDED.vulnerabilities_total,
            vulnerabilities_critical = EXCLUDED.vulnerabilities_critical,
            vulnerabilities_high = EXCLUDED.vulnerabilities_high,
            vulnerabilities_medium = EXCLUDED.vulnerabilities_medium,
            vulnerabilities_low = EXCLUDED.vulnerabilities_low,
            vulnerabilities_open = EXCLUDED.vulnerabilities_open,
            vulnerabilities_confirmed = EXCLUDED.vulnerabilities_confirmed,
            vulnerabilities_reopened = EXCLUDED.vulnerabilities_reopened,
            vulnerabilities_resolved = EXCLUDED.vulnerabilities_resolved,
            vulnerabilities_closed = EXCLUDED.vulnerabilities_closed,
            code_smells_total = EXCLUDED.code_smells_total,
            code_smells_blocker = EXCLUDED.code_smells_blocker,
            code_smells_critical = EXCLUDED.code_smells_critical,
            code_smells_major = EXCLUDED.code_smells_major,
            code_smells_minor = EXCLUDED.code_smells_minor,
            code_smells_info = EXCLUDED.code_smells_info,
            security_hotspots_total = EXCLUDED.security_hotspots_total,
            security_hotspots_high = EXCLUDED.security_hotspots_high,
            security_hotspots_medium = EXCLUDED.security_hotspots_medium,
            security_hotspots_low = EXCLUDED.security_hotspots_low,
            security_hotspots_to_review = EXCLUDED.security_hotspots_to_review,
            security_hotspots_reviewed = EXCLUDED.security_hotspots_reviewed,
            security_hotspots_acknowledged = EXCLUDED.security_hotspots_acknowledged,
            security_hotspots_fixed = EXCLUDED.security_hotspots_fixed,
            coverage_percentage = EXCLUDED.coverage_percentage,
            duplicated_lines_density = EXCLUDED.duplicated_lines_density,
            data_source_timestamp = EXCLUDED.data_source_timestamp,
            is_carried_forward = EXCLUDED.is_carried_forward,
            new_code_bugs_total = EXCLUDED.new_code_bugs_total,
            new_code_bugs_blocker = EXCLUDED.new_code_bugs_blocker,
            new_code_bugs_critical = EXCLUDED.new_code_bugs_critical,
            new_code_bugs_major = EXCLUDED.new_code_bugs_major,
            new_code_bugs_minor = EXCLUDED.new_code_bugs_minor,
            new_code_bugs_info = EXCLUDED.new_code_bugs_info,
            new_code_vulnerabilities_total = EXCLUDED.new_code_vulnerabilities_total,
            new_code_vulnerabilities_critical = EXCLUDED.new_code_vulnerabilities_critical,
            new_code_vulnerabilities_high = EXCLUDED.new_code_vulnerabilities_high,
            new_code_vulnerabilities_medium = EXCLUDED.new_code_vulnerabilities_medium,
            new_code_vulnerabilities_low = EXCLUDED.new_code_vulnerabilities_low,
            new_code_code_smells_total = EXCLUDED.new_code_code_smells_total,
            new_code_code_smells_blocker = EXCLUDED.new_code_code_smells_blocker,
            new_code_code_smells_critical = EXCLUDED.new_code_code_smells_critical,
            new_code_code_smells_major = EXCLUDED.new_code_code_smells_major,
            new_code_code_smells_minor = EXCLUDED.new_code_code_smells_minor,
            new_code_code_smells_info = EXCLUDED.new_code_code_smells_info,
            new_code_security_hotspots_total = EXCLUDED.new_code_security_hotspots_total,
            new_code_security_hotspots_high = EXCLUDED.new_code_security_hotspots_high,
            new_code_security_hotspots_medium = EXCLUDED.new_code_security_hotspots_medium,
            new_code_security_hotspots_low = EXCLUDED.new_code_security_hotspots_low,
            new_code_security_hotspots_to_review = EXCLUDED.new_code_security_hotspots_to_review,
            new_code_security_hotspots_reviewed = EXCLUDED.new_code_security_hotspots_reviewed,
            new_code_coverage_percentage = EXCLUDED.new_code_coverage_percentage,
            new_code_duplicated_lines_density = EXCLUDED.new_code_duplicated_lines_density,
            new_code_lines = EXCLUDED.new_code_lines,
            new_code_period_date = EXCLUDED.new_code_period_date
    """
    
    values = (
        project_id, metric_data['metric_date'],
        metric_data['bugs_total'], metric_data['bugs_blocker'], metric_data['bugs_critical'],
        metric_data['bugs_major'], metric_data['bugs_minor'], metric_data['bugs_info'],
        metric_data['bugs_open'], metric_data['bugs_confirmed'], metric_data['bugs_reopened'],
        metric_data['bugs_resolved'], metric_data['bugs_closed'],
        metric_data['vulnerabilities_total'], metric_data['vulnerabilities_critical'],
        metric_data['vulnerabilities_high'], metric_data['vulnerabilities_medium'],
        metric_data['vulnerabilities_low'], metric_data['vulnerabilities_open'],
        metric_data['vulnerabilities_confirmed'], metric_data['vulnerabilities_reopened'],
        metric_data['vulnerabilities_resolved'], metric_data['vulnerabilities_closed'],
        metric_data['code_smells_total'], metric_data['code_smells_blocker'],
        metric_data['code_smells_critical'], metric_data['code_smells_major'],
        metric_data['code_smells_minor'], metric_data['code_smells_info'],
        metric_data['security_hotspots_total'], metric_data['security_hotspots_high'],
        metric_data['security_hotspots_medium'], metric_data['security_hotspots_low'],
        metric_data['security_hotspots_to_review'], metric_data['security_hotspots_reviewed'],
        metric_data['security_hotspots_acknowledged'], metric_data['security_hotspots_fixed'],
        metric_data['coverage_percentage'], metric_data['duplicated_lines_density'],
        metric_data['data_source_timestamp'], metric_data['is_carried_forward'],
        # New code metrics values
        metric_data['new_code_bugs_total'], metric_data['new_code_bugs_blocker'],
        metric_data['new_code_bugs_critical'], metric_data['new_code_bugs_major'],
        metric_data['new_code_bugs_minor'], metric_data['new_code_bugs_info'],
        metric_data['new_code_vulnerabilities_total'], metric_data['new_code_vulnerabilities_critical'],
        metric_data['new_code_vulnerabilities_high'], metric_data['new_code_vulnerabilities_medium'],
        metric_data['new_code_vulnerabilities_low'], metric_data['new_code_code_smells_total'],
        metric_data['new_code_code_smells_blocker'], metric_data['new_code_code_smells_critical'],
        metric_data['new_code_code_smells_major'], metric_data['new_code_code_smells_minor'],
        metric_data['new_code_code_smells_info'], metric_data['new_code_security_hotspots_total'],
        metric_data['new_code_security_hotspots_high'], metric_data['new_code_security_hotspots_medium'],
        metric_data['new_code_security_hotspots_low'], metric_data['new_code_security_hotspots_to_review'],
        metric_data['new_code_security_hotspots_reviewed'], metric_data['new_code_coverage_percentage'],
        metric_data['new_code_duplicated_lines_density'], metric_data['new_code_lines'],
        metric_data['new_code_period_date']
    )
    
    cursor.execute(insert_query, values)

def insert_carried_forward_metric(cursor, project_id: int, metric_date: str, 
                                 last_known: Dict[str, Any]) -> None:
    """Insert carried forward metric when current data is not available.
    
    Creates a new metric record by copying the last known values with updated
    metadata to indicate the data was carried forward.
    
    Args:
        cursor: PostgreSQL database cursor
        project_id (int): Database ID of the project
        metric_date (str): Date for the carried forward record (YYYY-MM-DD)
        last_known (Dict[str, Any]): Dictionary containing the last known metric
                                    values to be carried forward
                                    
    Note:
        - Sets is_carried_forward flag to True
        - Updates data_source_timestamp to current time
        - Preserves all metric values from last known state
        
    Example:
        >>> # When today's data is unavailable
        >>> if not current_metrics:
        ...     insert_carried_forward_metric(
        ...         cursor, 
        ...         project_id=1,
        ...         metric_date='2025-01-16',
        ...         last_known=yesterday_metrics
        ...     )
    """
    # Copy last known values but update date and carry-forward flag
    metric_data = last_known.copy()
    metric_data['metric_date'] = metric_date
    metric_data['is_carried_forward'] = True
    metric_data['data_source_timestamp'] = datetime.now()
    
    insert_metric(cursor, project_id, metric_data)

# Define tasks
fetch_projects_task = PythonOperator(
    task_id='fetch_projects',
    python_callable=fetch_projects,
    dag=dag
)

extract_metrics_task = PythonOperator(
    task_id='extract_metrics',
    python_callable=extract_metrics_for_project,
    dag=dag
)

transform_load_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load_metrics,
    dag=dag
)

# Set task dependencies
fetch_projects_task >> extract_metrics_task >> transform_load_task