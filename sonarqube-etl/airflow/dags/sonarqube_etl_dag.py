from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import requests
import logging
import os
from typing import Dict, List, Any
import json
from dateutil import parser

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

def get_sonarqube_config():
    """Get SonarQube configuration from Airflow Variables or Environment"""
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

def fetch_projects(**context):
    """Fetch all projects from SonarQube"""
    config = get_sonarqube_config()
    
    # Debug logging
    token = config.get('token', 'NO_TOKEN_FOUND')
    logging.info(f"Using SonarQube token: {token[:10]}...{token[-4:] if len(token) > 14 else token}")
    logging.info(f"Using SonarQube URL: {config['base_url']}")
    
    projects = []
    page = 1
    page_size = 100
    
    while True:
        response = requests.get(
            f"{config['base_url']}/api/projects/search",
            params={'p': page, 'ps': page_size},
            auth=config['auth']
        )
        response.raise_for_status()
        
        data = response.json()
        projects.extend(data['components'])
        
        if len(projects) >= data['paging']['total']:
            break
        page += 1
    
    logging.info(f"Found {len(projects)} projects in SonarQube")
    context['task_instance'].xcom_push(key='projects', value=projects)
    return projects

def fetch_project_metrics(project_key: str, metric_date: str, config: Dict, use_history: bool = False) -> Dict:
    """Fetch metrics for a specific project, optionally from historical data"""
    # Overall code metrics
    metrics_to_fetch = [
        'bugs', 'vulnerabilities', 'code_smells', 'security_hotspots',
        'coverage', 'duplicated_lines_density', 'reliability_rating',
        'security_rating', 'sqale_rating'
    ]
    
    # New code metrics
    new_code_metrics = [
        'new_bugs', 'new_vulnerabilities', 'new_code_smells', 'new_security_hotspots',
        'new_coverage', 'new_duplicated_lines_density', 'new_lines'
    ]
    
    metrics = {}
    
    if use_history:
        # Fetch historical metrics using search_history API
        all_metrics = metrics_to_fetch + new_code_metrics
        for metric in all_metrics:
            try:
                response = requests.get(
                    f"{config['base_url']}/api/measures/search_history",
                    params={
                        'component': project_key,
                        'metrics': metric,
                        'from': metric_date,
                        'to': metric_date,
                        'ps': 1000
                    },
                    auth=config['auth']
                )
                response.raise_for_status()
                
                history_data = response.json()
                if history_data.get('measures') and len(history_data['measures']) > 0:
                    measure = history_data['measures'][0]
                    if measure.get('history') and len(measure['history']) > 0:
                        # Find the value for the specific date
                        for hist_point in measure['history']:
                            if hist_point.get('date', '').startswith(metric_date):
                                metrics[metric] = hist_point.get('value', 0)
                                break
                        if metric not in metrics:
                            # If no exact date match, use the closest value
                            metrics[metric] = measure['history'][-1].get('value', 0)
            except Exception as e:
                logging.warning(f"Failed to fetch historical data for metric {metric}: {str(e)}")
                metrics[metric] = 0
    else:
        # For non-historical mode, determine the best approach based on the date
        all_metrics = metrics_to_fetch + new_code_metrics
        current_date = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # For today or yesterday, use current values (they're most up-to-date)
        if metric_date >= yesterday:
            logging.info(f"Fetching current metrics for {project_key} on {metric_date}")
            response = requests.get(
                f"{config['base_url']}/api/measures/component",
                params={
                    'component': project_key,
                    'metricKeys': ','.join(all_metrics)
                },
                auth=config['auth']
            )
            response.raise_for_status()
            
            measures = response.json().get('component', {}).get('measures', [])
            metrics = {m['metric']: m.get('value', 0) for m in measures}
            
            # Log the fetched metrics for debugging
            logging.info(f"Fetched metrics for {project_key}: {metrics}")
            
            # Get new code period information
            component_data = response.json().get('component', {})
            if 'period' in component_data:
                period = component_data['period']
                metrics['new_code_period_date'] = period.get('date', None)
                metrics['new_code_period_mode'] = period.get('mode', 'days')
                metrics['new_code_period_value'] = period.get('value', '30')
        else:
            # For older dates, try to get historical data
            logging.info(f"Fetching historical metrics for {project_key} on {metric_date}")
            for metric in all_metrics:
                try:
                    response = requests.get(
                        f"{config['base_url']}/api/measures/search_history",
                        params={
                            'component': project_key,
                            'metrics': metric,
                            'from': metric_date,
                            'to': metric_date,
                            'ps': 1
                        },
                        auth=config['auth']
                    )
                    response.raise_for_status()
                    
                    history_data = response.json()
                    if history_data.get('measures') and len(history_data['measures']) > 0:
                        measure = history_data['measures'][0]
                        if measure.get('history') and len(measure['history']) > 0:
                            # Find the value for the specific date
                            for hist_point in measure['history']:
                                if hist_point.get('date', '').startswith(metric_date):
                                    metrics[metric] = hist_point.get('value', 0)
                                    break
                except Exception as e:
                    logging.warning(f"Failed to fetch historical data for metric {metric}: {str(e)}")
            
            # If no historical data found for past dates, return empty metrics (will trigger carry-forward)
            if not metrics:
                logging.info(f"No historical data found for {project_key} on {metric_date}, will use carry-forward")
                return {
                    'project_key': project_key,
                    'metric_date': metric_date,
                    'metrics': None,
                    'issues_breakdown': {},
                    'new_code_issues_breakdown': {}
                }
    
    # Fetch issues breakdown
    issue_types = ['BUG', 'VULNERABILITY', 'CODE_SMELL']
    severities = ['BLOCKER', 'CRITICAL', 'MAJOR', 'MINOR', 'INFO']
    statuses = ['OPEN', 'CONFIRMED', 'REOPENED', 'RESOLVED', 'CLOSED']
    
    issues_breakdown = {}
    
    for issue_type in issue_types:
        for severity in severities:
            response = requests.get(
                f"{config['base_url']}/api/issues/search",
                params={
                    'componentKeys': project_key,
                    'types': issue_type,
                    'severities': severity,
                    'resolved': 'false',
                    'ps': 1
                },
                auth=config['auth']
            )
            response.raise_for_status()
            key = f"{issue_type}_{severity}".lower()
            issues_breakdown[key] = response.json()['total']
        
        for status in statuses:
            response = requests.get(
                f"{config['base_url']}/api/issues/search",
                params={
                    'componentKeys': project_key,
                    'types': issue_type,
                    'statuses': status,
                    'ps': 1
                },
                auth=config['auth']
            )
            response.raise_for_status()
            key = f"{issue_type}_{status}".lower()
            issues_breakdown[key] = response.json()['total']
    
    # Fetch security hotspots breakdown
    hotspot_statuses = ['TO_REVIEW', 'REVIEWED']  # Valid statuses for SonarQube hotspots API
    hotspot_severities = ['HIGH', 'MEDIUM', 'LOW']
    
    for severity in hotspot_severities:
        response = requests.get(
            f"{config['base_url']}/api/hotspots/search",
            params={
                'projectKey': project_key,
                'securityCategory': severity,
                'ps': 1
            },
            auth=config['auth']
        )
        response.raise_for_status()
        key = f"security_hotspot_{severity}".lower()
        issues_breakdown[key] = response.json()['paging']['total']
    
    for status in hotspot_statuses:
        response = requests.get(
            f"{config['base_url']}/api/hotspots/search",
            params={
                'projectKey': project_key,
                'status': status,
                'ps': 1
            },
            auth=config['auth']
        )
        response.raise_for_status()
        key = f"security_hotspot_{status}".lower()
        issues_breakdown[key] = response.json()['paging']['total']
    
    # Fetch new code issues breakdown
    new_code_issues_breakdown = {}
    
    # Check if we have a new code period
    if not use_history or metrics.get('new_lines', 0) > 0:
        for issue_type in issue_types:
            for severity in severities:
                try:
                    response = requests.get(
                        f"{config['base_url']}/api/issues/search",
                        params={
                            'componentKeys': project_key,
                            'types': issue_type,
                            'severities': severity,
                            'resolved': 'false',
                            'inNewCodePeriod': 'true',
                            'ps': 1
                        },
                        auth=config['auth']
                    )
                    response.raise_for_status()
                    key = f"new_code_{issue_type}_{severity}".lower()
                    new_code_issues_breakdown[key] = response.json()['total']
                except Exception as e:
                    logging.warning(f"Failed to fetch new code issues for {issue_type}/{severity}: {str(e)}")
                    new_code_issues_breakdown[f"new_code_{issue_type}_{severity}".lower()] = 0
        
        # Fetch new code security hotspots
        for severity in hotspot_severities:
            try:
                response = requests.get(
                    f"{config['base_url']}/api/hotspots/search",
                    params={
                        'projectKey': project_key,
                        'securityCategory': severity,
                        'inNewCodePeriod': 'true',
                        'ps': 1
                    },
                    auth=config['auth']
                )
                response.raise_for_status()
                key = f"new_code_security_hotspot_{severity}".lower()
                new_code_issues_breakdown[key] = response.json()['paging']['total']
            except Exception as e:
                logging.warning(f"Failed to fetch new code hotspots for {severity}: {str(e)}")
                new_code_issues_breakdown[f"new_code_security_hotspot_{severity}".lower()] = 0
    
    return {
        'project_key': project_key,
        'metric_date': metric_date,
        'metrics': metrics,
        'issues_breakdown': issues_breakdown,
        'new_code_issues_breakdown': new_code_issues_breakdown
    }

def extract_metrics_for_project(**context):
    """Extract metrics for all projects for the previous day"""
    projects = context['task_instance'].xcom_pull(task_ids='fetch_projects', key='projects')
    config = get_sonarqube_config()
    
    # Always extract yesterday's data for daily runs
    execution_date = context['execution_date']
    metric_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logging.info(f"Extracting metrics for date: {metric_date}")
    
    all_metrics = []
    
    for project in projects:
        project_key = project['key']
        logging.info(f"Extracting metrics for project: {project_key}")
        
        try:
            # Use the standard fetch with historical API to ensure we get the right data
            metrics_data = fetch_project_metrics(project_key, metric_date, config, use_history=False)
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

def transform_and_load_metrics(**context):
    """Transform and load metrics into PostgreSQL"""
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

def transform_metric_data(metric_data: Dict) -> Dict:
    """Transform raw metric data into database format"""
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

def insert_metric(cursor, project_id: int, metric_data: Dict):
    """Insert metric data into database"""
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

def insert_carried_forward_metric(cursor, project_id: int, metric_date: str, last_known: Dict):
    """Insert carried forward metric when current data is not available"""
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