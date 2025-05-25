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
    catchup=False,
    max_active_runs=1,
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

def fetch_project_metrics(project_key: str, metric_date: str, config: Dict) -> Dict:
    """Fetch metrics for a specific project"""
    metrics_to_fetch = [
        'bugs', 'vulnerabilities', 'code_smells', 'security_hotspots',
        'coverage', 'duplicated_lines_density', 'reliability_rating',
        'security_rating', 'sqale_rating'
    ]
    
    # Fetch general metrics
    response = requests.get(
        f"{config['base_url']}/api/measures/component",
        params={
            'component': project_key,
            'metricKeys': ','.join(metrics_to_fetch)
        },
        auth=config['auth']
    )
    response.raise_for_status()
    
    measures = response.json().get('component', {}).get('measures', [])
    metrics = {m['metric']: m.get('value', 0) for m in measures}
    
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
    
    return {
        'project_key': project_key,
        'metric_date': metric_date,
        'metrics': metrics,
        'issues_breakdown': issues_breakdown
    }

def extract_metrics_for_project(**context):
    """Extract metrics for all projects"""
    projects = context['task_instance'].xcom_pull(task_ids='fetch_projects', key='projects')
    config = get_sonarqube_config()
    
    # Determine date range for extraction
    execution_date = context['execution_date']
    is_backfill = context['dag_run'].conf.get('backfill', False)
    
    if is_backfill:
        # Extract last 3 months of data
        end_date = execution_date
        start_date = end_date - timedelta(days=90)
        dates_to_extract = []
        current_date = start_date
        while current_date <= end_date:
            dates_to_extract.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
    else:
        # Extract only yesterday's data
        dates_to_extract = [(execution_date - timedelta(days=1)).strftime('%Y-%m-%d')]
    
    all_metrics = []
    
    for project in projects:
        project_key = project['key']
        logging.info(f"Extracting metrics for project: {project_key}")
        
        for metric_date in dates_to_extract:
            try:
                metrics_data = fetch_project_metrics(project_key, metric_date, config)
                all_metrics.append(metrics_data)
            except Exception as e:
                logging.error(f"Failed to fetch metrics for {project_key} on {metric_date}: {str(e)}")
                # For missing data, we'll handle carry-forward logic in the transform step
                all_metrics.append({
                    'project_key': project_key,
                    'metric_date': metric_date,
                    'metrics': None,
                    'issues_breakdown': None,
                    'error': str(e)
                })
    
    context['task_instance'].xcom_push(key='raw_metrics', value=all_metrics)
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
        'is_carried_forward': False
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
            data_source_timestamp, is_carried_forward
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
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
            is_carried_forward = EXCLUDED.is_carried_forward
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
        metric_data['data_source_timestamp'], metric_data['is_carried_forward']
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