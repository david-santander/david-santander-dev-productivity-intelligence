"""SonarQube ETL DAG v2 - Enhanced with security review and quality gate metrics.

This updated version includes:
- Security review metrics (security_hotspots_reviewed, security_review_rating)
- Quality gate status tracking
- Corrected metric key mappings
- Additional issue state metrics
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

# Import the enhanced SonarQube client
from sonarqube_client_v2 import SonarQubeClient

# Define dataset for data-aware scheduling
SONARQUBE_METRICS_DATASET = Dataset("postgres://sonarqube_metrics_db/daily_project_metrics")

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
    'sonarqube_etl_v2',
    default_args=default_args,
    description='Enhanced ETL process with security review and quality gate metrics',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ## SonarQube ETL DAG v2
    
    Enhanced version that extracts additional metrics from SonarQube:
    - Security review metrics and ratings
    - Quality gate status
    - Issue acceptance status
    - Corrected technical debt metrics
    
    ### Features
    - Extracts both overall and new code metrics
    - Tracks security hotspot review status
    - Monitors quality gate conditions
    - Supports all official SonarQube metrics
    """
)

def get_sonarqube_config() -> Dict[str, Any]:
    """Get SonarQube configuration from Airflow Variables or Environment."""
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

def fetch_projects(**context) -> List[Dict[str, Any]]:
    """Fetch all projects from SonarQube."""
    config = get_sonarqube_config()
    client = SonarQubeClient(config)
    
    projects = client.fetch_all_projects()
    
    context['task_instance'].xcom_push(key='projects', value=projects)
    return projects

def extract_metrics_for_project(**context) -> int:
    """Extract metrics for all projects for the previous day."""
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

def convert_numeric_rating_to_letter(rating_value):
    """Convert numeric rating to letter grade.
    SonarQube API returns: 1.0=A, 2.0=B, 3.0=C, 4.0=D, 5.0=E
    """
    if rating_value is None:
        return None
    
    try:
        rating_float = float(rating_value)
        if rating_float <= 1.0:
            return 'A'
        elif rating_float <= 2.0:
            return 'B'
        elif rating_float <= 3.0:
            return 'C'
        elif rating_float <= 4.0:
            return 'D'
        else:
            return 'E'
    except (ValueError, TypeError):
        # If it's already a string letter, return as is
        if isinstance(rating_value, str) and rating_value in ['A', 'B', 'C', 'D', 'E']:
            return rating_value
        return None

def transform_metric_data(metric_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw metric data into database format with corrected mappings."""
    metrics = metric_data['metrics']
    issues = metric_data['issues_breakdown']
    new_code_issues = metric_data.get('new_code_issues_breakdown', {})
    
    # Debug logging for new code issues
    if new_code_issues:
        logging.info(f"New code issues breakdown keys: {list(new_code_issues.keys())}")
        code_smell_keys = [k for k in new_code_issues.keys() if 'smell' in k.lower()]
        if code_smell_keys:
            logging.info(f"Code smell related keys: {code_smell_keys}")
            for key in code_smell_keys:
                logging.info(f"  {key}: {new_code_issues[key]}")
    
    # Parse new code period date if available
    new_code_period_date = None
    if 'new_code_period_date' in metrics and metrics['new_code_period_date']:
        try:
            new_code_period_date = parser.parse(metrics['new_code_period_date']).date()
        except:
            new_code_period_date = None
    
    # Calculate totals from breakdowns for verification
    bugs_breakdown_total = sum([
        issues.get('bug_blocker', 0),
        issues.get('bug_critical', 0),
        issues.get('bug_major', 0),
        issues.get('bug_minor', 0),
        issues.get('bug_info', 0)
    ])
    
    vulnerabilities_breakdown_total = sum([
        issues.get('vulnerability_blocker', 0),
        issues.get('vulnerability_critical', 0),
        issues.get('vulnerability_major', 0),
        issues.get('vulnerability_minor', 0),
        issues.get('vulnerability_info', 0)
    ])
    
    code_smells_breakdown_total = sum([
        issues.get('code_smell_blocker', 0),
        issues.get('code_smell_critical', 0),
        issues.get('code_smell_major', 0),
        issues.get('code_smell_minor', 0),
        issues.get('code_smell_info', 0)
    ])
    
    # Use calculated totals if they match or exceed the metric totals
    bugs_total = max(int(metrics.get('bugs', 0)), bugs_breakdown_total)
    vulnerabilities_total = max(int(metrics.get('vulnerabilities', 0)), vulnerabilities_breakdown_total)
    code_smells_total = max(int(metrics.get('code_smells', 0)), code_smells_breakdown_total)
    
    # For security hotspots, ensure total is at least as high as sum of all statuses
    security_hotspots_api_total = int(metrics.get('security_hotspots', 0))
    security_hotspots_status_total = sum([
        issues.get('security_hotspot_to_review', 0),
        issues.get('security_hotspot_acknowledged', 0),
        issues.get('security_hotspot_fixed', 0),
        issues.get('security_hotspot_safe', 0)
    ])
    security_hotspots_total = max(security_hotspots_api_total, security_hotspots_status_total)
    
    return {
        'metric_date': metric_data['metric_date'],
        
        # Size metrics
        'lines': int(metrics.get('lines', 0)),
        'ncloc': int(metrics.get('ncloc', 0)),
        'classes': int(metrics.get('classes', 0)),
        'functions': int(metrics.get('functions', 0)),
        'statements': int(metrics.get('statements', 0)),
        'files': int(metrics.get('files', 0)),
        'directories': int(metrics.get('directories', 0)),
        
        # Bug metrics
        'bugs_total': bugs_total,
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
        'bugs_false_positive': issues.get('bug_false-positive', 0),
        'bugs_wontfix': issues.get('bug_wontfix', 0),
        
        # Vulnerability metrics
        'vulnerabilities_total': vulnerabilities_total,
        'vulnerabilities_blocker': issues.get('vulnerability_blocker', 0),
        'vulnerabilities_critical': issues.get('vulnerability_critical', 0),
        'vulnerabilities_high': issues.get('vulnerability_major', 0),
        'vulnerabilities_medium': issues.get('vulnerability_minor', 0),
        'vulnerabilities_low': issues.get('vulnerability_info', 0),
        'vulnerabilities_open': issues.get('vulnerability_open', 0),
        'vulnerabilities_confirmed': issues.get('vulnerability_confirmed', 0),
        'vulnerabilities_reopened': issues.get('vulnerability_reopened', 0),
        'vulnerabilities_resolved': issues.get('vulnerability_resolved', 0),
        'vulnerabilities_closed': issues.get('vulnerability_closed', 0),
        'vulnerabilities_false_positive': issues.get('vulnerability_false-positive', 0),
        'vulnerabilities_wontfix': issues.get('vulnerability_wontfix', 0),
        
        # Code smell metrics
        'code_smells_total': code_smells_total,
        'code_smells_blocker': issues.get('code_smell_blocker', 0),
        'code_smells_critical': issues.get('code_smell_critical', 0),
        'code_smells_major': issues.get('code_smell_major', 0),
        'code_smells_minor': issues.get('code_smell_minor', 0),
        'code_smells_info': issues.get('code_smell_info', 0),
        'code_smells_open': issues.get('code_smell_open', 0),
        'code_smells_confirmed': issues.get('code_smell_confirmed', 0),
        'code_smells_reopened': issues.get('code_smell_reopened', 0),
        'code_smells_resolved': issues.get('code_smell_resolved', 0),
        'code_smells_closed': issues.get('code_smell_closed', 0),
        'code_smells_false_positive': issues.get('code_smell_false-positive', 0),
        'code_smells_wontfix': issues.get('code_smell_wontfix', 0),
        
        # Security hotspot metrics
        'security_hotspots_total': security_hotspots_total,
        'security_hotspots_high': issues.get('security_hotspot_high', 0),
        'security_hotspots_medium': issues.get('security_hotspot_medium', 0),
        'security_hotspots_low': issues.get('security_hotspot_low', 0),
        'security_hotspots_to_review': issues.get('security_hotspot_to_review', 0),
        'security_hotspots_acknowledged': issues.get('security_hotspot_acknowledged', 0),
        'security_hotspots_fixed': issues.get('security_hotspot_fixed', 0),
        'security_hotspots_safe': issues.get('security_hotspot_safe', 0),
        
        # Security review metrics
        'security_hotspots_reviewed': float(metrics.get('security_hotspots_reviewed', 0)),
        'security_review_rating': convert_numeric_rating_to_letter(metrics.get('security_review_rating')),
        
        # Quality ratings
        'reliability_rating': convert_numeric_rating_to_letter(metrics.get('reliability_rating')),
        'security_rating': convert_numeric_rating_to_letter(metrics.get('security_rating')),
        'sqale_rating': convert_numeric_rating_to_letter(metrics.get('sqale_rating')),
        
        # Remediation effort
        'reliability_remediation_effort': int(metrics.get('reliability_remediation_effort', 0)),
        'security_remediation_effort': int(metrics.get('security_remediation_effort', 0)),
        
        # Technical debt (corrected keys)
        'technical_debt': int(metrics.get('sqale_index', 0)),  # Map sqale_index to technical_debt
        'sqale_debt_ratio': float(metrics.get('sqale_debt_ratio', 0)),
        
        # Coverage metrics
        'coverage_percentage': float(metrics.get('coverage', 0)),
        'line_coverage_percentage': float(metrics.get('line_coverage', 0)),
        'branch_coverage_percentage': float(metrics.get('branch_coverage', 0)),
        'covered_lines': int(metrics.get('covered_lines', 0)),
        'uncovered_lines': int(metrics.get('uncovered_lines', 0)),
        'covered_conditions': int(metrics.get('covered_conditions', 0)),
        'uncovered_conditions': int(metrics.get('uncovered_conditions', 0)),
        'lines_to_cover': int(metrics.get('lines_to_cover', 0)),
        'conditions_to_cover': int(metrics.get('conditions_to_cover', 0)),
        
        # Duplication metrics
        'duplicated_lines_density': float(metrics.get('duplicated_lines_density', 0)),
        'duplicated_lines': int(metrics.get('duplicated_lines', 0)),
        'duplicated_blocks': int(metrics.get('duplicated_blocks', 0)),
        'duplicated_files': int(metrics.get('duplicated_files', 0)),
        
        # Complexity metrics
        'complexity': int(metrics.get('complexity', 0)),
        'cognitive_complexity': int(metrics.get('cognitive_complexity', 0)),
        
        # Comment metrics
        'comment_lines': int(metrics.get('comment_lines', 0)),
        'comment_lines_density': float(metrics.get('comment_lines_density', 0)),
        
        # Quality gate
        'alert_status': metrics.get('alert_status'),
        'quality_gate_details': metrics.get('quality_gate_details'),
        
        # Issue totals
        'violations': int(metrics.get('violations', 0)),
        'open_issues': int(metrics.get('open_issues', 0)),
        'confirmed_issues': int(metrics.get('confirmed_issues', 0)),
        'false_positive_issues': int(metrics.get('false_positive_issues', 0)),
        'accepted_issues': int(metrics.get('accepted_issues', 0)),
        
        # Additional metrics
        'effort_to_reach_maintainability_rating_a': int(metrics.get('effort_to_reach_maintainability_rating_a', 0)),
        
        # Metadata
        'data_source_timestamp': datetime.now(),
        'is_carried_forward': False,
        
        # New code metrics (with corrected keys)
        'new_code_lines': int(metrics.get('new_lines', 0)),
        'new_code_ncloc': int(metrics.get('new_ncloc', 0)),
        
        # New code bugs
        'new_code_bugs_total': int(metrics.get('new_bugs', 0)),
        'new_code_bugs_blocker': new_code_issues.get('new_code_bug_blocker', 0),
        'new_code_bugs_critical': new_code_issues.get('new_code_bug_critical', 0),
        'new_code_bugs_major': new_code_issues.get('new_code_bug_major', 0),
        'new_code_bugs_minor': new_code_issues.get('new_code_bug_minor', 0),
        'new_code_bugs_info': new_code_issues.get('new_code_bug_info', 0),
        'new_code_reliability_remediation_effort': int(metrics.get('new_reliability_remediation_effort', 0)),
        
        # New code vulnerabilities
        'new_code_vulnerabilities_total': int(metrics.get('new_vulnerabilities', 0)),
        'new_code_vulnerabilities_blocker': new_code_issues.get('new_code_vulnerability_blocker', 0),
        'new_code_vulnerabilities_critical': new_code_issues.get('new_code_vulnerability_critical', 0),
        'new_code_vulnerabilities_high': new_code_issues.get('new_code_vulnerability_major', 0),
        'new_code_vulnerabilities_medium': new_code_issues.get('new_code_vulnerability_minor', 0),
        'new_code_vulnerabilities_low': new_code_issues.get('new_code_vulnerability_info', 0),
        'new_code_security_remediation_effort': int(metrics.get('new_security_remediation_effort', 0)),
        
        # New code smells
        'new_code_code_smells_total': int(metrics.get('new_code_smells', 0)),
        'new_code_code_smells_blocker': new_code_issues.get('new_code_code_smell_blocker', 0),
        'new_code_code_smells_critical': new_code_issues.get('new_code_code_smell_critical', 0),
        'new_code_code_smells_major': new_code_issues.get('new_code_code_smell_major', 0),
        'new_code_code_smells_minor': new_code_issues.get('new_code_code_smell_minor', 0),
        'new_code_code_smells_info': new_code_issues.get('new_code_code_smell_info', 0),
        'new_code_technical_debt': int(metrics.get('new_technical_debt', 0)),
        'new_code_sqale_debt_ratio': float(metrics.get('new_sqale_debt_ratio', 0)),
        
        # New code security hotspots
        'new_code_security_hotspots_total': int(metrics.get('new_security_hotspots', 0)),
        'new_code_security_hotspots_high': new_code_issues.get('new_code_security_hotspot_high', 0),
        'new_code_security_hotspots_medium': new_code_issues.get('new_code_security_hotspot_medium', 0),
        'new_code_security_hotspots_low': new_code_issues.get('new_code_security_hotspot_low', 0),
        'new_code_security_hotspots_to_review': new_code_issues.get('new_code_security_hotspot_to_review', 0),
        'new_code_security_hotspots_acknowledged': new_code_issues.get('new_code_security_hotspot_acknowledged', 0),
        'new_code_security_hotspots_fixed': new_code_issues.get('new_code_security_hotspot_fixed', 0),
        'new_code_security_hotspots_safe': new_code_issues.get('new_code_security_hotspot_safe', 0),
        
        # New code security review
        'new_code_security_hotspots_reviewed': float(metrics.get('new_security_hotspots_reviewed', 0)),
        'new_code_security_review_rating': metrics.get('new_security_review_rating'),
        
        # New code coverage
        'new_code_coverage_percentage': float(metrics.get('new_coverage', 0)),
        'new_code_line_coverage_percentage': float(metrics.get('new_line_coverage', 0)),
        'new_code_branch_coverage_percentage': float(metrics.get('new_branch_coverage', 0)),
        'new_code_covered_lines': int(metrics.get('new_covered_lines', 0)),
        'new_code_uncovered_lines': int(metrics.get('new_uncovered_lines', 0)),
        'new_code_covered_conditions': int(metrics.get('new_covered_conditions', 0)),
        'new_code_uncovered_conditions': int(metrics.get('new_uncovered_conditions', 0)),
        'new_code_lines_to_cover': int(metrics.get('new_lines_to_cover', 0)),
        'new_code_conditions_to_cover': int(metrics.get('new_conditions_to_cover', 0)),
        
        # New code duplications
        'new_code_duplicated_lines_density': float(metrics.get('new_duplicated_lines_density', 0)),
        'new_code_duplicated_lines': int(metrics.get('new_duplicated_lines', 0)),
        'new_code_duplicated_blocks': int(metrics.get('new_duplicated_blocks', 0)),
        
        # New code complexity
        'new_code_complexity': int(metrics.get('new_complexity', 0)),
        'new_code_cognitive_complexity': int(metrics.get('new_cognitive_complexity', 0)),
        
        # New code issues
        'new_violations': int(metrics.get('new_violations', 0)),
        'new_accepted_issues': int(metrics.get('new_accepted_issues', 0)),
        'new_confirmed_issues': int(metrics.get('new_confirmed_issues', 0)),
        
        # New code period
        'new_code_period_date': new_code_period_date,
        'new_code_period_mode': metrics.get('new_code_period_mode'),
        'new_code_period_value': metrics.get('new_code_period_value')
    }

def insert_metric(cursor, project_id: int, metric_data: Dict[str, Any]) -> None:
    """Insert metric data into database with all new columns."""
    columns = list(metric_data.keys())
    columns.insert(0, 'project_id')
    values = [project_id] + list(metric_data.values())
    
    # Build the INSERT statement
    placeholders = ', '.join(['%s'] * len(values))
    column_names = ', '.join(columns)
    
    # Build the UPDATE clause for upsert
    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in ['project_id', 'metric_date']])
    
    query = f"""
        INSERT INTO sonarqube_metrics.daily_project_metrics ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (project_id, metric_date) DO UPDATE
        SET {update_clause}
    """
    
    cursor.execute(query, values)

def insert_carried_forward_metric(cursor, project_id: int, metric_date: str, last_known: Dict[str, Any]) -> None:
    """Insert carried forward metric when current data is unavailable."""
    # Copy last known values but update the date and carry-forward flag
    carried_data = last_known.copy()
    carried_data['metric_date'] = metric_date
    carried_data['is_carried_forward'] = True
    carried_data['data_source_timestamp'] = datetime.now()
    
    # Remove database-specific columns
    for col in ['metric_id', 'project_id', 'created_at']:
        carried_data.pop(col, None)
    
    insert_metric(cursor, project_id, carried_data)

def transform_and_load_metrics(**context) -> None:
    """Transform and load metrics into PostgreSQL."""
    raw_metrics = context['task_instance'].xcom_pull(task_ids='extract_metrics', key='raw_metrics')
    projects = context['task_instance'].xcom_pull(task_ids='fetch_projects', key='projects')
    
    pg_hook = PostgresHook(postgres_conn_id='sonarqube_metrics_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Create project mapping
    project_map = {p['key']: p['name'] for p in projects}
    
    try:
        # First, ensure all projects exist in the database
        for project in projects:
            project_key = project['key']
            project_name = project['name']
            last_analysis_date = project.get('lastAnalysisDate')
            
            cursor.execute("""
                INSERT INTO sonarqube_metrics.sq_projects (sonarqube_project_key, project_name, last_analysis_date_from_sq)
                VALUES (%s, %s, %s)
                ON CONFLICT (sonarqube_project_key) DO UPDATE
                SET project_name = EXCLUDED.project_name,
                    last_analysis_date_from_sq = EXCLUDED.last_analysis_date_from_sq,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING project_id
            """, (project_key, project_name, last_analysis_date))
            
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
    outlets=[SONARQUBE_METRICS_DATASET],
    dag=dag
)

# Set task dependencies
fetch_projects_task >> extract_metrics_task >> transform_load_task