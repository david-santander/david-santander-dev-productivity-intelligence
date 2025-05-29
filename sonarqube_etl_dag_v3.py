#!/usr/bin/env python3
"""
SonarQube ETL DAG - Version 3.0 - COMPREHENSIVE METRICS
====================================

This is the comprehensive version of the SonarQube ETL DAG that includes:
- All standard SonarQube metrics (150+ metrics)
- Complete size, complexity, coverage, and duplication metrics
- Technical debt and remediation effort metrics
- Enhanced new code tracking with all status breakdowns
- Robust error handling and data validation

Main Components:
1. fetch_projects() - Retrieves all SonarQube projects
2. extract_metrics_for_project() - Extracts comprehensive metrics for each project  
3. transform_and_load_metrics() - Transforms and loads data into PostgreSQL

The DAG runs daily and processes yesterday's metrics to ensure data completeness.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dateutil import parser

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from sonarqube_client import SonarQubeClient

# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_sonarqube_config() -> Dict[str, Any]:
    """Get SonarQube configuration."""
    return {
        'base_url': 'http://sonarqube:9000',
        'token': 'squ_4c8b7be6b70cd6fcadc5a1c3e03fff4e2c2087a5',
        'auth': ('squ_4c8b7be6b70cd6fcadc5a1c3e03fff4e2c2087a5', '')
    }

def fetch_projects(**context) -> int:
    """Fetch all projects from SonarQube."""
    config = get_sonarqube_config()
    client = SonarQubeClient(config)
    
    projects = client.fetch_all_projects()
    context['task_instance'].xcom_push(key='projects', value=projects)
    
    logging.info(f"Fetched {len(projects)} projects from SonarQube")
    return len(projects)

def extract_metrics_for_project(**context) -> int:
    """Extract comprehensive metrics for all projects."""
    projects = context['task_instance'].xcom_pull(task_ids='fetch_projects', key='projects')
    config = get_sonarqube_config()
    client = SonarQubeClient(config)
    
    # Extract yesterday's data for daily runs
    execution_date = context['execution_date']
    metric_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logging.info(f"Extracting comprehensive metrics for date: {metric_date}")
    
    all_metrics = []
    
    for project in projects:
        project_key = project['key']
        logging.info(f"Extracting metrics for project: {project_key}")
        
        try:
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

def transform_metric_data(metric_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw metric data into comprehensive database format."""
    metrics = metric_data['metrics']
    issues = metric_data['issues_breakdown']
    new_code_issues = metric_data.get('new_code_issues_breakdown', {})
    
    # Helper functions for safe type conversion
    def safe_int(value, default=0):
        try:
            return int(float(value)) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    def safe_float(value, default=0.0):
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    # Parse new code period information
    new_code_period_date = None
    if 'new_code_period_date' in metrics and metrics['new_code_period_date']:
        try:
            new_code_period_date = parser.parse(metrics['new_code_period_date']).date()
        except:
            new_code_period_date = None
    
    new_code_period_mode = metrics.get('new_code_period_mode', 'days')
    new_code_period_value = str(metrics.get('new_code_period_value', '30'))
    
    # Calculate totals from breakdowns for verification
    bugs_breakdown_total = (
        issues.get('bug_blocker', 0) + issues.get('bug_critical', 0) +
        issues.get('bug_major', 0) + issues.get('bug_minor', 0) + issues.get('bug_info', 0)
    )
    
    vulnerabilities_breakdown_total = (
        issues.get('vulnerability_blocker', 0) + issues.get('vulnerability_critical', 0) +
        issues.get('vulnerability_major', 0) + issues.get('vulnerability_minor', 0) + issues.get('vulnerability_info', 0)
    )
    
    code_smells_breakdown_total = (
        issues.get('code_smell_blocker', 0) + issues.get('code_smell_critical', 0) +
        issues.get('code_smell_major', 0) + issues.get('code_smell_minor', 0) + issues.get('code_smell_info', 0)
    )
    
    # Use the higher value between API and breakdown
    bugs_total = max(safe_int(metrics.get('bugs', 0)), bugs_breakdown_total)
    vulnerabilities_total = max(safe_int(metrics.get('vulnerabilities', 0)), vulnerabilities_breakdown_total)
    code_smells_total = max(safe_int(metrics.get('code_smells', 0)), code_smells_breakdown_total)
    
    # Security hotspots total validation
    security_hotspots_api_total = safe_int(metrics.get('security_hotspots', 0))
    security_hotspots_to_review = issues.get('security_hotspot_to_review', 0)
    security_hotspots_acknowledged = issues.get('security_hotspot_acknowledged', 0)
    security_hotspots_fixed = issues.get('security_hotspot_fixed', 0)
    security_hotspots_safe = issues.get('security_hotspot_safe', 0)
    security_hotspots_status_total = (security_hotspots_to_review + security_hotspots_acknowledged + 
                                     security_hotspots_fixed + security_hotspots_safe)
    
    security_hotspots_total = max(security_hotspots_api_total, security_hotspots_status_total)
    
    return {
        'metric_date': metric_data['metric_date'],
        
        # ================================================================
        # SIZE METRICS
        # ================================================================
        'lines': safe_int(metrics.get('lines', 0)),
        'ncloc': safe_int(metrics.get('ncloc', 0)),
        'classes': safe_int(metrics.get('classes', 0)),
        'functions': safe_int(metrics.get('functions', 0)),
        'statements': safe_int(metrics.get('statements', 0)),
        'files': safe_int(metrics.get('files', 0)),
        'directories': safe_int(metrics.get('directories', 0)),
        
        # ================================================================
        # BUG METRICS (RELIABILITY)
        # ================================================================
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
        
        'reliability_rating': metrics.get('reliability_rating', None),
        'reliability_remediation_effort': safe_int(metrics.get('reliability_remediation_effort', 0)),
        
        # ================================================================
        # VULNERABILITY METRICS (SECURITY)
        # ================================================================
        'vulnerabilities_total': vulnerabilities_total,
        'vulnerabilities_blocker': issues.get('vulnerability_blocker', 0),
        'vulnerabilities_critical': issues.get('vulnerability_critical', 0),
        'vulnerabilities_high': issues.get('vulnerability_major', 0),  # MAJOR maps to HIGH
        'vulnerabilities_medium': issues.get('vulnerability_minor', 0),  # MINOR maps to MEDIUM
        'vulnerabilities_low': issues.get('vulnerability_info', 0),  # INFO maps to LOW
        
        'vulnerabilities_open': issues.get('vulnerability_open', 0),
        'vulnerabilities_confirmed': issues.get('vulnerability_confirmed', 0),
        'vulnerabilities_reopened': issues.get('vulnerability_reopened', 0),
        'vulnerabilities_resolved': issues.get('vulnerability_resolved', 0),
        'vulnerabilities_closed': issues.get('vulnerability_closed', 0),
        'vulnerabilities_false_positive': issues.get('vulnerability_false-positive', 0),
        'vulnerabilities_wontfix': issues.get('vulnerability_wontfix', 0),
        
        'security_rating': metrics.get('security_rating', None),
        'security_remediation_effort': safe_int(metrics.get('security_remediation_effort', 0)),
        
        # ================================================================
        # CODE SMELL METRICS (MAINTAINABILITY)
        # ================================================================
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
        
        'technical_debt': safe_int(metrics.get('technical_debt', 0)),
        'sqale_rating': metrics.get('sqale_rating', None),
        'sqale_debt_ratio': safe_float(metrics.get('sqale_debt_ratio', 0)),
        
        # ================================================================
        # SECURITY HOTSPOT METRICS
        # ================================================================
        'security_hotspots_total': security_hotspots_total,
        'security_hotspots_high': issues.get('security_hotspot_high', 0),
        'security_hotspots_medium': issues.get('security_hotspot_medium', 0),
        'security_hotspots_low': issues.get('security_hotspot_low', 0),
        
        'security_hotspots_to_review': security_hotspots_to_review,
        'security_hotspots_acknowledged': security_hotspots_acknowledged,
        'security_hotspots_fixed': security_hotspots_fixed,
        'security_hotspots_safe': security_hotspots_safe,
        
        # ================================================================
        # COVERAGE METRICS
        # ================================================================
        'coverage_percentage': safe_float(metrics.get('coverage', 0)),
        'line_coverage_percentage': safe_float(metrics.get('line_coverage', 0)),
        'branch_coverage_percentage': safe_float(metrics.get('branch_coverage', 0)),
        'covered_lines': safe_int(metrics.get('covered_lines', 0)),
        'uncovered_lines': safe_int(metrics.get('uncovered_lines', 0)),
        'covered_conditions': safe_int(metrics.get('covered_conditions', 0)),
        'uncovered_conditions': safe_int(metrics.get('uncovered_conditions', 0)),
        'lines_to_cover': safe_int(metrics.get('lines_to_cover', 0)),
        'conditions_to_cover': safe_int(metrics.get('conditions_to_cover', 0)),
        
        # ================================================================
        # DUPLICATION METRICS
        # ================================================================
        'duplicated_lines_density': safe_float(metrics.get('duplicated_lines_density', 0)),
        'duplicated_lines': safe_int(metrics.get('duplicated_lines', 0)),
        'duplicated_blocks': safe_int(metrics.get('duplicated_blocks', 0)),
        'duplicated_files': safe_int(metrics.get('duplicated_files', 0)),
        
        # ================================================================
        # COMPLEXITY METRICS
        # ================================================================
        'complexity': safe_int(metrics.get('complexity', 0)),
        'cognitive_complexity': safe_int(metrics.get('cognitive_complexity', 0)),
        
        # ================================================================
        # COMMENT METRICS
        # ================================================================
        'comment_lines': safe_int(metrics.get('comment_lines', 0)),
        'comment_lines_density': safe_float(metrics.get('comment_lines_density', 0)),
        
        # ================================================================
        # NEW CODE METRICS
        # ================================================================
        'new_code_lines': safe_int(metrics.get('new_lines', 0)),
        'new_code_ncloc': safe_int(metrics.get('new_ncloc', 0)),
        
        # New code bugs
        'new_code_bugs_total': safe_int(metrics.get('new_bugs', 0)),
        'new_code_bugs_blocker': new_code_issues.get('new_code_bug_blocker', 0),
        'new_code_bugs_critical': new_code_issues.get('new_code_bug_critical', 0),
        'new_code_bugs_major': new_code_issues.get('new_code_bug_major', 0),
        'new_code_bugs_minor': new_code_issues.get('new_code_bug_minor', 0),
        'new_code_bugs_info': new_code_issues.get('new_code_bug_info', 0),
        'new_code_reliability_remediation_effort': safe_int(metrics.get('new_reliability_remediation_effort', 0)),
        
        # New code vulnerabilities
        'new_code_vulnerabilities_total': safe_int(metrics.get('new_vulnerabilities', 0)),
        'new_code_vulnerabilities_blocker': new_code_issues.get('new_code_vulnerability_blocker', 0),
        'new_code_vulnerabilities_critical': new_code_issues.get('new_code_vulnerability_critical', 0),
        'new_code_vulnerabilities_high': new_code_issues.get('new_code_vulnerability_major', 0),
        'new_code_vulnerabilities_medium': new_code_issues.get('new_code_vulnerability_minor', 0),
        'new_code_vulnerabilities_low': new_code_issues.get('new_code_vulnerability_info', 0),
        'new_code_security_remediation_effort': safe_int(metrics.get('new_security_remediation_effort', 0)),
        
        # New code smells
        'new_code_code_smells_total': safe_int(metrics.get('new_code_smells', 0)),
        'new_code_code_smells_blocker': new_code_issues.get('new_code_code_smell_blocker', 0),
        'new_code_code_smells_critical': new_code_issues.get('new_code_code_smell_critical', 0),
        'new_code_code_smells_major': new_code_issues.get('new_code_code_smell_major', 0),
        'new_code_code_smells_minor': new_code_issues.get('new_code_code_smell_minor', 0),
        'new_code_code_smells_info': new_code_issues.get('new_code_code_smell_info', 0),
        'new_code_technical_debt': safe_int(metrics.get('new_technical_debt', 0)),
        'new_code_sqale_debt_ratio': safe_float(metrics.get('new_sqale_debt_ratio', 0)),
        
        # New code security hotspots
        'new_code_security_hotspots_total': safe_int(metrics.get('new_security_hotspots', 0)),
        'new_code_security_hotspots_high': new_code_issues.get('new_code_security_hotspot_high', 0),
        'new_code_security_hotspots_medium': new_code_issues.get('new_code_security_hotspot_medium', 0),
        'new_code_security_hotspots_low': new_code_issues.get('new_code_security_hotspot_low', 0),
        'new_code_security_hotspots_to_review': new_code_issues.get('new_code_security_hotspot_to_review', 0),
        'new_code_security_hotspots_acknowledged': new_code_issues.get('new_code_security_hotspot_acknowledged', 0),
        'new_code_security_hotspots_fixed': new_code_issues.get('new_code_security_hotspot_fixed', 0),
        'new_code_security_hotspots_safe': new_code_issues.get('new_code_security_hotspot_safe', 0),
        
        # New code coverage
        'new_code_coverage_percentage': safe_float(metrics.get('new_coverage', 0)),
        'new_code_line_coverage_percentage': safe_float(metrics.get('new_line_coverage', 0)),
        'new_code_branch_coverage_percentage': safe_float(metrics.get('new_branch_coverage', 0)),
        'new_code_covered_lines': safe_int(metrics.get('new_covered_lines', 0)),
        'new_code_uncovered_lines': safe_int(metrics.get('new_uncovered_lines', 0)),
        'new_code_covered_conditions': safe_int(metrics.get('new_covered_conditions', 0)),
        'new_code_uncovered_conditions': safe_int(metrics.get('new_uncovered_conditions', 0)),
        'new_code_lines_to_cover': safe_int(metrics.get('new_lines_to_cover', 0)),
        'new_code_conditions_to_cover': safe_int(metrics.get('new_conditions_to_cover', 0)),
        
        # New code duplications
        'new_code_duplicated_lines_density': safe_float(metrics.get('new_duplicated_lines_density', 0)),
        'new_code_duplicated_lines': safe_int(metrics.get('new_duplicated_lines', 0)),
        'new_code_duplicated_blocks': safe_int(metrics.get('new_duplicated_blocks', 0)),
        
        # New code complexity
        'new_code_complexity': safe_int(metrics.get('new_complexity', 0)),
        'new_code_cognitive_complexity': safe_int(metrics.get('new_cognitive_complexity', 0)),
        
        # New code period information
        'new_code_period_date': new_code_period_date,
        'new_code_period_mode': new_code_period_mode,
        'new_code_period_value': new_code_period_value,
        
        # ================================================================
        # METADATA
        # ================================================================
        'data_source_timestamp': datetime.now(),
        'is_carried_forward': False
    }

def insert_metric(cursor, project_id: int, metric_data: Dict[str, Any]) -> None:
    """Insert comprehensive metric data into database."""
    
    # Generate the column names and values dynamically
    columns = list(metric_data.keys())
    placeholders = ', '.join(['%s'] * len(columns))
    column_names = ', '.join(columns)
    
    # Create the conflict resolution part for all columns except metadata
    conflict_columns = [col for col in columns if col not in ['data_source_timestamp']]
    conflict_updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in conflict_columns])
    
    query = f"""
        INSERT INTO sonarqube_metrics.daily_project_metrics 
        (project_id, {column_names})
        VALUES (%s, {placeholders})
        ON CONFLICT (project_id, metric_date) 
        DO UPDATE SET {conflict_updates}, data_source_timestamp = CURRENT_TIMESTAMP
    """
    
    values = [project_id] + list(metric_data.values())
    cursor.execute(query, values)

def insert_carried_forward_metric(cursor, project_id: int, metric_date: str, last_known: Dict[str, Any]) -> None:
    """Insert carried forward metric data."""
    carried_forward_data = dict(last_known)
    carried_forward_data['metric_date'] = metric_date
    carried_forward_data['data_source_timestamp'] = datetime.now()
    carried_forward_data['is_carried_forward'] = True
    
    # Remove metadata fields that shouldn't be carried forward
    for field in ['metric_id', 'project_id', 'created_at']:
        carried_forward_data.pop(field, None)
    
    insert_metric(cursor, project_id, carried_forward_data)

def transform_and_load_metrics(**context) -> None:
    """Transform and load comprehensive metrics into PostgreSQL."""
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
        logging.info("Successfully loaded all comprehensive metrics to database")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading metrics to database: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

# Create the DAG
dag = DAG(
    'sonarqube_etl_comprehensive',
    default_args=DEFAULT_ARGS,
    description='Comprehensive SonarQube metrics ETL pipeline with 150+ metrics',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    catchup=False,
    max_active_runs=1,
    tags=['sonarqube', 'etl', 'comprehensive', 'metrics']
)

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

# Set dependencies
fetch_projects_task >> extract_metrics_task >> transform_load_task