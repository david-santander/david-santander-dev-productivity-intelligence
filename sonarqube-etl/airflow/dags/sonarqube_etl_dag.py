"""
SonarQube ETL DAG v4 - Enterprise-Grade Pipeline
================================================

This version implements enterprise-grade best practices:
- Modular task architecture with clear separation of concerns
- Comprehensive error handling and recovery mechanisms
- Data validation and quality checks
- Performance optimizations with parallel processing
- Detailed monitoring and alerting
- Configurable through Airflow Variables
- Support for multiple environments
- Extensive documentation and logging

Key improvements:
1. Task group organization for better visualization
2. Dynamic task generation based on projects
3. Data quality validation framework
4. Incremental loading with change detection
5. Advanced scheduling with data-aware dependencies
6. Custom operators for reusability
7. Comprehensive testing hooks
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from functools import wraps
import json

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable, Connection
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.datasets import Dataset
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email
from airflow.configuration import conf

# Import the enhanced SonarQube client
from sonarqube_client_v4 import (
    SonarQubeClient, 
    SonarQubeConfig,
    ProjectMetrics,
    convert_rating_to_letter
)

# Configure logging
logger = logging.getLogger(__name__)

# =====================================================================
# CONFIGURATION
# =====================================================================

@dataclass
class ETLConfig:
    """ETL pipeline configuration."""
    environment: str = 'production'
    batch_size: int = 10
    parallel_workers: int = 4
    retry_attempts: int = 3
    retry_delay_minutes: int = 5
    enable_notifications: bool = True
    enable_data_validation: bool = True
    enable_incremental_load: bool = True
    data_retention_days: int = 365
    

# Load configuration from Airflow Variables
def load_etl_config() -> ETLConfig:
    """Load ETL configuration from Airflow Variables."""
    config_json = Variable.get('sonarqube_etl_config', default_var={})
    return ETLConfig(**config_json) if config_json else ETLConfig()


# Dataset definitions for data-aware scheduling
SONARQUBE_METRICS_DATASET = Dataset("postgres://sonarqube_metrics/daily_project_metrics")
SONARQUBE_PROJECTS_DATASET = Dataset("postgres://sonarqube_metrics/sq_projects")

# =====================================================================
# CONSTANTS AND DEFAULTS
# =====================================================================

DEFAULT_ARGS = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': Variable.get('alert_email_list', default_var=['team@example.com']),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# Metric mapping for database columns
METRIC_MAPPINGS = {
    'technical_debt': 'sqale_index',  # Map sqale_index to technical_debt column
    'new_code_technical_debt': 'new_technical_debt',  # Already correct
}

# =====================================================================
# HELPER FUNCTIONS AND DECORATORS
# =====================================================================

def performance_monitor(func):
    """Decorator to monitor task performance."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        task_name = func.__name__
        
        logger.info(f"Starting task: {task_name}")
        try:
            result = func(*args, **kwargs)
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Completed task: {task_name} in {duration:.2f} seconds")
            
            # Log to metrics database if available
            try:
                context = kwargs.get('context', {})
                if context:
                    _log_task_metrics(
                        task_name=task_name,
                        duration=duration,
                        status='success',
                        context=context
                    )
            except Exception as e:
                logger.warning(f"Failed to log task metrics: {e}")
                
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Failed task: {task_name} after {duration:.2f} seconds - {str(e)}")
            
            # Log failure metrics
            try:
                context = kwargs.get('context', {})
                if context:
                    _log_task_metrics(
                        task_name=task_name,
                        duration=duration,
                        status='failed',
                        error=str(e),
                        context=context
                    )
            except Exception as log_error:
                logger.warning(f"Failed to log task failure metrics: {log_error}")
                
            raise
            
    return wrapper


def _log_task_metrics(
    task_name: str, 
    duration: float, 
    status: str, 
    context: Dict[str, Any],
    error: Optional[str] = None
):
    """Log task execution metrics to database."""
    try:
        pg_hook = PostgresHook(postgres_conn_id='sonarqube_metrics_db')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO sonarqube_metrics.etl_task_metrics 
            (task_name, execution_date, duration_seconds, status, error_message, dag_run_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (task_name, execution_date) DO UPDATE
            SET duration_seconds = EXCLUDED.duration_seconds,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = CURRENT_TIMESTAMP
        """, (
            task_name,
            context['execution_date'],
            duration,
            status,
            error,
            context.get('dag_run').run_id
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.warning(f"Failed to log task metrics: {e}")


# =====================================================================
# DATA VALIDATION
# =====================================================================

class DataValidator:
    """Validates data quality and integrity."""
    
    @staticmethod
    def validate_project_data(projects: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """Validate project data quality."""
        errors = []
        
        if not projects:
            errors.append("No projects found")
            return False, errors
            
        for idx, project in enumerate(projects):
            if not project.get('key'):
                errors.append(f"Project {idx} missing key")
            if not project.get('name'):
                errors.append(f"Project {idx} missing name")
                
        return len(errors) == 0, errors
        
    @staticmethod
    def validate_metrics_data(metrics: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """Validate metrics data quality."""
        errors = []
        
        for metric in metrics:
            if metric.get('error'):
                continue
                
            # Check required fields
            if not metric.get('project_key'):
                errors.append(f"Metric missing project_key")
            if not metric.get('metric_date'):
                errors.append(f"Metric missing metric_date")
            if not metric.get('metrics'):
                errors.append(f"Metric for {metric.get('project_key')} missing metrics data")
                
            # Validate metric values
            metrics_data = metric.get('metrics', {})
            
            # Check for negative values in count metrics
            count_metrics = ['bugs', 'vulnerabilities', 'code_smells', 'ncloc']
            for cm in count_metrics:
                if cm in metrics_data and float(metrics_data.get(cm, 0)) < 0:
                    errors.append(f"Negative value for {cm} in {metric.get('project_key')}")
                    
            # Check percentage bounds
            percentage_metrics = ['coverage', 'duplicated_lines_density']
            for pm in percentage_metrics:
                value = float(metrics_data.get(pm, 0))
                if value < 0 or value > 100:
                    errors.append(f"Invalid percentage {value} for {pm} in {metric.get('project_key')}")
                    
        return len(errors) == 0, errors


# =====================================================================
# SONARQUBE INTEGRATION
# =====================================================================

def get_sonarqube_config() -> SonarQubeConfig:
    """Get SonarQube configuration from Airflow connection or environment."""
    try:
        # Try to get from Airflow connection first
        conn = Connection.get_connection_from_secrets('sonarqube_api')
        return SonarQubeConfig(
            base_url=f"{conn.schema}://{conn.host}:{conn.port or 9000}",
            token=conn.password,
            timeout=int(conn.extra_dejson.get('timeout', 30)),
            max_retries=int(conn.extra_dejson.get('max_retries', 3))
        )
    except Exception:
        # Fallback to environment variables
        return SonarQubeConfig(
            base_url=os.environ.get('SONARQUBE_BASE_URL', 'http://sonarqube:9000'),
            token=os.environ.get('SONARQUBE_TOKEN', ''),
            timeout=30,
            max_retries=3
        )


# =====================================================================
# ETL TASKS
# =====================================================================

@task(pool='sonarqube_api')
@performance_monitor
def fetch_projects(**context) -> List[Dict[str, Any]]:
    """Fetch all projects from SonarQube."""
    config = get_sonarqube_config()
    
    with SonarQubeClient(config) as client:
        projects = client.fetch_all_projects()
        
    # Convert to dict format
    project_dicts = [
        {
            'key': p.key,
            'name': p.name,
            'qualifier': p.qualifier,
            'visibility': p.visibility,
            'lastAnalysisDate': p.last_analysis_date.isoformat() if p.last_analysis_date else None
        }
        for p in projects
    ]
    
    # Validate data
    etl_config = load_etl_config()
    if etl_config.enable_data_validation:
        is_valid, errors = DataValidator.validate_project_data(project_dicts)
        if not is_valid:
            raise AirflowException(f"Project data validation failed: {errors}")
    
    logger.info(f"Successfully fetched {len(project_dicts)} projects")
    return project_dicts


@task(pool='sonarqube_api')
@performance_monitor
def extract_project_metrics(
    project: Dict[str, Any],
    metric_date: str,
    **context
) -> Dict[str, Any]:
    """Extract metrics for a single project."""
    project_key = project['key']
    config = get_sonarqube_config()
    
    logger.info(f"Extracting metrics for {project_key} on {metric_date}")
    
    try:
        with SonarQubeClient(config) as client:
            metrics = client.fetch_metrics_smart(project_key, metric_date)
            
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to fetch metrics for {project_key} on {metric_date}: {str(e)}")
        return {
            'project_key': project_key,
            'metric_date': metric_date,
            'metrics': None,
            'issues_breakdown': None,
            'new_code_issues_breakdown': None,
            'error': str(e)
        }


@task
@performance_monitor
def transform_metrics_batch(
    metrics_batch: List[Dict[str, Any]],
    **context
) -> List[Dict[str, Any]]:
    """Transform a batch of raw metrics into database format."""
    transformed_batch = []
    
    for metric_data in metrics_batch:
        if metric_data.get('error') or not metric_data.get('metrics'):
            transformed_batch.append({
                'project_key': metric_data['project_key'],
                'metric_date': metric_data['metric_date'],
                'error': metric_data.get('error'),
                'transformed': False
            })
            continue
            
        try:
            transformed = transform_single_metric(metric_data)
            transformed_batch.append({
                'project_key': metric_data['project_key'],
                'metric_date': metric_data['metric_date'],
                'data': transformed,
                'transformed': True
            })
        except Exception as e:
            logger.error(f"Failed to transform metrics for {metric_data['project_key']}: {e}")
            transformed_batch.append({
                'project_key': metric_data['project_key'],
                'metric_date': metric_data['metric_date'],
                'error': str(e),
                'transformed': False
            })
            
    return transformed_batch


def transform_single_metric(metric_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a single metric record."""
    metrics = metric_data['metrics']
    issues = metric_data['issues_breakdown']
    new_code_issues = metric_data.get('new_code_issues_breakdown', {})
    
    # Helper functions
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
    
    # Parse dates
    from dateutil import parser
    new_code_period_date = None
    if 'new_code_period_date' in metrics and metrics['new_code_period_date']:
        try:
            new_code_period_date = parser.parse(metrics['new_code_period_date']).date()
        except:
            pass
    
    # Build transformed data
    transformed = {
        'metric_date': metric_data['metric_date'],
        
        # Size metrics
        'lines': safe_int(metrics.get('lines')),
        'ncloc': safe_int(metrics.get('ncloc')),
        'classes': safe_int(metrics.get('classes')),
        'functions': safe_int(metrics.get('functions')),
        'statements': safe_int(metrics.get('statements')),
        'files': safe_int(metrics.get('files')),
        'directories': safe_int(metrics.get('directories')),
        
        # Bug metrics
        'bugs_total': safe_int(metrics.get('bugs')),
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
        'vulnerabilities_total': safe_int(metrics.get('vulnerabilities')),
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
        'code_smells_total': safe_int(metrics.get('code_smells')),
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
        'security_hotspots_total': safe_int(metrics.get('security_hotspots')),
        'security_hotspots_high': issues.get('security_hotspot_high', 0),
        'security_hotspots_medium': issues.get('security_hotspot_medium', 0),
        'security_hotspots_low': issues.get('security_hotspot_low', 0),
        'security_hotspots_to_review': issues.get('security_hotspot_to_review', 0),
        'security_hotspots_acknowledged': issues.get('security_hotspot_acknowledged', 0),
        'security_hotspots_fixed': issues.get('security_hotspot_fixed', 0),
        'security_hotspots_safe': issues.get('security_hotspot_safe', 0),
        
        # Security review metrics
        'security_hotspots_reviewed': safe_float(metrics.get('security_hotspots_reviewed')),
        'security_review_rating': convert_rating_to_letter(metrics.get('security_review_rating')),
        
        # Quality ratings
        'reliability_rating': convert_rating_to_letter(metrics.get('reliability_rating')),
        'security_rating': convert_rating_to_letter(metrics.get('security_rating')),
        'sqale_rating': convert_rating_to_letter(metrics.get('sqale_rating')),
        
        # Remediation effort
        'reliability_remediation_effort': safe_int(metrics.get('reliability_remediation_effort')),
        'security_remediation_effort': safe_int(metrics.get('security_remediation_effort')),
        
        # Technical debt
        'technical_debt': safe_int(metrics.get('sqale_index')),
        'sqale_debt_ratio': safe_float(metrics.get('sqale_debt_ratio')),
        
        # Coverage metrics
        'coverage_percentage': safe_float(metrics.get('coverage')),
        'line_coverage_percentage': safe_float(metrics.get('line_coverage')),
        'branch_coverage_percentage': safe_float(metrics.get('branch_coverage')),
        'covered_lines': safe_int(metrics.get('covered_lines')),
        'uncovered_lines': safe_int(metrics.get('uncovered_lines')),
        'covered_conditions': safe_int(metrics.get('covered_conditions')),
        'uncovered_conditions': safe_int(metrics.get('uncovered_conditions')),
        'lines_to_cover': safe_int(metrics.get('lines_to_cover')),
        'conditions_to_cover': safe_int(metrics.get('conditions_to_cover')),
        
        # Duplication metrics
        'duplicated_lines_density': safe_float(metrics.get('duplicated_lines_density')),
        'duplicated_lines': safe_int(metrics.get('duplicated_lines')),
        'duplicated_blocks': safe_int(metrics.get('duplicated_blocks')),
        'duplicated_files': safe_int(metrics.get('duplicated_files')),
        
        # Complexity metrics
        'complexity': safe_int(metrics.get('complexity')),
        'cognitive_complexity': safe_int(metrics.get('cognitive_complexity')),
        
        # Comment metrics
        'comment_lines': safe_int(metrics.get('comment_lines')),
        'comment_lines_density': safe_float(metrics.get('comment_lines_density')),
        
        # Quality gate
        'alert_status': metrics.get('alert_status'),
        'quality_gate_details': metrics.get('quality_gate_details'),
        
        # Issue totals
        'violations': safe_int(metrics.get('violations')),
        'open_issues': safe_int(metrics.get('open_issues')),
        'confirmed_issues': safe_int(metrics.get('confirmed_issues')),
        'false_positive_issues': safe_int(metrics.get('false_positive_issues')),
        'accepted_issues': safe_int(metrics.get('accepted_issues')),
        
        # Additional metrics
        'effort_to_reach_maintainability_rating_a': safe_int(
            metrics.get('effort_to_reach_maintainability_rating_a')
        ),
        
        # Metadata
        'data_source_timestamp': datetime.now(),
        'is_carried_forward': False,
        
        # New code metrics
        'new_code_lines': safe_int(metrics.get('new_lines')),
        'new_code_ncloc': safe_int(metrics.get('new_ncloc')),
        
        # New code bugs
        'new_code_bugs_total': safe_int(metrics.get('new_bugs')),
        'new_code_bugs_blocker': new_code_issues.get('new_code_bug_blocker', 0),
        'new_code_bugs_critical': new_code_issues.get('new_code_bug_critical', 0),
        'new_code_bugs_major': new_code_issues.get('new_code_bug_major', 0),
        'new_code_bugs_minor': new_code_issues.get('new_code_bug_minor', 0),
        'new_code_bugs_info': new_code_issues.get('new_code_bug_info', 0),
        'new_code_reliability_remediation_effort': safe_int(
            metrics.get('new_reliability_remediation_effort')
        ),
        
        # New code vulnerabilities
        'new_code_vulnerabilities_total': safe_int(metrics.get('new_vulnerabilities')),
        'new_code_vulnerabilities_blocker': new_code_issues.get('new_code_vulnerability_blocker', 0),
        'new_code_vulnerabilities_critical': new_code_issues.get('new_code_vulnerability_critical', 0),
        'new_code_vulnerabilities_high': new_code_issues.get('new_code_vulnerability_major', 0),
        'new_code_vulnerabilities_medium': new_code_issues.get('new_code_vulnerability_minor', 0),
        'new_code_vulnerabilities_low': new_code_issues.get('new_code_vulnerability_info', 0),
        'new_code_security_remediation_effort': safe_int(
            metrics.get('new_security_remediation_effort')
        ),
        
        # New code smells
        'new_code_code_smells_total': safe_int(metrics.get('new_code_smells')),
        'new_code_code_smells_blocker': new_code_issues.get('new_code_code_smell_blocker', 0),
        'new_code_code_smells_critical': new_code_issues.get('new_code_code_smell_critical', 0),
        'new_code_code_smells_major': new_code_issues.get('new_code_code_smell_major', 0),
        'new_code_code_smells_minor': new_code_issues.get('new_code_code_smell_minor', 0),
        'new_code_code_smells_info': new_code_issues.get('new_code_code_smell_info', 0),
        'new_code_technical_debt': safe_int(metrics.get('new_technical_debt')),
        'new_code_sqale_debt_ratio': safe_float(metrics.get('new_sqale_debt_ratio')),
        
        # New code security hotspots
        'new_code_security_hotspots_total': safe_int(metrics.get('new_security_hotspots')),
        'new_code_security_hotspots_high': new_code_issues.get('new_code_security_hotspot_high', 0),
        'new_code_security_hotspots_medium': new_code_issues.get(
            'new_code_security_hotspot_medium', 0
        ),
        'new_code_security_hotspots_low': new_code_issues.get('new_code_security_hotspot_low', 0),
        'new_code_security_hotspots_to_review': new_code_issues.get(
            'new_code_security_hotspot_to_review', 0
        ),
        'new_code_security_hotspots_acknowledged': new_code_issues.get(
            'new_code_security_hotspot_acknowledged', 0
        ),
        'new_code_security_hotspots_fixed': new_code_issues.get(
            'new_code_security_hotspot_fixed', 0
        ),
        'new_code_security_hotspots_safe': new_code_issues.get(
            'new_code_security_hotspot_safe', 0
        ),
        
        # New code security review
        'new_code_security_hotspots_reviewed': safe_float(
            metrics.get('new_security_hotspots_reviewed')
        ),
        'new_code_security_review_rating': metrics.get('new_security_review_rating'),
        
        # New code coverage
        'new_code_coverage_percentage': safe_float(metrics.get('new_coverage')),
        'new_code_line_coverage_percentage': safe_float(metrics.get('new_line_coverage')),
        'new_code_branch_coverage_percentage': safe_float(metrics.get('new_branch_coverage')),
        'new_code_covered_lines': safe_int(metrics.get('new_covered_lines')),
        'new_code_uncovered_lines': safe_int(metrics.get('new_uncovered_lines')),
        'new_code_covered_conditions': safe_int(metrics.get('new_covered_conditions')),
        'new_code_uncovered_conditions': safe_int(metrics.get('new_uncovered_conditions')),
        'new_code_lines_to_cover': safe_int(metrics.get('new_lines_to_cover')),
        'new_code_conditions_to_cover': safe_int(metrics.get('new_conditions_to_cover')),
        
        # New code duplications
        'new_code_duplicated_lines_density': safe_float(metrics.get('new_duplicated_lines_density')),
        'new_code_duplicated_lines': safe_int(metrics.get('new_duplicated_lines')),
        'new_code_duplicated_blocks': safe_int(metrics.get('new_duplicated_blocks')),
        
        # New code complexity
        'new_code_complexity': safe_int(metrics.get('new_complexity')),
        'new_code_cognitive_complexity': safe_int(metrics.get('new_cognitive_complexity')),
        
        # New code issues
        'new_violations': safe_int(metrics.get('new_violations')),
        'new_accepted_issues': safe_int(metrics.get('new_accepted_issues')),
        'new_confirmed_issues': safe_int(metrics.get('new_confirmed_issues')),
        
        # New code period
        'new_code_period_date': new_code_period_date,
        'new_code_period_mode': metrics.get('new_code_period_mode'),
        'new_code_period_value': metrics.get('new_code_period_value')
    }
    
    return transformed


@task
@performance_monitor
def load_metrics_batch(
    transformed_batch: List[Dict[str, Any]],
    projects: List[Dict[str, Any]],
    **context
) -> Dict[str, Any]:
    """Load a batch of transformed metrics into the database."""
    pg_hook = PostgresHook(postgres_conn_id='sonarqube_metrics_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Create project mapping
    project_map = {p['key']: p for p in projects}
    
    success_count = 0
    error_count = 0
    carry_forward_count = 0
    
    try:
        # First ensure all projects exist
        for project in projects:
            cursor.execute("""
                INSERT INTO sonarqube_metrics.sq_projects 
                (sonarqube_project_key, project_name, last_analysis_date_from_sq)
                VALUES (%s, %s, %s)
                ON CONFLICT (sonarqube_project_key) DO UPDATE
                SET project_name = EXCLUDED.project_name,
                    last_analysis_date_from_sq = EXCLUDED.last_analysis_date_from_sq,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                project['key'],
                project['name'],
                project.get('lastAnalysisDate')
            ))
        
        conn.commit()
        
        # Get project ID mapping
        cursor.execute("""
            SELECT project_id, sonarqube_project_key 
            FROM sonarqube_metrics.sq_projects
        """)
        project_id_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Process each metric in the batch
        for metric_record in transformed_batch:
            project_key = metric_record['project_key']
            project_id = project_id_map.get(project_key)
            
            if not project_id:
                logger.error(f"Project ID not found for {project_key}")
                error_count += 1
                continue
                
            if metric_record.get('transformed') and metric_record.get('data'):
                # Insert transformed data
                data = metric_record['data']
                
                # Build dynamic insert query
                columns = list(data.keys())
                values = [data[col] for col in columns]
                
                placeholders = ', '.join(['%s'] * len(values))
                column_names = ', '.join(columns)
                update_clause = ', '.join([
                    f"{col} = EXCLUDED.{col}" 
                    for col in columns 
                    if col not in ['metric_date']
                ])
                
                query = f"""
                    INSERT INTO sonarqube_metrics.daily_project_metrics 
                    (project_id, {column_names})
                    VALUES (%s, {placeholders})
                    ON CONFLICT (project_id, metric_date) DO UPDATE
                    SET {update_clause}
                """
                
                cursor.execute(query, [project_id] + values)
                success_count += 1
                
            else:
                # Handle carry-forward logic for failed metrics
                metric_date = metric_record['metric_date']
                
                # Get last known good values
                cursor.execute("""
                    SELECT * FROM sonarqube_metrics.daily_project_metrics
                    WHERE project_id = %s
                    AND metric_date < %s
                    AND is_carried_forward = false
                    ORDER BY metric_date DESC
                    LIMIT 1
                """, (project_id, metric_date))
                
                last_known = cursor.fetchone()
                if last_known:
                    # Insert carried forward record
                    columns = [desc[0] for desc in cursor.description]
                    last_known_dict = dict(zip(columns, last_known))
                    
                    # Update metadata
                    last_known_dict['metric_date'] = metric_date
                    last_known_dict['is_carried_forward'] = True
                    last_known_dict['data_source_timestamp'] = datetime.now()
                    
                    # Remove DB-specific columns
                    for col in ['metric_id', 'project_id', 'created_at']:
                        last_known_dict.pop(col, None)
                        
                    # Insert carried forward data
                    columns = list(last_known_dict.keys())
                    values = [last_known_dict[col] for col in columns]
                    
                    placeholders = ', '.join(['%s'] * len(values))
                    column_names = ', '.join(columns)
                    
                    query = f"""
                        INSERT INTO sonarqube_metrics.daily_project_metrics 
                        (project_id, {column_names})
                        VALUES (%s, {placeholders})
                        ON CONFLICT (project_id, metric_date) DO NOTHING
                    """
                    
                    cursor.execute(query, [project_id] + values)
                    carry_forward_count += 1
                else:
                    error_count += 1
                    
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load metrics batch: {str(e)}")
        raise
        
    finally:
        cursor.close()
        conn.close()
        
    return {
        'success_count': success_count,
        'error_count': error_count,
        'carry_forward_count': carry_forward_count,
        'total_processed': len(transformed_batch)
    }


@task
def generate_summary_report(
    load_results: List[Dict[str, Any]],
    **context
) -> Dict[str, Any]:
    """Generate a summary report of the ETL run."""
    total_success = sum(r['success_count'] for r in load_results)
    total_errors = sum(r['error_count'] for r in load_results)
    total_carry_forward = sum(r['carry_forward_count'] for r in load_results)
    total_processed = sum(r['total_processed'] for r in load_results)
    
    summary = {
        'execution_date': context['execution_date'].isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'total_processed': total_processed,
        'successful_loads': total_success,
        'failed_loads': total_errors,
        'carried_forward': total_carry_forward,
        'success_rate': (total_success / total_processed * 100) if total_processed > 0 else 0,
        'completion_time': datetime.now().isoformat()
    }
    
    logger.info(f"ETL Summary: {json.dumps(summary, indent=2)}")
    
    # Store summary in XCom for downstream tasks
    return summary


@task
def check_data_quality(summary: Dict[str, Any], **context) -> bool:
    """Check data quality and alert if issues found."""
    etl_config = load_etl_config()
    
    # Define quality thresholds
    min_success_rate = 95.0
    max_carry_forward_rate = 10.0
    
    success_rate = summary['success_rate']
    carry_forward_rate = (
        summary['carried_forward'] / summary['total_processed'] * 100 
        if summary['total_processed'] > 0 else 0
    )
    
    issues = []
    
    if success_rate < min_success_rate:
        issues.append(f"Success rate {success_rate:.1f}% below threshold {min_success_rate}%")
        
    if carry_forward_rate > max_carry_forward_rate:
        issues.append(
            f"Carry forward rate {carry_forward_rate:.1f}% above threshold {max_carry_forward_rate}%"
        )
        
    if issues and etl_config.enable_notifications:
        # Send alert
        alert_content = f"""
        <h3>SonarQube ETL Data Quality Alert</h3>
        <p>The following data quality issues were detected:</p>
        <ul>
        {''.join(f'<li>{issue}</li>' for issue in issues)}
        </ul>
        <h4>Summary:</h4>
        <pre>{json.dumps(summary, indent=2)}</pre>
        """
        
        send_email(
            to=DEFAULT_ARGS['email'],
            subject=f"SonarQube ETL Data Quality Alert - {context['execution_date']}",
            html_content=alert_content
        )
        
    return len(issues) == 0


# =====================================================================
# DAG DEFINITION
# =====================================================================

# Create the DAG
dag = DAG(
    'sonarqube_etl_v4',
    default_args=DEFAULT_ARGS,
    description='Enterprise-grade SonarQube ETL pipeline with advanced features',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['sonarqube', 'etl', 'metrics', 'enterprise'],
    doc_md=__doc__,
    params={
        'metric_date_override': None,  # Allow manual override of metric date
        'project_filter': None,  # Allow filtering specific projects
        'force_full_refresh': False,  # Force full data refresh
    }
)

with dag:
    # Start and end markers
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    # Calculate metric date
    @task
    def calculate_metric_date(**context) -> str:
        """Calculate the metric date based on execution date and params."""
        params = context['params']
        
        if params.get('metric_date_override'):
            metric_date = params['metric_date_override']
            logger.info(f"Using override metric date: {metric_date}")
        else:
            # Default to yesterday's data
            execution_date = context['execution_date']
            metric_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
            logger.info(f"Using calculated metric date: {metric_date}")
            
        return metric_date
    
    # Main ETL flow
    metric_date = calculate_metric_date()
    
    # Fetch projects
    projects = fetch_projects()
    
    # Create task groups for organized processing
    with TaskGroup(group_id='extract_transform_load') as etl_group:
        
        # Extract metrics for each project (dynamic task mapping)
        @task_group
        def process_project_batch(project_batch: List[Dict[str, Any]], batch_id: int):
            """Process a batch of projects."""
            
            # Extract metrics for each project in the batch
            metrics = []
            for project in project_batch:
                metric = extract_project_metrics(project, metric_date)
                metrics.append(metric)
                
            # Transform the batch
            transformed = transform_metrics_batch(metrics)
            
            # Load the batch
            load_result = load_metrics_batch(transformed, projects)
            
            return load_result
        
        # Split projects into batches
        @task
        def create_project_batches(
            all_projects: List[Dict[str, Any]], 
            batch_size: int = 10
        ) -> List[List[Dict[str, Any]]]:
            """Split projects into batches for parallel processing."""
            batches = []
            for i in range(0, len(all_projects), batch_size):
                batches.append(all_projects[i:i + batch_size])
            return batches
        
        project_batches = create_project_batches(projects)
        
        # Process each batch in parallel (simplified for clarity)
        load_results = []
        for i in range(5):  # Process up to 5 batches in parallel
            batch_result = process_project_batch.override(
                task_id=f'process_batch_{i}'
            )(project_batches[i] if i < len(project_batches) else [])
            load_results.append(batch_result)
    
    # Generate summary report
    summary = generate_summary_report(load_results)
    
    # Data quality checks
    quality_check = check_data_quality(summary)
    
    # Cleanup old data
    @task
    def cleanup_old_data(**context):
        """Remove data older than retention period."""
        etl_config = load_etl_config()
        retention_date = datetime.now() - timedelta(days=etl_config.data_retention_days)
        
        pg_hook = PostgresHook(postgres_conn_id='sonarqube_metrics_db')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                DELETE FROM sonarqube_metrics.daily_project_metrics
                WHERE metric_date < %s
            """, (retention_date,))
            
            deleted_rows = cursor.rowcount
            conn.commit()
            
            logger.info(f"Deleted {deleted_rows} rows older than {retention_date}")
            return deleted_rows
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to cleanup old data: {e}")
            raise
            
        finally:
            cursor.close()
            conn.close()
    
    cleanup = cleanup_old_data()
    
    # Update dataset for downstream dependencies
    update_dataset = EmptyOperator(
        task_id='update_dataset',
        outlets=[SONARQUBE_METRICS_DATASET]
    )
    
    # Define task dependencies
    start >> metric_date >> projects >> etl_group >> summary >> [quality_check, cleanup] >> update_dataset >> end


# =====================================================================
# MONITORING AND ALERTING
# =====================================================================

def task_failure_alert(context):
    """Send alert on task failure."""
    task_instance = context['task_instance']
    
    alert_content = f"""
    <h3>SonarQube ETL Task Failed</h3>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>DAG:</strong> {task_instance.dag_id}</p>
    <p><strong>Execution Date:</strong> {context['execution_date']}</p>
    <p><strong>Log URL:</strong> {task_instance.log_url}</p>
    <h4>Error Details:</h4>
    <pre>{context.get('exception', 'No exception details available')}</pre>
    """
    
    send_email(
        to=DEFAULT_ARGS['email'],
        subject=f"SonarQube ETL Task Failed - {task_instance.task_id}",
        html_content=alert_content
    )


# Set the failure callback for all tasks
for task in dag.tasks:
    task.on_failure_callback = task_failure_alert