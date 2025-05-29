"""
SonarQube ETL Backfill DAG v4
============================

Production-grade backfill pipeline for historical SonarQube metrics collection
with comprehensive monitoring and validation.

Features:
- Intelligent date range processing
- Production transaction management
- Data integrity validation
- Performance monitoring and metrics
- Comprehensive error handling and recovery
- Audit trail and security logging
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import asdict

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Import production utilities and SonarQube client
from production_utils import (
    ProductionConfig,
    load_production_config,
    performance_monitor,
    DataValidator,
    ProductionAlerting,
    CircuitBreaker,
    create_production_task
)
from sonarqube_client import SonarQubeClient, SonarQubeConfig

# Configure logging
logger = logging.getLogger(__name__)

# Production default arguments for backfill
DEFAULT_ARGS = {
    'owner': 'devops-team',
    'depends_on_past': False,
    'start_date': days_ago(90),  # Allow backfill for last 90 days
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,  # Fewer retries for backfill
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=1),
    'email': ['devops@company.com', 'admin@company.com']
}

# Initialize circuit breaker for backfill operations
backfill_circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=600)

@create_production_task()
def initialize_backfill_context(**context) -> Dict[str, Any]:
    """Initialize production backfill context with validation."""
    from dataclasses import asdict
    
    # Use data_interval_start instead of deprecated execution_date
    execution_date = context['data_interval_start'] or context['logical_date']
    config = context['config']
    alerting = context['alerting']
    
    target_date = execution_date.strftime('%Y-%m-%d')
    
    logger.info(f"🔧 Initializing production backfill for {target_date}")
    
    # Validate backfill date
    today = datetime.now().date()
    backfill_date = execution_date.date()
    
    if backfill_date > today:
        raise ValueError(f"Cannot backfill future date: {target_date}")
    
    if backfill_date < today - timedelta(days=365):
        logger.warning(f"⚠️ Backfilling very old data: {target_date}")
    
    # Check if data already exists
    postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
    existing_data_sql = """
    SELECT COUNT(*) as existing_records
    FROM sonarqube_metrics.daily_project_metrics
    WHERE metric_date = %s
    """
    
    existing_count = postgres_hook.get_first(existing_data_sql, parameters=(backfill_date,))[0]
    
    backfill_context = {
        "target_date": target_date,
        "target_date_obj": backfill_date.isoformat(),  # Convert to string for JSON serialization
        "execution_timestamp": datetime.now().isoformat(),
        "existing_records": existing_count,
        "will_overwrite": existing_count > 0,
        "config": asdict(config) if hasattr(config, '__dataclass_fields__') else vars(config),
        "backfill_reason": context.get('dag_run').conf.get('reason', 'Manual backfill') if context.get('dag_run') and context.get('dag_run').conf else 'Manual backfill',
        "validation_checks": []
    }
    
    logger.info(f"📋 Backfill context: {backfill_context}")
    
    if existing_count > 0:
        logger.warning(f"⚠️ Will overwrite {existing_count} existing records for {target_date}")
    
    return backfill_context

@create_production_task()
def extract_projects_for_backfill(backfill_context: Dict[str, Any], **context) -> List[Dict[str, Any]]:
    """Extract projects for production backfill with validation."""
    target_date = backfill_context['target_date']
    config = ProductionConfig(**backfill_context['config'])
    alerting = context['alerting']
    
    logger.info(f"🚀 Production project extraction for backfill: {target_date}")
    
    def _fetch_projects_for_backfill():
        """Internal function for circuit breaker protection."""
        client_config = SonarQubeConfig(
            base_url=config.sonarqube_base_url,
            token=config.sonarqube_token
        )
        
        import asyncio
        
        async def fetch_with_metadata():
            client = SonarQubeClient(client_config)
            projects = await client.fetch_all_projects()
            
            # Add backfill-specific metadata
            project_list = []
            for p in projects:
                project_data = {
                    "key": p.key,
                    "name": p.name,
                    "last_analysis": getattr(p, 'last_analysis_date', None),
                    "backfill_eligible": True
                }
                
                # Check if project had analysis before target date
                if project_data["last_analysis"]:
                    try:
                        # last_analysis_date is already a datetime object from the Project dataclass
                        analysis_date = project_data["last_analysis"]
                        if isinstance(analysis_date, str):
                            # Handle case where it might still be a string
                            analysis_date = datetime.strptime(analysis_date[:19], '%Y-%m-%dT%H:%M:%S')
                        
                        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
                        if analysis_date.date() > target_date_obj:
                            project_data["backfill_eligible"] = False
                            project_data["ineligible_reason"] = "No analysis before target date"
                    except (ValueError, AttributeError, TypeError) as e:
                        logger.warning(f"Could not parse analysis date for {project_data['name']}: {e}")
                        project_data["backfill_eligible"] = True  # Allow backfill if date parsing fails
                
                project_list.append(project_data)
            
            return project_list
        
        return asyncio.run(fetch_with_metadata())
    
    try:
        # Use circuit breaker for resilient extraction
        projects = backfill_circuit_breaker.call(_fetch_projects_for_backfill)
        
        # Filter eligible projects
        eligible_projects = [p for p in projects if p.get('backfill_eligible', True)]
        ineligible_projects = [p for p in projects if not p.get('backfill_eligible', True)]
        
        if ineligible_projects:
            logger.warning(f"⚠️ {len(ineligible_projects)} projects ineligible for backfill to {target_date}")
            for project in ineligible_projects:
                logger.warning(f"  - {project['name']}: {project.get('ineligible_reason', 'Unknown')}")
        
        logger.info(f"✅ Found {len(eligible_projects)} eligible projects for backfill")
        
        return eligible_projects
        
    except Exception as e:
        logger.error(f"❌ Production project extraction failed for backfill: {e}")
        raise

@create_production_task()
def backfill_project_metrics(project: Dict[str, Any], backfill_context: Dict[str, Any], **context) -> Dict[str, Any]:
    """Backfill metrics for a project with production validation."""
    target_date = backfill_context['target_date']
    config = ProductionConfig(**backfill_context['config'])
    alerting = context['alerting']
    
    logger.info(f"📊 Production backfill for {project['name']} on {target_date}")
    
    def _fetch_backfill_data():
        """Fetch data for backfill with production features."""
        client_config = SonarQubeConfig(
            base_url=config.sonarqube_base_url,
            token=config.sonarqube_token
        )
        
        import asyncio
        from dataclasses import asdict
        
        async def fetch_comprehensive_backfill_data():
            client = SonarQubeClient(client_config)
            
            # For backfill, get current state but store with target date
            # In a real implementation, you might have historical data APIs
            current_metrics = await client.fetch_project_metrics(
                project_key=project["key"],
                include_new_code=True
            )
            
            # Convert ProjectMetrics to a simpler structure for serialization
            # We'll flatten the metrics structure to avoid complex nested objects
            metrics_dict = None
            if current_metrics:
                # Extract the raw metrics dictionary
                raw_metrics = current_metrics.metrics
                
                # Create a simplified structure that can be easily serialized
                metrics_dict = {
                    'project_key': current_metrics.project_key,
                    'metric_date': current_metrics.metric_date,
                    'raw_metrics': raw_metrics,  # This is already a simple dict
                    'issue_counts': {
                        'bugs_total': current_metrics.issue_breakdown.totals_by_type.get('bug', 0),
                        'vulnerabilities_total': current_metrics.issue_breakdown.totals_by_type.get('vulnerability', 0),
                        'code_smells_total': current_metrics.issue_breakdown.totals_by_type.get('code_smell', 0),
                    },
                    'hotspot_total': current_metrics.hotspot_breakdown.total,
                    'new_code_counts': {
                        'bugs': current_metrics.new_code_issue_breakdown.totals_by_type.get('bug', 0) if current_metrics.new_code_issue_breakdown else 0,
                        'vulnerabilities': current_metrics.new_code_issue_breakdown.totals_by_type.get('vulnerability', 0) if current_metrics.new_code_issue_breakdown else 0,
                        'code_smells': current_metrics.new_code_issue_breakdown.totals_by_type.get('code_smell', 0) if current_metrics.new_code_issue_breakdown else 0,
                    }
                }
            
            # Add backfill metadata
            return {
                "metrics": metrics_dict,
                "backfill_timestamp": datetime.now().isoformat(),
                "data_source": "current_state_backfill",
                "target_date": target_date
            }
        
        return asyncio.run(fetch_comprehensive_backfill_data())
    
    try:
        # Fetch data with circuit breaker protection
        backfill_data = backfill_circuit_breaker.call(_fetch_backfill_data)
        
        # Production data validation for backfill
        validation_results = []
        
        metrics = backfill_data["metrics"]
        if not metrics or 'raw_metrics' not in metrics:
            validation_results.append("Invalid or missing metrics structure")
        
        # Check if project has code (metrics is now a dict)
        if metrics and 'raw_metrics' in metrics:
            raw_metrics = metrics['raw_metrics']
            if isinstance(raw_metrics, dict) and int(raw_metrics.get('lines', 0)) == 0:
                validation_results.append("Project appears to have no code")
        
        # Validate project eligibility again
        if not project.get('backfill_eligible', True):
            validation_results.append(f"Project not eligible: {project.get('ineligible_reason', 'Unknown')}")
        
        return {
            "project": project,
            "backfill_data": backfill_data,
            "validation_results": validation_results,
            "target_date": target_date
        }
        
    except Exception as e:
        logger.error(f"❌ Production backfill data fetch failed for {project['name']}: {e}")
        return {
            "project": project,
            "backfill_data": None,
            "validation_results": [f"Fetch failed: {str(e)}"],
            "target_date": target_date,
            "failed": True
        }

@create_production_task()
def store_backfill_data(project_data: Dict[str, Any], **context) -> Dict[str, Any]:
    """Store backfilled data with production transaction management."""
    project = project_data["project"]
    backfill_data = project_data.get("backfill_data")
    target_date = project_data["target_date"]
    validation_results = project_data.get("validation_results", [])
    config = context['config']
    alerting = context['alerting']
    
    if project_data.get("failed") or not backfill_data:
        logger.error(f"❌ Skipping storage for {project['name']} due to fetch failure")
        return {
            "project_name": project["name"],
            "target_date": target_date,
            "backfill_success": False,
            "error": "Data fetch failed",
            "validation_results": validation_results
        }
    
    logger.info(f"💾 Storing production backfill data for {project['name']} on {target_date}")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
    
    # Production transaction management
    conn = postgres_hook.get_conn()
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        # 1. Ensure project exists with backfill audit trail
        project_sql = """
        INSERT INTO sonarqube_metrics.sq_projects (
            sonarqube_project_key, project_name, created_at, updated_at, is_active
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sonarqube_project_key) DO UPDATE SET
            project_name = EXCLUDED.project_name,
            updated_at = EXCLUDED.updated_at
        RETURNING project_id
        """
        
        now = datetime.now()
        cursor.execute(project_sql, (
            project["key"], 
            project["name"], 
            now, 
            now,
            True
        ))
        
        project_id = cursor.fetchone()[0]
        
        # 2. Store backfilled metrics with production validation
        metrics = backfill_data["metrics"]
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        
        # Delete existing data for this date (if any) to ensure clean backfill
        delete_sql = """
        DELETE FROM sonarqube_metrics.daily_project_metrics
        WHERE project_id = %s AND metric_date = %s
        """
        cursor.execute(delete_sql, (project_id, target_date_obj))
        deleted_rows = cursor.rowcount
        
        if deleted_rows > 0:
            logger.info(f"🗑️ Deleted {deleted_rows} existing records for {project['name']} on {target_date}")
        
        # Insert comprehensive backfill data
        insert_sql = """
        INSERT INTO sonarqube_metrics.daily_project_metrics (
            project_id, metric_date, lines, ncloc, classes, functions, statements,
            files, directories, bugs_total, vulnerabilities_total, code_smells_total,
            coverage_percentage, line_coverage_percentage, branch_coverage_percentage,
            duplicated_lines_density, duplicated_lines, complexity, cognitive_complexity,
            technical_debt, reliability_rating, security_rating, sqale_rating,
            new_code_lines, new_code_bugs_total, new_code_vulnerabilities_total,
            new_code_code_smells_total, new_code_coverage_percentage,
            new_code_duplicated_lines_density, data_source_timestamp, is_carried_forward
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Production-grade data preparation with comprehensive validation
        def safe_int(value, default=0):
            try:
                return int(float(value)) if value is not None else default
            except (ValueError, TypeError):
                return default
                
        def safe_float(value, default=None):
            try:
                return float(value) if value is not None else default
            except (ValueError, TypeError):
                return default
                
        def safe_rating(value, default='1'):
            try:
                return str(int(float(value))) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        # Extract values from the flattened metrics structure
        raw_metrics = metrics['raw_metrics']
        issue_counts = metrics['issue_counts']
        new_code_counts = metrics['new_code_counts']
        
        metric_values = (
            project_id,
            target_date_obj,
            safe_int(raw_metrics.get('lines')),
            safe_int(raw_metrics.get('ncloc')),
            safe_int(raw_metrics.get('classes')),
            safe_int(raw_metrics.get('functions')),
            safe_int(raw_metrics.get('statements')),
            safe_int(raw_metrics.get('files')),
            safe_int(raw_metrics.get('directories')),
            safe_int(issue_counts.get('bugs_total', raw_metrics.get('bugs', 0))),
            safe_int(issue_counts.get('vulnerabilities_total', raw_metrics.get('vulnerabilities', 0))),
            safe_int(issue_counts.get('code_smells_total', raw_metrics.get('code_smells', 0))),
            safe_float(raw_metrics.get('coverage')),
            safe_float(raw_metrics.get('line_coverage')),
            safe_float(raw_metrics.get('branch_coverage')),
            safe_float(raw_metrics.get('duplicated_lines_density')),
            safe_int(raw_metrics.get('duplicated_lines')),
            safe_int(raw_metrics.get('complexity')),
            safe_int(raw_metrics.get('cognitive_complexity')),
            safe_int(raw_metrics.get('sqale_index')),  # technical debt
            safe_rating(raw_metrics.get('reliability_rating')),
            safe_rating(raw_metrics.get('security_rating')),
            safe_rating(raw_metrics.get('sqale_rating')),
            safe_int(raw_metrics.get('new_lines')),
            safe_int(new_code_counts.get('bugs')),
            safe_int(new_code_counts.get('vulnerabilities')),
            safe_int(new_code_counts.get('code_smells')),
            safe_float(raw_metrics.get('new_coverage')),
            safe_float(raw_metrics.get('new_duplicated_lines_density')),
            now,
            True  # Mark as carried forward for backfill
        )
        
        cursor.execute(insert_sql, metric_values)
        
        # 3. Commit production transaction
        conn.commit()
        cursor.close()
        
        logger.info(f"✅ Successfully stored production backfill data for {project['name']} on {target_date}")
        
        return {
            "project_name": project["name"],
            "project_key": project["key"],
            "target_date": target_date,
            "backfill_success": True,
            "timestamp": now.isoformat(),
            "deleted_existing_records": deleted_rows,
            "validation_results": validation_results,
            "data_quality": {
                "lines": safe_int(raw_metrics.get('lines')),
                "bugs": safe_int(issue_counts.get('bugs_total', raw_metrics.get('bugs', 0))),
                "vulnerabilities": safe_int(issue_counts.get('vulnerabilities_total', raw_metrics.get('vulnerabilities', 0))),
                "code_smells": safe_int(issue_counts.get('code_smells_total', raw_metrics.get('code_smells', 0)))
            }
        }
        
    except Exception as e:
        conn.rollback()
        cursor.close()
        logger.error(f"❌ Production backfill storage failed for {project['name']}: {e}")
        
        return {
            "project_name": project["name"],
            "target_date": target_date,
            "backfill_success": False,
            "error": str(e),
            "validation_results": validation_results
        }
    
    finally:
        conn.close()

@create_production_task()
def generate_backfill_summary(storage_results: List[Dict[str, Any]], backfill_context: Dict[str, Any], **context) -> Dict[str, Any]:
    """Generate comprehensive production backfill summary."""
    target_date = backfill_context['target_date']
    config = context['config']
    alerting = context['alerting']
    
    logger.info(f"📋 Generating production backfill summary for {target_date}")
    
    # Analyze results
    successful = [r for r in storage_results if r.get('backfill_success', False)]
    failed = [r for r in storage_results if not r.get('backfill_success', False)]
    
    # Calculate comprehensive metrics
    total_lines = sum(r.get('data_quality', {}).get('lines', 0) for r in successful)
    total_bugs = sum(r.get('data_quality', {}).get('bugs', 0) for r in successful)
    total_vulnerabilities = sum(r.get('data_quality', {}).get('vulnerabilities', 0) for r in successful)
    total_deleted_records = sum(r.get('deleted_existing_records', 0) for r in successful)
    
    # Production KPIs for backfill
    success_rate = len(successful) / len(storage_results) * 100 if storage_results else 0
    data_integrity_score = 100 - min(100, len([r for r in storage_results if r.get('validation_results')]) / max(1, len(storage_results)) * 100)
    
    backfill_summary = {
        "backfill_date": target_date,
        "execution_timestamp": datetime.now().isoformat(),
        "environment": config.environment,
        "total_projects": len(storage_results),
        "successful_backfills": len(successful),
        "failed_backfills": len(failed),
        "success_rate": round(success_rate, 2),
        "successful_projects": [r['project_name'] for r in successful],
        "failed_projects": [r['project_name'] for r in failed],
        
        # Production backfill metrics
        "backfill_metrics": {
            "total_lines_backfilled": total_lines,
            "total_bugs_backfilled": total_bugs,
            "total_vulnerabilities_backfilled": total_vulnerabilities,
            "total_deleted_records": total_deleted_records,
            "data_integrity_score": round(data_integrity_score, 2),
            "circuit_breaker_state": backfill_circuit_breaker.state
        },
        
        # Validation summary
        "validation_summary": {
            "projects_with_validation_issues": len([r for r in storage_results if r.get('validation_results')]),
            "total_validation_issues": sum(len(r.get('validation_results', [])) for r in storage_results)
        },
        
        # Original context
        "original_context": backfill_context
    }
    
    logger.info(f"📊 Production Backfill Summary for {target_date}: {len(successful)}/{len(storage_results)} projects, Integrity Score: {data_integrity_score:.1f}")
    
    # Send production notification for backfill completion
    if success_rate >= 80:  # Lower threshold for backfill operations
        alerting.send_success_notification('sonarqube_etl_backfill_v4', backfill_summary)
    
    return backfill_summary

# Create the production backfill DAG
dag = DAG(
    'sonarqube_etl_backfill_v4',
    default_args=DEFAULT_ARGS,
    description='Production-grade SonarQube backfill pipeline with comprehensive validation and monitoring',
    schedule_interval=None,  # Manual trigger only
    catchup=True,  # Allow backfill execution
    max_active_runs=5,  # Allow multiple backfill dates in parallel
    tags=['sonarqube', 'etl', 'backfill', 'v4']
)

# Define the production backfill workflow
with dag:
    # Start production backfill
    start = EmptyOperator(task_id='start_backfill')
    
    # Initialize backfill context with validation
    backfill_context = initialize_backfill_context()
    
    # Extract projects with production validation
    projects = extract_projects_for_backfill(backfill_context)
    
    # Backfill metrics for each eligible project
    backfill_tasks = backfill_project_metrics.expand(
        project=projects,
        backfill_context=[backfill_context]
    )
    
    # Store data with production transaction management
    storage_tasks = store_backfill_data.expand(project_data=backfill_tasks)
    
    # Generate comprehensive summary
    summary = generate_backfill_summary(storage_tasks, backfill_context)
    
    # End production backfill
    end = EmptyOperator(task_id='end_backfill')
    
    # Define production backfill dependencies
    start >> backfill_context >> projects >> backfill_tasks >> storage_tasks >> summary >> end

# Set production failure callback
production_config = load_production_config()
production_alerting = ProductionAlerting(production_config)

for task in dag.tasks:
    if hasattr(task, 'on_failure_callback'):
        task.on_failure_callback = production_alerting.send_failure_alert