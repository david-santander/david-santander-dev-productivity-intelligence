"""
Production Utilities for SonarQube ETL DAGs
==========================================

Production-grade utilities for monitoring, alerting, security, and data validation.
"""

import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from functools import wraps
from dataclasses import dataclass, asdict
import hashlib
import os

from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

# Configure logging
logger = logging.getLogger(__name__)

# =====================================================================
# PRODUCTION CONFIGURATION
# =====================================================================

@dataclass
class ProductionConfig:
    """Production configuration for ETL operations."""
    environment: str = "production"
    batch_size: int = 10
    parallel_workers: int = 4
    max_retries: int = 3
    retry_delay_minutes: int = 5
    data_retention_days: int = 365
    enable_notifications: bool = True
    enable_data_validation: bool = True
    enable_performance_monitoring: bool = True
    enable_security_logging: bool = True
    alert_thresholds: Dict[str, Any] = None
    notification_channels: List[str] = None
    sonarqube_base_url: str = ""
    sonarqube_token: str = ""
    
    def __post_init__(self):
        if self.alert_thresholds is None:
            self.alert_thresholds = {
                "max_execution_time_minutes": 60,
                "min_success_rate_percent": 95,
                "max_error_count": 5,
                "data_freshness_hours": 25
            }
        
        if self.notification_channels is None:
            self.notification_channels = ["email"]

def load_production_config() -> ProductionConfig:
    """Load production configuration from Airflow Variables."""
    try:
        # Try to load from JSON config first
        config_dict = Variable.get("sonarqube_etl_config", deserialize_json=True, default_var=None)
        
        if config_dict:
            # Merge with production defaults
            production_config = ProductionConfig()
            for key, value in config_dict.items():
                if hasattr(production_config, key):
                    setattr(production_config, key, value)
        else:
            # Fall back to individual environment variables
            production_config = ProductionConfig()
            
            # Load SonarQube configuration from individual variables
            sonarqube_base_url = Variable.get("SONARQUBE_BASE_URL", default_var=None)
            if sonarqube_base_url:
                production_config.sonarqube_base_url = sonarqube_base_url
                
            sonarqube_token = Variable.get("SONARQUBE_TOKEN", default_var=None)
            if sonarqube_token:
                production_config.sonarqube_token = sonarqube_token
        
        logger.info(f"🔧 Production configuration loaded: {production_config.environment}")
        logger.info(f"📡 SonarQube URL: {production_config.sonarqube_base_url}")
        return production_config
        
    except Exception as e:
        logger.warning(f"Failed to load production config, using defaults: {e}")
        # Try to load individual variables as fallback
        production_config = ProductionConfig()
        
        try:
            sonarqube_base_url = Variable.get("SONARQUBE_BASE_URL", default_var=None)
            if sonarqube_base_url:
                production_config.sonarqube_base_url = sonarqube_base_url
                
            sonarqube_token = Variable.get("SONARQUBE_TOKEN", default_var=None)
            if sonarqube_token:
                production_config.sonarqube_token = sonarqube_token
                
            logger.info(f"📡 Loaded individual variables - URL: {production_config.sonarqube_base_url}")
        except Exception as ve:
            logger.error(f"Failed to load individual variables: {ve}")
            
        return production_config

# =====================================================================
# PERFORMANCE MONITORING
# =====================================================================

def performance_monitor(func: Callable) -> Callable:
    """Decorator for monitoring task performance."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        task_name = func.__name__
        
        logger.info(f"🚀 Starting task: {task_name}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Log performance metrics
            performance_data = {
                "task_name": task_name,
                "execution_time_seconds": round(execution_time, 2),
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "memory_usage_mb": _get_memory_usage()
            }
            
            _log_performance_metrics(performance_data)
            logger.info(f"✅ Task completed: {task_name} in {execution_time:.2f}s")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # Log error metrics
            error_data = {
                "task_name": task_name,
                "execution_time_seconds": round(execution_time, 2),
                "status": "error",
                "error_message": str(e),
                "timestamp": datetime.now().isoformat()
            }
            
            _log_performance_metrics(error_data)
            logger.error(f"❌ Task failed: {task_name} after {execution_time:.2f}s - {e}")
            
            raise
    
    return wrapper

def _get_memory_usage() -> float:
    """Get current memory usage in MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except ImportError:
        return 0.0

def _log_performance_metrics(metrics: Dict[str, Any]):
    """Log performance metrics to database."""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
        
        insert_sql = """
        INSERT INTO sonarqube_metrics.etl_task_metrics (
            task_name, execution_time_seconds, status, error_message,
            memory_usage_mb, timestamp, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        postgres_hook.run(
            insert_sql,
            parameters=(
                metrics.get("task_name"),
                metrics.get("execution_time_seconds"),
                metrics.get("status"),
                metrics.get("error_message"),
                metrics.get("memory_usage_mb"),
                datetime.now(),
                json.dumps(metrics)
            )
        )
        
    except Exception as e:
        logger.warning(f"Failed to log performance metrics: {e}")

# =====================================================================
# DATA VALIDATION FRAMEWORK
# =====================================================================

@dataclass
class ValidationResult:
    """Result of data validation check."""
    check_name: str
    passed: bool
    message: str
    data: Dict[str, Any] = None
    severity: str = "error"  # error, warning, info

class DataValidator:
    """Production data validation framework."""
    
    def __init__(self):
        self.postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
        self.validation_results: List[ValidationResult] = []
    
    def validate_data_completeness(self, project_count: int) -> ValidationResult:
        """Validate that all expected projects have data."""
        sql = """
        SELECT COUNT(DISTINCT project_id) as projects_with_data
        FROM sonarqube_metrics.daily_project_metrics
        WHERE metric_date = CURRENT_DATE
        """
        
        result = self.postgres_hook.get_first(sql)
        projects_with_data = result[0] if result else 0
        
        passed = projects_with_data >= project_count
        message = f"Expected {project_count} projects, found {projects_with_data} with data"
        
        return ValidationResult(
            check_name="data_completeness",
            passed=passed,
            message=message,
            data={"expected": project_count, "actual": projects_with_data}
        )
    
    def validate_data_freshness(self, max_age_hours: int = 25) -> ValidationResult:
        """Validate that data is fresh."""
        sql = """
        SELECT MAX(data_source_timestamp) as latest_update
        FROM sonarqube_metrics.daily_project_metrics
        WHERE metric_date >= CURRENT_DATE - INTERVAL '1 day'
        """
        
        result = self.postgres_hook.get_first(sql)
        latest_update = result[0] if result else None
        
        if latest_update:
            age_hours = (datetime.now() - latest_update).total_seconds() / 3600
            passed = age_hours <= max_age_hours
            message = f"Data age: {age_hours:.1f} hours (max: {max_age_hours})"
        else:
            passed = False
            age_hours = float('inf')
            message = "No recent data found"
        
        return ValidationResult(
            check_name="data_freshness",
            passed=passed,
            message=message,
            data={"age_hours": age_hours, "max_age_hours": max_age_hours}
        )
    
    def validate_data_quality(self) -> ValidationResult:
        """Validate data quality metrics."""
        sql = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN lines > 0 THEN 1 END) as records_with_lines,
            COUNT(CASE WHEN bugs_total >= 0 THEN 1 END) as records_with_bugs,
            AVG(CASE WHEN lines > 0 THEN lines END) as avg_lines
        FROM sonarqube_metrics.daily_project_metrics
        WHERE metric_date >= CURRENT_DATE - INTERVAL '1 day'
        """
        
        result = self.postgres_hook.get_first(sql)
        total_records, records_with_lines, records_with_bugs, avg_lines = result
        
        quality_score = (records_with_lines / total_records * 100) if total_records > 0 else 0
        passed = quality_score >= 90
        
        message = f"Data quality score: {quality_score:.1f}% ({records_with_lines}/{total_records} records with valid data)"
        
        return ValidationResult(
            check_name="data_quality",
            passed=passed,
            message=message,
            data={
                "quality_score": quality_score,
                "total_records": total_records,
                "records_with_lines": records_with_lines,
                "avg_lines": avg_lines
            }
        )
    
    def run_all_validations(self, project_count: int) -> List[ValidationResult]:
        """Run all validation checks."""
        logger.info("🔍 Running production data validation checks")
        
        validations = [
            self.validate_data_completeness(project_count),
            self.validate_data_freshness(),
            self.validate_data_quality()
        ]
        
        self.validation_results.extend(validations)
        
        passed_count = sum(1 for v in validations if v.passed)
        logger.info(f"📊 Validation results: {passed_count}/{len(validations)} checks passed")
        
        return validations

# =====================================================================
# PRODUCTION ALERTING
# =====================================================================

class ProductionAlerting:
    """Production alerting and notification system."""
    
    def __init__(self, config: ProductionConfig):
        self.config = config
    
    def send_success_notification(self, dag_id: str, execution_data: Dict[str, Any]):
        """Send success notification."""
        if not self.config.enable_notifications:
            return
        
        subject = f"✅ SonarQube ETL Success - {dag_id}"
        
        content = f"""
        <h3>✅ SonarQube ETL Pipeline Completed Successfully</h3>
        
        <h4>📊 Execution Summary:</h4>
        <ul>
            <li><strong>DAG:</strong> {dag_id}</li>
            <li><strong>Environment:</strong> {self.config.environment}</li>
            <li><strong>Execution Time:</strong> {execution_data.get('execution_time', 'N/A')}</li>
            <li><strong>Projects Processed:</strong> {execution_data.get('projects_processed', 'N/A')}</li>
            <li><strong>Success Rate:</strong> {execution_data.get('success_rate', 'N/A')}%</li>
        </ul>
        
        <h4>🎯 Key Metrics:</h4>
        <ul>
            <li><strong>Total Records:</strong> {execution_data.get('total_records', 'N/A')}</li>
            <li><strong>Data Quality Score:</strong> {execution_data.get('quality_score', 'N/A')}%</li>
            <li><strong>Validation Status:</strong> {execution_data.get('validation_status', 'N/A')}</li>
        </ul>
        
        <p><strong>Timestamp:</strong> {datetime.now().isoformat()}</p>
        <p><em>🏗️ Production SonarQube ETL Platform</em></p>
        """
        
        self._send_email_notification(subject, content)
    
    def send_failure_alert(self, context: Dict[str, Any]):
        """Send comprehensive failure alert."""
        if not self.config.enable_notifications:
            return
        
        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date']
        
        subject = f"🚨 CRITICAL: SonarQube ETL Failure - {dag_id}"
        
        content = f"""
        <h3>🚨 CRITICAL: SonarQube ETL Pipeline Failure</h3>
        
        <h4>📋 Failure Details:</h4>
        <ul>
            <li><strong>DAG:</strong> {dag_id}</li>
            <li><strong>Failed Task:</strong> {task_id}</li>
            <li><strong>Environment:</strong> {self.config.environment}</li>
            <li><strong>Execution Date:</strong> {execution_date}</li>
            <li><strong>Failure Time:</strong> {datetime.now().isoformat()}</li>
        </ul>
        
        <h4>🔍 Error Information:</h4>
        <pre>{context.get('exception', 'No exception details available')}</pre>
        
        <h4>🛠️ Troubleshooting:</h4>
        <ul>
            <li><strong>Log URL:</strong> <a href="{task_instance.log_url}">View Logs</a></li>
            <li><strong>Retry Count:</strong> {task_instance.try_number}/{task_instance.max_tries}</li>
            <li><strong>Next Retry:</strong> {task_instance.next_retry_datetime or 'No more retries'}</li>
        </ul>
        
        <h4>⚡ Immediate Actions Required:</h4>
        <ol>
            <li>Check SonarQube API connectivity</li>
            <li>Verify database connections</li>
            <li>Review task logs for specific errors</li>
            <li>Check system resources and performance</li>
        </ol>
        
        <p><strong>⚠️ This is an automated alert from the Production SonarQube ETL Platform</strong></p>
        """
        
        self._send_email_notification(subject, content)
        
        # Log security event
        if self.config.enable_security_logging:
            self._log_security_event("etl_failure", {
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": str(execution_date),
                "error": str(context.get('exception', ''))
            })
    
    def send_data_quality_alert(self, validation_results: List[ValidationResult]):
        """Send data quality alert."""
        failed_checks = [v for v in validation_results if not v.passed]
        
        if not failed_checks:
            return
        
        subject = f"⚠️ Data Quality Alert - {len(failed_checks)} checks failed"
        
        content = f"""
        <h3>⚠️ Data Quality Alert</h3>
        
        <p><strong>{len(failed_checks)} out of {len(validation_results)} validation checks failed</strong></p>
        
        <h4>❌ Failed Checks:</h4>
        <ul>
        """
        
        for check in failed_checks:
            content += f"<li><strong>{check.check_name}:</strong> {check.message}</li>"
        
        content += """
        </ul>
        
        <h4>✅ Passed Checks:</h4>
        <ul>
        """
        
        passed_checks = [v for v in validation_results if v.passed]
        for check in passed_checks:
            content += f"<li><strong>{check.check_name}:</strong> {check.message}</li>"
        
        content += f"""
        </ul>
        
        <p><strong>Timestamp:</strong> {datetime.now().isoformat()}</p>
        <p><em>🔍 Production Data Quality Monitoring</em></p>
        """
        
        self._send_email_notification(subject, content)
    
    def _send_email_notification(self, subject: str, content: str):
        """Send email notification."""
        try:
            # In production, this would use proper email configuration
            logger.info(f"📧 Email notification: {subject}")
            logger.info(f"📄 Content: {content[:200]}...")
            
            # Uncomment when email is configured
            # send_email(
            #     to=['admin@company.com', 'devops@company.com'],
            #     subject=subject,
            #     html_content=content
            # )
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
    
    def _log_security_event(self, event_type: str, event_data: Dict[str, Any]):
        """Log security events for audit trail."""
        try:
            postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
            
            security_log = {
                "event_type": event_type,
                "event_data": event_data,
                "timestamp": datetime.now().isoformat(),
                "user": "airflow_system",
                "environment": self.config.environment
            }
            
            # In production, this would go to a dedicated security log table
            logger.info(f"🔒 Security event logged: {event_type} - {json.dumps(security_log)}")
            
        except Exception as e:
            logger.error(f"Failed to log security event: {e}")

# =====================================================================
# CIRCUIT BREAKER PATTERN
# =====================================================================

class CircuitBreaker:
    """Circuit breaker for external API calls."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
            else:
                raise AirflowException("Circuit breaker is OPEN - API calls blocked")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt reset."""
        if self.last_failure_time is None:
            return True
        
        return (time.time() - self.last_failure_time) >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful call."""
        self.failure_count = 0
        self.state = "CLOSED"
    
    def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"🔴 Circuit breaker opened after {self.failure_count} failures")

# =====================================================================
# PRODUCTION TASK FACTORY
# =====================================================================

def create_production_task(**task_kwargs):
    """Decorator factory to create production-grade tasks with monitoring and error handling."""
    
    def decorator(func: Callable) -> Callable:
        @performance_monitor
        @wraps(func)
        def production_wrapper(*args, **kwargs):
            config = load_production_config()
            alerting = ProductionAlerting(config)
            
            try:
                # Add production context to kwargs
                kwargs['config'] = config
                kwargs['alerting'] = alerting
                
                return func(*args, **kwargs)
                
            except Exception as e:
                # Production error handling
                logger.error(f"Production task failed: {func.__name__} - {e}")
                raise
        
        # Set production task defaults
        production_task_kwargs = {
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(hours=2),
            'on_failure_callback': lambda context: ProductionAlerting(load_production_config()).send_failure_alert(context),
            **task_kwargs
        }
        
        # Apply task decorator with production settings
        from airflow.decorators import task
        return task(**production_task_kwargs)(production_wrapper)
    
    return decorator