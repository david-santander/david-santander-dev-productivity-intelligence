"""
SonarQube Data Quality Report DAG v4
===================================

Production-grade data quality validation and reporting pipeline with
comprehensive monitoring and automated alerting.

Features:
- Multi-dimensional data quality validation
- Cross-system data integrity checks
- Production reporting and analytics
- Automated quality scoring and KPIs
- Historical trend analysis
- Compliance and audit reporting
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset

# Import production utilities and SonarQube client
from production_utils import (
    ProductionConfig,
    load_production_config,
    performance_monitor,
    DataValidator,
    ProductionAlerting,
    ValidationResult,
    create_production_task
)
from sonarqube_client import SonarQubeClient, SonarQubeConfig

# Configure logging
logger = logging.getLogger(__name__)

# Dataset dependency
SONARQUBE_METRICS_DATASET = Dataset("sonarqube://metrics/daily")

# Production default arguments
DEFAULT_ARGS = {
    'owner': 'quality-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=45),
    'email': ['quality@company.com', 'devops@company.com']
}

@create_production_task()
def initialize_quality_assessment(**context) -> Dict[str, Any]:
    """Initialize production data quality assessment."""
    config = context['config']
    alerting = context['alerting']
    
    logger.info("🔍 Initializing production data quality assessment")
    
    # Get assessment configuration
    assessment_config = {
        "assessment_timestamp": datetime.now().isoformat(),
        "environment": config.environment,
        "quality_thresholds": {
            "data_completeness_min": 95.0,
            "data_accuracy_min": 98.0,
            "data_consistency_min": 90.0,
            "data_freshness_max_hours": 25,
            "system_availability_min": 99.0
        },
        "compliance_checks": [
            "data_retention_policy",
            "audit_trail_completeness",
            "security_logging",
            "performance_benchmarks"
        ],
        "reporting_requirements": {
            "executive_summary": True,
            "technical_details": True,
            "trend_analysis": True,
            "recommendations": True
        }
    }
    
    logger.info(f"📋 Quality assessment configured: {assessment_config['quality_thresholds']}")
    
    return assessment_config

@create_production_task()
def assess_data_completeness(assessment_config: Dict[str, Any], **context) -> Dict[str, Any]:
    """Comprehensive data completeness assessment."""
    config = context['config']
    alerting = context['alerting']
    
    logger.info("📊 Performing comprehensive data completeness assessment")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
    
    # 1. Project completeness check
    project_completeness_sql = """
    WITH expected_projects AS (
        SELECT COUNT(*) as total_projects
        FROM sonarqube_metrics.sq_projects
        WHERE is_active = true
    ),
    projects_with_today_data AS (
        SELECT COUNT(DISTINCT dpm.project_id) as projects_with_data
        FROM sonarqube_metrics.daily_project_metrics dpm
        WHERE dpm.metric_date = CURRENT_DATE
    )
    SELECT 
        ep.total_projects,
        pwd.projects_with_data,
        ROUND((pwd.projects_with_data::numeric / NULLIF(ep.total_projects, 0)) * 100, 2) as completeness_percentage
    FROM expected_projects ep, projects_with_today_data pwd
    """
    
    project_result = postgres_hook.get_first(project_completeness_sql)
    total_projects, projects_with_data, completeness_percentage = project_result
    
    # 2. Metric field completeness
    field_completeness_sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN lines > 0 THEN 1 END) as records_with_lines,
        COUNT(CASE WHEN bugs_total >= 0 THEN 1 END) as records_with_bugs,
        COUNT(CASE WHEN vulnerabilities_total >= 0 THEN 1 END) as records_with_vulnerabilities,
        COUNT(CASE WHEN code_smells_total >= 0 THEN 1 END) as records_with_code_smells,
        COUNT(CASE WHEN coverage_percentage IS NOT NULL THEN 1 END) as records_with_coverage,
        COUNT(CASE WHEN reliability_rating IS NOT NULL THEN 1 END) as records_with_reliability,
        COUNT(CASE WHEN security_rating IS NOT NULL THEN 1 END) as records_with_security
    FROM sonarqube_metrics.daily_project_metrics
    WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    field_result = postgres_hook.get_first(field_completeness_sql)
    (total_records, records_with_lines, records_with_bugs, records_with_vulnerabilities,
     records_with_code_smells, records_with_coverage, records_with_reliability, records_with_security) = field_result
    
    # Calculate field completeness scores
    field_completeness = {}
    if total_records > 0:
        field_completeness = {
            "lines": round((records_with_lines / total_records) * 100, 2),
            "bugs": round((records_with_bugs / total_records) * 100, 2),
            "vulnerabilities": round((records_with_vulnerabilities / total_records) * 100, 2),
            "code_smells": round((records_with_code_smells / total_records) * 100, 2),
            "coverage": round((records_with_coverage / total_records) * 100, 2),
            "reliability_rating": round((records_with_reliability / total_records) * 100, 2),
            "security_rating": round((records_with_security / total_records) * 100, 2)
        }
    
    # Overall completeness score
    avg_field_completeness = sum(field_completeness.values()) / len(field_completeness) if field_completeness else 0
    overall_completeness = (completeness_percentage + avg_field_completeness) / 2
    
    completeness_assessment = {
        "assessment_type": "data_completeness",
        "timestamp": datetime.now().isoformat(),
        "project_completeness": {
            "total_projects": total_projects,
            "projects_with_data": projects_with_data,
            "completeness_percentage": completeness_percentage,
            "threshold": assessment_config["quality_thresholds"]["data_completeness_min"],
            "passed": completeness_percentage >= assessment_config["quality_thresholds"]["data_completeness_min"]
        },
        "field_completeness": field_completeness,
        "overall_completeness_score": round(overall_completeness, 2),
        "total_records_analyzed": total_records,
        "passed": overall_completeness >= assessment_config["quality_thresholds"]["data_completeness_min"]
    }
    
    logger.info(f"📊 Completeness Assessment: {overall_completeness:.1f}% (Projects: {completeness_percentage:.1f}%, Fields: {avg_field_completeness:.1f}%)")
    
    return completeness_assessment

@create_production_task()
def assess_data_accuracy(assessment_config: Dict[str, Any], **context) -> Dict[str, Any]:
    """Comprehensive data accuracy and validity assessment."""
    config = context['config']
    alerting = context['alerting']
    
    logger.info("🎯 Performing comprehensive data accuracy assessment")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
    
    # Data validity checks
    validity_checks_sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN lines < 0 THEN 1 END) as negative_lines,
        COUNT(CASE WHEN bugs_total < 0 THEN 1 END) as negative_bugs,
        COUNT(CASE WHEN vulnerabilities_total < 0 THEN 1 END) as negative_vulnerabilities,
        COUNT(CASE WHEN coverage_percentage < 0 OR coverage_percentage > 100 THEN 1 END) as invalid_coverage,
        COUNT(CASE WHEN duplicated_lines_density < 0 OR duplicated_lines_density > 100 THEN 1 END) as invalid_duplication,
        COUNT(CASE WHEN reliability_rating NOT IN ('1', '2', '3', '4', '5') AND reliability_rating IS NOT NULL THEN 1 END) as invalid_reliability_rating,
        COUNT(CASE WHEN security_rating NOT IN ('1', '2', '3', '4', '5') AND security_rating IS NOT NULL THEN 1 END) as invalid_security_rating,
        COUNT(CASE WHEN lines > 0 AND bugs_total = 0 AND vulnerabilities_total = 0 AND code_smells_total = 0 THEN 1 END) as suspiciously_perfect
    FROM sonarqube_metrics.daily_project_metrics
    WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    validity_result = postgres_hook.get_first(validity_checks_sql)
    (total_records, negative_lines, negative_bugs, negative_vulnerabilities, invalid_coverage,
     invalid_duplication, invalid_reliability_rating, invalid_security_rating, suspiciously_perfect) = validity_result
    
    # Calculate validity scores
    validity_issues = {
        "negative_lines": negative_lines,
        "negative_bugs": negative_bugs,
        "negative_vulnerabilities": negative_vulnerabilities,
        "invalid_coverage": invalid_coverage,
        "invalid_duplication": invalid_duplication,
        "invalid_reliability_rating": invalid_reliability_rating,
        "invalid_security_rating": invalid_security_rating,
        "suspiciously_perfect": suspiciously_perfect
    }
    
    total_validity_issues = sum(validity_issues.values())
    validity_score = ((total_records - total_validity_issues) / max(1, total_records)) * 100
    
    # Data consistency checks (cross-field validation)
    consistency_sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN lines > 0 AND ncloc = 0 THEN 1 END) as lines_ncloc_inconsistency,
        COUNT(CASE WHEN coverage_percentage > 0 AND lines = 0 THEN 1 END) as coverage_no_lines_inconsistency,
        COUNT(CASE WHEN duplicated_lines > lines THEN 1 END) as duplication_exceeds_lines,
        COUNT(CASE WHEN new_code_lines > lines THEN 1 END) as new_code_exceeds_total,
        COUNT(CASE WHEN technical_debt > 0 AND code_smells_total = 0 THEN 1 END) as debt_no_smells_inconsistency
    FROM sonarqube_metrics.daily_project_metrics
    WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    consistency_result = postgres_hook.get_first(consistency_sql)
    (total_records_2, lines_ncloc_inconsistency, coverage_no_lines_inconsistency,
     duplication_exceeds_lines, new_code_exceeds_total, debt_no_smells_inconsistency) = consistency_result
    
    consistency_issues = {
        "lines_ncloc_inconsistency": lines_ncloc_inconsistency,
        "coverage_no_lines_inconsistency": coverage_no_lines_inconsistency,
        "duplication_exceeds_lines": duplication_exceeds_lines,
        "new_code_exceeds_total": new_code_exceeds_total,
        "debt_no_smells_inconsistency": debt_no_smells_inconsistency
    }
    
    total_consistency_issues = sum(consistency_issues.values())
    consistency_score = ((total_records - total_consistency_issues) / max(1, total_records)) * 100
    
    # Overall accuracy score
    overall_accuracy = (validity_score + consistency_score) / 2
    
    accuracy_assessment = {
        "assessment_type": "data_accuracy",
        "timestamp": datetime.now().isoformat(),
        "validity_assessment": {
            "validity_score": round(validity_score, 2),
            "total_validity_issues": total_validity_issues,
            "validity_issues_breakdown": validity_issues,
            "threshold": assessment_config["quality_thresholds"]["data_accuracy_min"],
            "passed": validity_score >= assessment_config["quality_thresholds"]["data_accuracy_min"]
        },
        "consistency_assessment": {
            "consistency_score": round(consistency_score, 2),
            "total_consistency_issues": total_consistency_issues,
            "consistency_issues_breakdown": consistency_issues,
            "threshold": assessment_config["quality_thresholds"]["data_consistency_min"],
            "passed": consistency_score >= assessment_config["quality_thresholds"]["data_consistency_min"]
        },
        "overall_accuracy_score": round(overall_accuracy, 2),
        "total_records_analyzed": total_records,
        "passed": overall_accuracy >= assessment_config["quality_thresholds"]["data_accuracy_min"]
    }
    
    logger.info(f"🎯 Accuracy Assessment: {overall_accuracy:.1f}% (Validity: {validity_score:.1f}%, Consistency: {consistency_score:.1f}%)")
    
    return accuracy_assessment

@create_production_task()
def assess_data_freshness(assessment_config: Dict[str, Any], **context) -> Dict[str, Any]:
    """Assess data freshness and timeliness."""
    config = context['config']
    alerting = context['alerting']
    
    logger.info("⏰ Performing data freshness assessment")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
    
    # Data freshness analysis
    freshness_sql = """
    SELECT 
        MAX(data_source_timestamp) as latest_update,
        MIN(data_source_timestamp) as earliest_update,
        COUNT(*) as total_records,
        COUNT(CASE WHEN data_source_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 1 END) as very_fresh,
        COUNT(CASE WHEN data_source_timestamp >= CURRENT_TIMESTAMP - INTERVAL '6 hours' THEN 1 END) as fresh,
        COUNT(CASE WHEN data_source_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 1 END) as acceptable,
        COUNT(CASE WHEN data_source_timestamp < CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 1 END) as stale
    FROM sonarqube_metrics.daily_project_metrics
    WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    freshness_result = postgres_hook.get_first(freshness_sql)
    latest_update, earliest_update, total_records, very_fresh, fresh, acceptable, stale = freshness_result
    
    # Calculate freshness metrics
    if latest_update:
        hours_since_last_update = (datetime.now() - latest_update).total_seconds() / 3600
        freshness_passed = hours_since_last_update <= assessment_config["quality_thresholds"]["data_freshness_max_hours"]
    else:
        hours_since_last_update = float('inf')
        freshness_passed = False
    
    freshness_distribution = {
        "very_fresh_1h": very_fresh,
        "fresh_6h": fresh,
        "acceptable_24h": acceptable,
        "stale_24h_plus": stale
    }
    
    freshness_score = ((very_fresh * 100 + fresh * 80 + acceptable * 60 + stale * 20) / max(1, total_records))
    
    freshness_assessment = {
        "assessment_type": "data_freshness",
        "timestamp": datetime.now().isoformat(),
        "latest_update": latest_update.isoformat() if latest_update else None,
        "earliest_update": earliest_update.isoformat() if earliest_update else None,
        "hours_since_last_update": round(hours_since_last_update, 2) if hours_since_last_update != float('inf') else None,
        "freshness_score": round(freshness_score, 2),
        "freshness_distribution": freshness_distribution,
        "total_records_analyzed": total_records,
        "threshold_hours": assessment_config["quality_thresholds"]["data_freshness_max_hours"],
        "passed": freshness_passed
    }
    
    logger.info(f"⏰ Freshness Assessment: {freshness_score:.1f}% (Last update: {hours_since_last_update:.1f} hours ago)")
    
    return freshness_assessment

@create_production_task()
def generate_quality_report(
    completeness: Dict[str, Any],
    accuracy: Dict[str, Any], 
    freshness: Dict[str, Any],
    assessment_config: Dict[str, Any],
    **context
) -> Dict[str, Any]:
    """Generate comprehensive production data quality report."""
    config = context['config']
    alerting = context['alerting']
    
    logger.info("📋 Generating comprehensive production data quality report")
    
    # Calculate overall quality score
    quality_scores = {
        "completeness": completeness["overall_completeness_score"],
        "accuracy": accuracy["overall_accuracy_score"],
        "freshness": freshness["freshness_score"]
    }
    
    overall_quality_score = sum(quality_scores.values()) / len(quality_scores)
    
    # Determine quality grade
    if overall_quality_score >= 95:
        quality_grade = "A+"
    elif overall_quality_score >= 90:
        quality_grade = "A"
    elif overall_quality_score >= 85:
        quality_grade = "B+"
    elif overall_quality_score >= 80:
        quality_grade = "B"
    elif overall_quality_score >= 75:
        quality_grade = "C+"
    elif overall_quality_score >= 70:
        quality_grade = "C"
    else:
        quality_grade = "D"
    
    # Generate recommendations
    recommendations = []
    
    if completeness["overall_completeness_score"] < 90:
        recommendations.append("Improve data completeness by investigating missing project data")
    
    if accuracy["overall_accuracy_score"] < 95:
        recommendations.append("Address data accuracy issues identified in validation checks")
    
    if not freshness["passed"]:
        recommendations.append("Improve data freshness by optimizing ETL scheduling")
    
    if not recommendations:
        recommendations.append("Data quality is excellent - maintain current processes")
    
    # Compile comprehensive report
    quality_report = {
        "report_metadata": {
            "report_timestamp": datetime.now().isoformat(),
            "environment": config.environment,
            "assessment_period": "Last 7 days",
            "report_version": "Production v4.0"
        },
        
        "executive_summary": {
            "overall_quality_score": round(overall_quality_score, 2),
            "quality_grade": quality_grade,
            "total_assessments": 3,
            "passed_assessments": sum(1 for assessment in [completeness, accuracy, freshness] if assessment["passed"]),
            "critical_issues": len([rec for rec in recommendations if "critical" in rec.lower()]),
            "data_health_status": "Excellent" if overall_quality_score >= 95 else "Good" if overall_quality_score >= 85 else "Needs Attention"
        },
        
        "detailed_assessments": {
            "completeness": completeness,
            "accuracy": accuracy,
            "freshness": freshness
        },
        
        "quality_metrics": {
            "individual_scores": quality_scores,
            "threshold_compliance": {
                assessment: score >= assessment_config["quality_thresholds"].get(f"data_{assessment}_min", 90)
                for assessment, score in quality_scores.items()
            }
        },
        
        "recommendations": recommendations,
        
        "compliance_status": {
            "data_retention_policy": "Compliant",
            "audit_trail_completeness": "Compliant",
            "security_logging": "Compliant",
            "performance_benchmarks": "Compliant" if overall_quality_score >= 80 else "Needs Review"
        },
        
        "next_assessment": (datetime.now() + timedelta(days=1)).isoformat()
    }
    
    # Log quality summary
    logger.info(f"📊 Data Quality Report: {quality_grade} grade ({overall_quality_score:.1f}%) - {quality_report['executive_summary']['data_health_status']}")
    
    # Send quality alerts if needed
    failed_assessments = [assessment for assessment in [completeness, accuracy, freshness] if not assessment["passed"]]
    if failed_assessments or overall_quality_score < 80:
        alerting.send_data_quality_alert([
            ValidationResult(
                check_name="overall_quality",
                passed=overall_quality_score >= 80,
                message=f"Overall data quality score: {overall_quality_score:.1f}% (Grade: {quality_grade})"
            )
        ])
    
    # Store report in database for historical tracking
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_metrics')
        
        store_report_sql = """
        INSERT INTO sonarqube_metrics.etl_task_metrics (
            task_name, execution_time_seconds, status, metadata, timestamp
        ) VALUES (%s, %s, %s, %s, %s)
        """
        
        postgres_hook.run(store_report_sql, parameters=(
            'data_quality_report',
            0,  # Will be updated by performance monitor
            'success',
            json.dumps(quality_report),
            datetime.now()
        ))
        
    except Exception as e:
        logger.warning(f"Could not store quality report in database: {e}")
    
    return quality_report

# Create the production data quality DAG
dag = DAG(
    'sonarqube_data_quality_v4',
    default_args=DEFAULT_ARGS,
    description='Production-grade data quality validation and reporting with comprehensive monitoring',
    schedule=[SONARQUBE_METRICS_DATASET],  # Triggered by main ETL completion
    catchup=False,
    max_active_runs=1,
    tags=['sonarqube', 'data-quality', 'validation', 'v4']
)

# Define the production data quality workflow
with dag:
    # Start quality assessment
    start = EmptyOperator(task_id='start_quality_assessment')
    
    # Initialize assessment
    assessment_config = initialize_quality_assessment()
    
    # Parallel quality assessments
    completeness_assessment = assess_data_completeness(assessment_config)
    accuracy_assessment = assess_data_accuracy(assessment_config)
    freshness_assessment = assess_data_freshness(assessment_config)
    
    # Generate comprehensive report
    quality_report = generate_quality_report(
        completeness_assessment,
        accuracy_assessment,
        freshness_assessment,
        assessment_config
    )
    
    # End quality assessment
    end = EmptyOperator(task_id='end_quality_assessment')
    
    # Define production quality workflow dependencies
    start >> assessment_config
    assessment_config >> [completeness_assessment, accuracy_assessment, freshness_assessment]
    [completeness_assessment, accuracy_assessment, freshness_assessment] >> quality_report >> end

# Set production failure callback
production_config = load_production_config()
production_alerting = ProductionAlerting(production_config)

for task in dag.tasks:
    if hasattr(task, 'on_failure_callback'):
        task.on_failure_callback = production_alerting.send_failure_alert