from typing import Dict, Any
import pytz
from datetime import datetime

METRIC_FIELDS = []

def generate_markdown_report(comparison_results: Dict[str, Any]) -> str:
    """Generate a Markdown formatted comparison report.
    
    Args:
        comparison_results: Comparison data structure
        
    Returns:
        Markdown formatted report string
    """
    report_time = datetime.now(pytz.UTC)
    
    md_lines = [
        f"# SonarQube Metrics Comparison Report",
        f"",
        f"**Generated:** {report_time.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"**Execution Date:** {comparison_results['execution_date']}",
        f"**DAG Run ID:** {comparison_results['dag_run_id']}",
        f"",
        f"## Health Status",
        f"",
    ]
    
    # Add health status section
    health_status = comparison_results.get('health_status', {})
    overall_status = health_status.get('overall_status', 'unknown').upper()
    status_emoji = {'HEALTHY': 'PASS', 'WARNING': 'WARN', 'CRITICAL': 'FAIL', 'UNKNOWN': '????'}.get(overall_status, '????')
    
    md_lines.extend([
        f"### Overall Status: {status_emoji} {overall_status}",
        f"",
    ])
    
    # Critical Issues
    if health_status.get('critical_issues'):
        md_lines.extend([
            f"#### Critical Issues",
            f"",
        ])
        for issue in health_status['critical_issues']:
            md_lines.append(f"- {issue}")
        md_lines.append("")
    
    # Warnings
    if health_status.get('warnings'):
        md_lines.extend([
            f"#### Warnings",
            f"",
        ])
        for warning in health_status['warnings']:
            md_lines.append(f"- {warning}")
        md_lines.append("")
    
    # Health Metrics Table
    md_lines.extend([
        f"### Health Metrics",
        f"",
        f"| Category | Metric | Value | Status |",
        f"|----------|--------|-------|--------|",
    ])
    
    # Data Freshness
    freshness = health_status.get('data_freshness', {})
    if freshness:
        md_lines.extend([
            f"| Data Freshness | Latest PostgreSQL Date | {freshness.get('postgres_latest_date', 'N/A')} | {freshness.get('postgres_status', 'N/A')} |",
            f"| Data Freshness | Data Age (days) | {freshness.get('postgres_data_age_days', 'N/A')} | - |",
            f"| Data Freshness | Carried Forward Projects | {freshness.get('carried_forward_projects', 0)} ({freshness.get('carried_forward_percentage', 0)}%) | - |",
        ])
    
    # Data Completeness
    completeness = health_status.get('data_completeness', {})
    if completeness:
        md_lines.extend([
            f"| Data Completeness | Total Unique Projects | {completeness.get('total_unique_projects', 0)} | - |",
            f"| Data Completeness | Missing in PostgreSQL | {completeness.get('missing_in_postgres_count', 0)} | - |",
            f"| Data Completeness | Missing in SonarQube | {completeness.get('missing_in_sonarqube_count', 0)} | - |",
            f"| Data Completeness | Completeness % | {completeness.get('completeness_percentage', 0)}% | - |",
        ])
    
    # Sync Status
    sync_status = health_status.get('sync_status', {})
    if sync_status:
        md_lines.extend([
            f"| Sync Status | Projects in Sync | {sync_status.get('projects_in_sync', 0)} | - |",
            f"| Sync Status | Projects with Discrepancies | {sync_status.get('projects_with_discrepancies', 0)} | - |",
            f"| Sync Status | Total Discrepancies | {sync_status.get('total_discrepancies', 0)} | - |",
            f"| Sync Status | Sync Percentage | {sync_status.get('sync_percentage', 0)}% | - |",
        ])
    
    md_lines.extend([
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
    
    # Add Data Quality Metrics
    md_lines.extend([
        f"",
        f"## Data Quality Metrics",
        f"",
        f"### Metric Coverage Analysis",
        f"",
        f"| Metric Type | Total Fields | Fields with Data | Coverage % |",
        f"|-------------|--------------|------------------|------------|",
    ])
    
    # Calculate metric coverage
    metric_coverage = {}
    for project_data in comparison_results['projects'].values():
        if 'metrics' in project_data:
            for metric, data in project_data['metrics'].items():
                if metric not in metric_coverage:
                    metric_coverage[metric] = {'total': 0, 'non_zero': 0}
                metric_coverage[metric]['total'] += 1
                if data['postgres_value'] != 0 or data['sonarqube_value'] != 0:
                    metric_coverage[metric]['non_zero'] += 1
    
    # Group metrics by type
    metric_types = {
        'Bugs': [m for m in METRIC_FIELDS if m.startswith('bugs_')],
        'Vulnerabilities': [m for m in METRIC_FIELDS if m.startswith('vulnerabilities_')],
        'Code Smells': [m for m in METRIC_FIELDS if m.startswith('code_smells_')],
        'Security Hotspots': [m for m in METRIC_FIELDS if m.startswith('security_hotspots_')],
        'Coverage': [m for m in METRIC_FIELDS if 'coverage' in m],
        'Duplication': [m for m in METRIC_FIELDS if 'duplicated' in m],
        'New Code': [m for m in METRIC_FIELDS if m.startswith('new_code_')]
    }
    
    for type_name, metrics in metric_types.items():
        total_fields = len(metrics)
        fields_with_data = sum(1 for m in metrics if m in metric_coverage and metric_coverage[m]['non_zero'] > 0)
        coverage_pct = round(fields_with_data / total_fields * 100, 1) if total_fields > 0 else 0
        md_lines.append(f"| {type_name} | {total_fields} | {fields_with_data} | {coverage_pct}% |")
    
    # Add Actionable Insights
    md_lines.extend([
        f"",
        f"## Actionable Insights",
        f"",
    ])
    
    insights = []
    
    # Check for critical metrics
    critical_metrics = ['bugs_blocker', 'vulnerabilities_critical', 'security_hotspots_high']
    for project_key, project_data in comparison_results['projects'].items():
        if 'metrics' in project_data:
            for metric in critical_metrics:
                if metric in project_data['metrics']:
                    value = project_data['metrics'][metric]['sonarqube_value']
                    if value > 0:
                        metric_name = metric.replace('_', ' ').title()
                        insights.append(f"- **{project_key}**: {value} {metric_name} require immediate attention")
    
    # Check for poor coverage
    for project_key, project_data in comparison_results['projects'].items():
        if 'metrics' in project_data:
            coverage = project_data['metrics'].get('coverage_percentage', {}).get('sonarqube_value', 0)
            if coverage < 50 and coverage > 0:
                insights.append(f"- **{project_key}**: Code coverage is only {coverage}% - consider adding more tests")
    
    # Check for high duplication
    for project_key, project_data in comparison_results['projects'].items():
        if 'metrics' in project_data:
            duplication = project_data['metrics'].get('duplicated_lines_density', {}).get('sonarqube_value', 0)
            if duplication > 10:
                insights.append(f"- **{project_key}**: {duplication}% code duplication - consider refactoring")
    
    if insights:
        md_lines.extend(insights)
    else:
        md_lines.append("- All metrics are within acceptable thresholds")
    
    # Add project details
    md_lines.extend([
        f"",
        f"## Project Comparison Details",
        f""
    ])
    
    # Sort projects by discrepancy count
    projects_with_discrepancies = [
        (key, data) for key, data in comparison_results['projects'].items()
