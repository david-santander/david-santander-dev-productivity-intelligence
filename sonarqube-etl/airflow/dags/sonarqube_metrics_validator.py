"""
SonarQube Metrics Validator
Validates which metrics are available in your SonarQube instance.
"""

import asyncio
import logging
from typing import Dict, List, Set, Tuple
from sonarqube_client import SonarQubeClient, SonarQubeConfig, MetricDefinitions

logger = logging.getLogger(__name__)


class MetricsValidator:
    """Validates available metrics in SonarQube."""
    
    def __init__(self, client: SonarQubeClient):
        self.client = client
        self.valid_metrics: Set[str] = set()
        self.invalid_metrics: Set[str] = set()
        
    async def validate_metric(self, project_key: str, metric_key: str) -> bool:
        """Check if a single metric is valid."""
        try:
            response = await self.client._make_request(
                'GET',
                '/api/measures/component',
                params={
                    'component': project_key,
                    'metricKeys': metric_key
                }
            )
            # If we get here, the metric exists
            return True
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                return False
            # For other errors, we can't determine validity
            logger.warning(f"Unexpected error checking metric {metric_key}: {e}")
            return None
            
    async def validate_all_metrics(self, project_key: str) -> Tuple[Set[str], Set[str]]:
        """Validate all defined metrics."""
        all_metrics = list(MetricDefinitions.get_all_metrics().keys())
        all_metrics.extend(MetricDefinitions.get_new_code_metrics())
        
        logger.info(f"Validating {len(all_metrics)} metrics...")
        
        # Test metrics in small batches
        batch_size = 10
        for i in range(0, len(all_metrics), batch_size):
            batch = all_metrics[i:i + batch_size]
            try:
                response = await self.client._make_request(
                    'GET',
                    '/api/measures/component',
                    params={
                        'component': project_key,
                        'metricKeys': ','.join(batch)
                    }
                )
                # Metrics that returned are valid
                measures = response.get('component', {}).get('measures', [])
                for measure in measures:
                    self.valid_metrics.add(measure['metric'])
                    
            except Exception as e:
                if "404" in str(e):
                    # Some metrics in this batch are invalid, test individually
                    for metric in batch:
                        is_valid = await self.validate_metric(project_key, metric)
                        if is_valid:
                            self.valid_metrics.add(metric)
                        elif is_valid is False:
                            self.invalid_metrics.add(metric)
                            
        # Mark remaining metrics as invalid
        for metric in all_metrics:
            if metric not in self.valid_metrics:
                self.invalid_metrics.add(metric)
                
        return self.valid_metrics, self.invalid_metrics
        
    def get_valid_metrics_by_category(self) -> Dict[str, List[str]]:
        """Get valid metrics organized by category."""
        result = {}
        all_metrics_def = MetricDefinitions.get_all_metrics()
        
        for metric, category in all_metrics_def.items():
            if metric in self.valid_metrics:
                category_name = category.value
                if category_name not in result:
                    result[category_name] = []
                result[category_name].append(metric)
                
        # Check new code metrics
        new_code_valid = []
        for metric in self.valid_metrics:
            if metric.startswith('new_'):
                new_code_valid.append(metric)
                
        if new_code_valid:
            result['new_code'] = new_code_valid
            
        return result


# Now let's create a fixed metric fetcher that only requests valid metrics
class ValidatedMetricsFetcher:
    """Metric fetcher that only requests metrics known to be valid."""
    
    # These are the metrics that commonly exist in SonarQube
    # This list should be updated based on your SonarQube version
    KNOWN_VALID_METRICS = {
        # Size metrics
        'lines', 'ncloc', 'classes', 'functions', 'statements', 'files',
        
        # Issue metrics
        'bugs', 'vulnerabilities', 'code_smells', 'security_hotspots',
        'violations', 'blocker_violations', 'critical_violations',
        'major_violations', 'minor_violations', 'info_violations',
        
        # Rating metrics
        'reliability_rating', 'security_rating', 'sqale_rating',
        'security_review_rating',
        
        # Coverage metrics
        'coverage', 'line_coverage', 'branch_coverage',
        'uncovered_lines', 'uncovered_conditions',
        'lines_to_cover', 'conditions_to_cover',
        
        # Duplication metrics
        'duplicated_lines_density', 'duplicated_lines',
        'duplicated_blocks', 'duplicated_files',
        
        # Complexity metrics
        'complexity', 'cognitive_complexity',
        
        # Comments
        'comment_lines', 'comment_lines_density',
        
        # Technical debt
        'sqale_index', 'sqale_debt_ratio',
        'effort_to_reach_maintainability_rating_a',
        
        # Security
        'security_hotspots_reviewed',
        
        # Quality gate
        'alert_status', 'quality_gate_details',
    }
    
    # New code metrics that commonly exist
    KNOWN_VALID_NEW_CODE_METRICS = {
        'new_lines', 'new_coverage', 'new_line_coverage',
        'new_branch_coverage', 'new_uncovered_lines',
        'new_uncovered_conditions', 'new_lines_to_cover',
        'new_conditions_to_cover', 'new_duplicated_lines_density',
        'new_duplicated_lines', 'new_duplicated_blocks',
        'new_bugs', 'new_vulnerabilities', 'new_code_smells',
        'new_security_hotspots', 'new_violations',
        'new_blocker_violations', 'new_critical_violations',
        'new_major_violations', 'new_minor_violations',
        'new_info_violations', 'new_reliability_rating',
        'new_security_rating', 'new_maintainability_rating',
        'new_security_review_rating', 'new_technical_debt',
        'new_sqale_debt_ratio', 'new_security_hotspots_reviewed',
    }
    
    @classmethod
    def get_safe_metrics(cls) -> List[str]:
        """Get a list of metrics that are safe to request."""
        all_metrics = list(cls.KNOWN_VALID_METRICS)
        all_metrics.extend(cls.KNOWN_VALID_NEW_CODE_METRICS)
        return all_metrics


async def main():
    """Run metric validation."""
    # Use the production config
    config = SonarQubeConfig(
        base_url="http://sonarqube:9000",
        token="your_token_here"  # Replace with actual token
    )
    
    client = SonarQubeClient(config)
    validator = MetricsValidator(client)
    
    # You need at least one project to test metrics
    projects = await client.fetch_all_projects()
    if not projects:
        logger.error("No projects found in SonarQube")
        return
        
    project_key = projects[0].key
    logger.info(f"Using project '{project_key}' for validation")
    
    valid, invalid = await validator.validate_all_metrics(project_key)
    
    print("\n=== VALID METRICS ===")
    valid_by_category = validator.get_valid_metrics_by_category()
    for category, metrics in valid_by_category.items():
        print(f"\n{category}:")
        for metric in sorted(metrics):
            print(f"  - {metric}")
            
    print("\n=== INVALID METRICS ===")
    for metric in sorted(invalid):
        print(f"  - {metric}")
        
    print(f"\nTotal: {len(valid)} valid, {len(invalid)} invalid")
    
    # Generate a list of valid metrics for the code
    print("\n=== VALID METRICS LIST (for code) ===")
    print("VALID_METRICS = {")
    for metric in sorted(valid):
        print(f'    "{metric}",')
    print("}")


if __name__ == "__main__":
    asyncio.run(main())