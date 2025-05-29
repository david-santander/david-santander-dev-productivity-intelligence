"""SonarQube API Client v2 - Aligned with official API documentation.

This updated version corrects metric keys to match the official SonarQube API
documentation and adds support for security review metrics, quality gate status,
and other missing metrics.

Changes from v1:
- Fixed technical debt metric keys (sqale_index instead of technical_debt)
- Added security review metrics
- Added quality gate metrics
- Added violations and issue state metrics
- Corrected typo in new_reliability_remediation_effort
"""

import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dateutil import parser


class SonarQubeClient:
    """Enhanced client for interacting with SonarQube API v2."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize SonarQube client with configuration."""
        self.base_url = config['base_url']
        self.auth = config['auth']
        self.token = config['token']
        
        # Define comprehensive metrics per official SonarQube API documentation
        self.metrics_to_fetch = [
            # Size metrics
            'lines', 'ncloc', 'classes', 'functions', 'statements', 'files', 'directories',
            
            # Issue metrics (using correct keys)
            'bugs', 'vulnerabilities', 'code_smells', 'security_hotspots',
            'violations',  # Total issues
            'open_issues', 'confirmed_issues', 'false_positive_issues',
            'accepted_issues',  # Issues accepted by developers
            
            # Quality ratings
            'reliability_rating', 'security_rating', 'sqale_rating',
            'security_review_rating',  # New: Security hotspot review rating
            
            # Remediation effort
            'reliability_remediation_effort', 'security_remediation_effort',
            'effort_to_reach_maintainability_rating_a',
            
            # Technical debt (correct keys)
            'sqale_index',  # Technical debt in minutes
            'sqale_debt_ratio',  # Technical debt ratio
            
            # Security review metrics
            'security_hotspots_reviewed',  # Percentage of reviewed hotspots
            
            # Coverage metrics
            'coverage', 'line_coverage', 'branch_coverage',
            'covered_lines', 'uncovered_lines', 'covered_conditions', 'uncovered_conditions',
            'lines_to_cover', 'conditions_to_cover',
            
            # Duplication metrics
            'duplicated_lines_density', 'duplicated_lines', 'duplicated_blocks', 'duplicated_files',
            
            # Complexity metrics
            'complexity', 'cognitive_complexity',
            
            # Comment metrics
            'comment_lines', 'comment_lines_density',
            
            # Quality gate
            'alert_status', 'quality_gate_details'
        ]
        
        self.new_code_metrics = [
            # New code size
            'new_lines', 'new_ncloc',
            
            # New code issues
            'new_bugs', 'new_vulnerabilities', 'new_code_smells', 'new_security_hotspots',
            'new_violations',  # Total new issues
            'new_accepted_issues', 'new_confirmed_issues',
            
            # New code remediation effort (fixed typo)
            'new_reliability_remediation_effort', 'new_security_remediation_effort',
            
            # New code technical debt (correct keys)
            'new_technical_debt',  # This is the correct key for new code
            'new_sqale_debt_ratio',
            
            # New code security review
            'new_security_hotspots_reviewed',
            'new_security_review_rating',
            
            # New code coverage
            'new_coverage', 'new_line_coverage', 'new_branch_coverage',
            'new_covered_lines', 'new_uncovered_lines', 'new_covered_conditions', 
            'new_uncovered_conditions', 'new_lines_to_cover', 'new_conditions_to_cover',
            
            # New code duplications
            'new_duplicated_lines_density', 'new_duplicated_lines', 'new_duplicated_blocks',
            
            # New code complexity
            'new_complexity', 'new_cognitive_complexity'
        ]
        
        # Issue categorization
        self.issue_types = ['BUG', 'VULNERABILITY', 'CODE_SMELL']
        self.severities = ['BLOCKER', 'CRITICAL', 'MAJOR', 'MINOR', 'INFO']
        self.statuses = ['OPEN', 'CONFIRMED', 'REOPENED', 'RESOLVED', 'CLOSED']
        self.resolutions = ['FALSE-POSITIVE', 'WONTFIX']
        self.hotspot_priorities = ['HIGH', 'MEDIUM', 'LOW']
        self.hotspot_statuses = ['TO_REVIEW', 'ACKNOWLEDGED', 'FIXED', 'SAFE']
        
    def fetch_all_projects(self) -> List[Dict[str, Any]]:
        """Fetch all projects from SonarQube with pagination."""
        logging.info(f"Fetching projects from {self.base_url}")
        
        projects = []
        page = 1
        page_size = 100
        
        while True:
            response = requests.get(
                f"{self.base_url}/api/projects/search",
                params={'p': page, 'ps': page_size},
                auth=self.auth
            )
            response.raise_for_status()
            
            data = response.json()
            projects.extend(data['components'])
            
            if len(projects) >= data['paging']['total']:
                break
            page += 1
            
        logging.info(f"Found {len(projects)} projects in SonarQube")
        return projects
    
    def fetch_current_metrics(self, project_key: str) -> Dict[str, Any]:
        """Fetch current metrics for a project including quality gate status."""
        logging.info(f"Fetching current metrics for {project_key}")
        
        all_metrics = self.metrics_to_fetch + self.new_code_metrics
        
        response = requests.get(
            f"{self.base_url}/api/measures/component",
            params={
                'component': project_key,
                'metricKeys': ','.join(all_metrics)
            },
            auth=self.auth
        )
        response.raise_for_status()
        
        measures = response.json().get('component', {}).get('measures', [])
        metrics = {m['metric']: m.get('value', 0) for m in measures}
        
        # Special handling for quality_gate_details (it's returned as periods)
        for measure in measures:
            if measure['metric'] == 'quality_gate_details':
                metrics['quality_gate_details'] = measure.get('value', '{}')
        
        # Get new code period information
        component_data = response.json().get('component', {})
        if 'period' in component_data:
            period = component_data['period']
            metrics['new_code_period_date'] = period.get('date', None)
            metrics['new_code_period_mode'] = period.get('mode', 'days')
            metrics['new_code_period_value'] = period.get('value', '30')
            
        logging.debug(f"Retrieved {len(metrics)} metrics for {project_key}")
        return metrics
    
    def fetch_historical_metrics(self, project_key: str, metric_date: str) -> Dict[str, Any]:
        """Fetch historical metrics for a specific date."""
        logging.info(f"Fetching historical metrics for {project_key} on {metric_date}")
        
        metrics = {}
        all_metrics = self.metrics_to_fetch + self.new_code_metrics
        
        # Remove metrics that don't have history
        metrics_with_history = [m for m in all_metrics if m not in ['quality_gate_details', 'alert_status']]
        
        for metric in metrics_with_history:
            try:
                response = requests.get(
                    f"{self.base_url}/api/measures/search_history",
                    params={
                        'component': project_key,
                        'metrics': metric,
                        'from': metric_date,
                        'to': metric_date,
                        'ps': 1000
                    },
                    auth=self.auth
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
        
        # For quality gate status, we need to check if it was available at that date
        # This is a limitation - historical quality gate status is not easily retrievable
        metrics['alert_status'] = None
        metrics['quality_gate_details'] = None
                
        return metrics
    
    def fetch_issue_breakdown(self, project_key: str, is_new_code: bool = False) -> Dict[str, int]:
        """Fetch detailed issue breakdown by type, severity, and status."""
        issues_breakdown = {}
        prefix = "new_code_" if is_new_code else ""
        
        # Fetch issues by type and severity
        for issue_type in self.issue_types:
            type_total = 0
            
            for severity in self.severities:
                params = {
                    'componentKeys': project_key,
                    'types': issue_type,
                    'severities': severity,
                    'resolved': 'false',
                    'ps': 1
                }
                if is_new_code:
                    params['sinceLeakPeriod'] = 'true'
                    
                try:
                    response = requests.get(
                        f"{self.base_url}/api/issues/search",
                        params=params,
                        auth=self.auth
                    )
                    response.raise_for_status()
                    key = f"{prefix}{issue_type}_{severity}".lower()
                    count = response.json()['total']
                    issues_breakdown[key] = count
                    type_total += count
                except Exception as e:
                    logging.warning(f"Failed to fetch issues for {issue_type}/{severity}: {str(e)}")
                    issues_breakdown[f"{prefix}{issue_type}_{severity}".lower()] = 0
            
            # Store the calculated total
            issues_breakdown[f"{prefix}{issue_type}_calculated_total"] = type_total
            
            # Fetch issues by type and status (only for overall, not new code)
            if not is_new_code:
                for status in self.statuses:
                    try:
                        response = requests.get(
                            f"{self.base_url}/api/issues/search",
                            params={
                                'componentKeys': project_key,
                                'types': issue_type,
                                'statuses': status,
                                'ps': 1
                            },
                            auth=self.auth
                        )
                        response.raise_for_status()
                        key = f"{issue_type}_{status}".lower()
                        issues_breakdown[key] = response.json()['total']
                    except Exception as e:
                        logging.warning(f"Failed to fetch issues for {issue_type}/{status}: {str(e)}")
                        issues_breakdown[f"{issue_type}_{status}".lower()] = 0
                
                # Fetch issues by type and resolution
                for resolution in self.resolutions:
                    try:
                        response = requests.get(
                            f"{self.base_url}/api/issues/search",
                            params={
                                'componentKeys': project_key,
                                'types': issue_type,
                                'resolutions': resolution,
                                'ps': 1
                            },
                            auth=self.auth
                        )
                        response.raise_for_status()
                        key = f"{issue_type}_{resolution}".lower()
                        issues_breakdown[key] = response.json()['total']
                    except Exception as e:
                        logging.warning(f"Failed to fetch issues for {issue_type}/{resolution}: {str(e)}")
                        issues_breakdown[f"{issue_type}_{resolution}".lower()] = 0
        
        # Fetch security hotspots
        self._fetch_hotspot_breakdown(project_key, issues_breakdown, is_new_code)
        
        return issues_breakdown
    
    def _fetch_hotspot_breakdown(self, project_key: str, issues_breakdown: Dict[str, int], 
                                is_new_code: bool = False) -> None:
        """Fetch security hotspot breakdown (internal method)."""
        prefix = "new_code_" if is_new_code else ""
        
        # Calculate total for security hotspots
        hotspot_total = 0
        
        # Fetch by priority using vulnerabilityProbability parameter
        for priority in self.hotspot_priorities:
            params = {
                'projectKey': project_key,
                'vulnerabilityProbability': priority,
                'ps': 1
            }
            if is_new_code:
                params['sinceLeakPeriod'] = 'true'
                
            try:
                response = requests.get(
                    f"{self.base_url}/api/hotspots/search",
                    params=params,
                    auth=self.auth
                )
                response.raise_for_status()
                key = f"{prefix}security_hotspot_{priority}".lower()
                count = response.json()['paging']['total']
                issues_breakdown[key] = count
                hotspot_total += count
            except Exception as e:
                logging.warning(f"Failed to fetch hotspots for priority {priority}: {str(e)}")
                issues_breakdown[f"{prefix}security_hotspot_{priority}".lower()] = 0
        
        # Store calculated total
        issues_breakdown[f"{prefix}security_hotspot_calculated_total"] = hotspot_total
        
        # Fetch by status
        for status in self.hotspot_statuses:
            params = {
                'projectKey': project_key,
                'status': status,
                'ps': 1
            }
            if is_new_code:
                params['sinceLeakPeriod'] = 'true'
                
            try:
                response = requests.get(
                    f"{self.base_url}/api/hotspots/search",
                    params=params,
                    auth=self.auth
                )
                response.raise_for_status()
                key = f"{prefix}security_hotspot_{status}".lower()
                issues_breakdown[key] = response.json()['paging']['total']
            except Exception as e:
                logging.warning(f"Failed to fetch hotspots for status {status}: {str(e)}")
                issues_breakdown[f"{prefix}security_hotspot_{status}".lower()] = 0
    
    def fetch_metrics_smart(self, project_key: str, metric_date: str) -> Dict[str, Any]:
        """Intelligently fetch metrics using the most appropriate API."""
        # Determine which API to use based on date
        requested_date = parser.parse(metric_date).date()
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        # Use current API for today or yesterday
        if requested_date >= yesterday:
            logging.info(f"Using current API for {project_key} on {metric_date} (recent date)")
            metrics = self.fetch_current_metrics(project_key)
        else:
            logging.info(f"Using historical API for {project_key} on {metric_date} (past date)")
            metrics = self.fetch_historical_metrics(project_key, metric_date)
            
        # Fetch issue breakdowns
        issues_breakdown = self.fetch_issue_breakdown(project_key, is_new_code=False)
        new_code_issues_breakdown = {}
        
        # Only fetch new code issues if there are new lines
        if metrics and int(metrics.get('new_lines', 0)) > 0:
            new_code_issues_breakdown = self.fetch_issue_breakdown(project_key, is_new_code=True)
        
        return {
            'project_key': project_key,
            'metric_date': metric_date,
            'metrics': metrics,
            'issues_breakdown': issues_breakdown,
            'new_code_issues_breakdown': new_code_issues_breakdown
        }
    
    def fetch_project_metrics(self, project_key: str, metric_date: str, 
                            use_history: bool = False) -> Dict[str, Any]:
        """Fetch project metrics with explicit API selection (backward compatibility)."""
        if use_history:
            # Force historical API
            metrics = self.fetch_historical_metrics(project_key, metric_date)
            
            # Fetch issue breakdowns
            issues_breakdown = self.fetch_issue_breakdown(project_key, is_new_code=False)
            new_code_issues_breakdown = {}
            
            if metrics and int(metrics.get('new_lines', 0)) > 0:
                new_code_issues_breakdown = self.fetch_issue_breakdown(project_key, is_new_code=True)
            
            return {
                'project_key': project_key,
                'metric_date': metric_date,
                'metrics': metrics,
                'issues_breakdown': issues_breakdown,
                'new_code_issues_breakdown': new_code_issues_breakdown
            }
        else:
            # Use smart selection
            return self.fetch_metrics_smart(project_key, metric_date)