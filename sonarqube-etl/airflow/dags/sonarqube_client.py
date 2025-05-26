"""SonarQube API Client for metrics extraction.

This module provides a unified interface for interacting with the SonarQube API,
handling both current and historical metrics retrieval. It encapsulates all API
communication logic and provides a clean interface for the ETL DAGs.

The client supports:
- Project listing with pagination
- Current metrics retrieval
- Historical metrics retrieval
- Intelligent API selection based on date
- Comprehensive error handling and logging

Example:
    Basic usage of the SonarQube client::
    
        from sonarqube_client import SonarQubeClient
        
        # Initialize client
        config = {
            'base_url': 'http://sonarqube:9000',
            'token': 'your-token',
            'auth': ('your-token', '')
        }
        client = SonarQubeClient(config)
        
        # Get all projects
        projects = client.fetch_all_projects()
        
        # Get current metrics
        metrics = client.fetch_current_metrics('my-project')
        
        # Get historical metrics
        old_metrics = client.fetch_historical_metrics('my-project', '2025-01-01')

Classes:
    SonarQubeClient: Main client class for SonarQube API interactions
"""

import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dateutil import parser


class SonarQubeClient:
    """Client for interacting with SonarQube API.
    
    This class provides a unified interface for all SonarQube API operations,
    including project retrieval, metrics extraction, and issue analysis.
    
    Attributes:
        base_url (str): SonarQube server base URL
        auth (Tuple[str, str]): Authentication tuple for requests
        metrics_to_fetch (List[str]): Standard metrics to retrieve
        new_code_metrics (List[str]): Metrics specific to new code
        issue_types (List[str]): Types of issues to analyze
        severities (List[str]): Issue severity levels
        statuses (List[str]): Issue status values
        
    Example:
        >>> client = SonarQubeClient(config)
        >>> projects = client.fetch_all_projects()
        >>> for project in projects:
        ...     metrics = client.fetch_metrics_smart(project['key'], '2025-01-15')
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize SonarQube client with configuration.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary containing:
                - base_url: SonarQube server URL
                - token: API authentication token
                - auth: Authentication tuple
        """
        self.base_url = config['base_url']
        self.auth = config['auth']
        self.token = config['token']
        
        # Define metrics to fetch
        self.metrics_to_fetch = [
            'bugs', 'vulnerabilities', 'code_smells', 'security_hotspots',
            'coverage', 'duplicated_lines_density', 'reliability_rating',
            'security_rating', 'sqale_rating'
        ]
        
        self.new_code_metrics = [
            'new_bugs', 'new_vulnerabilities', 'new_code_smells', 'new_security_hotspots',
            'new_coverage', 'new_duplicated_lines_density', 'new_lines'
        ]
        
        # Issue categorization
        self.issue_types = ['BUG', 'VULNERABILITY', 'CODE_SMELL']
        self.severities = ['BLOCKER', 'CRITICAL', 'MAJOR', 'MINOR', 'INFO']
        self.statuses = ['OPEN', 'CONFIRMED', 'REOPENED', 'RESOLVED', 'CLOSED']
        self.hotspot_severities = ['HIGH', 'MEDIUM', 'LOW']
        self.hotspot_statuses = ['TO_REVIEW', 'REVIEWED']
        
    def fetch_all_projects(self) -> List[Dict[str, Any]]:
        """Fetch all projects from SonarQube with pagination.
        
        Returns:
            List[Dict[str, Any]]: List of project dictionaries containing:
                - key: Project identifier
                - name: Project display name
                - qualifier: Project type
                - visibility: Project visibility setting
                
        Raises:
            requests.HTTPError: If API request fails
            
        Example:
            >>> projects = client.fetch_all_projects()
            >>> print(f"Found {len(projects)} projects")
        """
        logging.info(f"Fetching projects from {self.base_url}")
        logging.info(f"Using token: {self.token[:10]}...{self.token[-4:] if len(self.token) > 14 else self.token}")
        
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
        """Fetch current metrics for a project.
        
        Uses the measures/component API endpoint which returns the latest
        metric values for a project.
        
        Args:
            project_key (str): SonarQube project key
            
        Returns:
            Dict[str, Any]: Dictionary containing current metric values
            
        Raises:
            requests.HTTPError: If API request fails
            
        Example:
            >>> metrics = client.fetch_current_metrics('my-project')
            >>> print(f"Current bugs: {metrics.get('bugs', 0)}")
        """
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
        """Fetch historical metrics for a specific date.
        
        Uses the measures/search_history API endpoint to retrieve metric
        values from a specific date in the past.
        
        Args:
            project_key (str): SonarQube project key
            metric_date (str): Date to fetch metrics for (YYYY-MM-DD)
            
        Returns:
            Dict[str, Any]: Dictionary containing historical metric values
            
        Example:
            >>> old_metrics = client.fetch_historical_metrics('my-project', '2025-01-01')
            >>> print(f"Bugs on Jan 1: {old_metrics.get('bugs', 0)}")
        """
        logging.info(f"Fetching historical metrics for {project_key} on {metric_date}")
        
        metrics = {}
        all_metrics = self.metrics_to_fetch + self.new_code_metrics
        
        for metric in all_metrics:
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
                
        return metrics
    
    def fetch_issue_breakdown(self, project_key: str, is_new_code: bool = False) -> Dict[str, int]:
        """Fetch detailed issue breakdown by type, severity, and status.
        
        Args:
            project_key (str): SonarQube project key
            is_new_code (bool): Whether to fetch issues only in new code period
            
        Returns:
            Dict[str, int]: Issue counts keyed by type_severity or type_status
            
        Example:
            >>> issues = client.fetch_issue_breakdown('my-project')
            >>> print(f"Critical bugs: {issues.get('bug_critical', 0)}")
        """
        issues_breakdown = {}
        prefix = "new_code_" if is_new_code else ""
        
        # Fetch issues by type and severity
        for issue_type in self.issue_types:
            for severity in self.severities:
                params = {
                    'componentKeys': project_key,
                    'types': issue_type,
                    'severities': severity,
                    'resolved': 'false',
                    'ps': 1
                }
                if is_new_code:
                    params['inNewCodePeriod'] = 'true'
                    
                try:
                    response = requests.get(
                        f"{self.base_url}/api/issues/search",
                        params=params,
                        auth=self.auth
                    )
                    response.raise_for_status()
                    key = f"{prefix}{issue_type}_{severity}".lower()
                    issues_breakdown[key] = response.json()['total']
                except Exception as e:
                    logging.warning(f"Failed to fetch issues for {issue_type}/{severity}: {str(e)}")
                    issues_breakdown[f"{prefix}{issue_type}_{severity}".lower()] = 0
            
            # Fetch issues by type and status (only for overall, not new code)
            if not is_new_code:
                for status in self.statuses:
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
        
        # Fetch security hotspots
        self._fetch_hotspot_breakdown(project_key, issues_breakdown, is_new_code)
        
        return issues_breakdown
    
    def _fetch_hotspot_breakdown(self, project_key: str, issues_breakdown: Dict[str, int], 
                                is_new_code: bool = False) -> None:
        """Fetch security hotspot breakdown (internal method).
        
        Args:
            project_key (str): SonarQube project key
            issues_breakdown (Dict[str, int]): Dictionary to update with hotspot counts
            is_new_code (bool): Whether to fetch only new code hotspots
        """
        prefix = "new_code_" if is_new_code else ""
        
        # Fetch by severity
        for severity in self.hotspot_severities:
            params = {
                'projectKey': project_key,
                'securityCategory': severity,
                'ps': 1
            }
            if is_new_code:
                params['inNewCodePeriod'] = 'true'
                
            try:
                response = requests.get(
                    f"{self.base_url}/api/hotspots/search",
                    params=params,
                    auth=self.auth
                )
                response.raise_for_status()
                key = f"{prefix}security_hotspot_{severity}".lower()
                issues_breakdown[key] = response.json()['paging']['total']
            except Exception as e:
                logging.warning(f"Failed to fetch hotspots for {severity}: {str(e)}")
                issues_breakdown[f"{prefix}security_hotspot_{severity}".lower()] = 0
        
        # Fetch by status (only for overall)
        if not is_new_code:
            for status in self.hotspot_statuses:
                response = requests.get(
                    f"{self.base_url}/api/hotspots/search",
                    params={
                        'projectKey': project_key,
                        'status': status,
                        'ps': 1
                    },
                    auth=self.auth
                )
                response.raise_for_status()
                key = f"security_hotspot_{status}".lower()
                issues_breakdown[key] = response.json()['paging']['total']
    
    def fetch_metrics_smart(self, project_key: str, metric_date: str) -> Dict[str, Any]:
        """Intelligently fetch metrics using the most appropriate API.
        
        Automatically chooses between current and historical API based on
        the requested date. Uses current API for today/yesterday, historical
        API for older dates.
        
        Args:
            project_key (str): SonarQube project key
            metric_date (str): Date to fetch metrics for (YYYY-MM-DD)
            
        Returns:
            Dict[str, Any]: Complete metrics data including:
                - project_key: The project identifier
                - metric_date: The date of metrics
                - metrics: Core metric values
                - issues_breakdown: Detailed issue counts
                - new_code_issues_breakdown: New code issue counts
                
        Example:
            >>> # Automatically uses current API for recent date
            >>> metrics = client.fetch_metrics_smart('my-project', '2025-01-25')
            >>> 
            >>> # Automatically uses historical API for old date
            >>> old_metrics = client.fetch_metrics_smart('my-project', '2024-12-01')
        """
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
        """Fetch project metrics with explicit API selection.
        
        This method maintains backward compatibility with the original
        fetch_project_metrics function signature.
        
        Args:
            project_key (str): SonarQube project key
            metric_date (str): Date to fetch metrics for (YYYY-MM-DD)
            use_history (bool): Force use of historical API if True
            
        Returns:
            Dict[str, Any]: Complete metrics data
            
        Note:
            This method exists for backward compatibility. New code should
            use fetch_metrics_smart() or specific fetch methods directly.
        """
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