"""
SonarQube API Client v4 - Enhanced Modular Architecture
======================================================

This version provides a clean, modular, and extensible architecture with:
- Separation of concerns with dedicated modules for different metric types
- Comprehensive error handling and retry logic
- Type hints and dataclasses for better code clarity
- Async support for improved performance
- Configurable metric fetchers
- Built-in caching mechanisms
- Extensive logging and monitoring

Main improvements:
1. Modular design with separate metric fetchers
2. Better error handling with custom exceptions
3. Response models using dataclasses
4. Configurable retry strategies
5. Performance optimizations
6. Cleaner API interface
"""

import logging
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional, Set, Union, Callable
from functools import lru_cache
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser


# Configure logging
logger = logging.getLogger(__name__)


# =====================================================================
# ENUMS AND CONSTANTS
# =====================================================================

class IssueType(Enum):
    """SonarQube issue types."""
    BUG = "BUG"
    VULNERABILITY = "VULNERABILITY"
    CODE_SMELL = "CODE_SMELL"
    
    
class Severity(Enum):
    """Issue severity levels."""
    BLOCKER = "BLOCKER"
    CRITICAL = "CRITICAL"
    MAJOR = "MAJOR"
    MINOR = "MINOR"
    INFO = "INFO"
    

class IssueStatus(Enum):
    """Issue status values."""
    OPEN = "OPEN"
    CONFIRMED = "CONFIRMED"
    REOPENED = "REOPENED"
    RESOLVED = "RESOLVED"
    CLOSED = "CLOSED"
    

class Resolution(Enum):
    """Issue resolution types."""
    FALSE_POSITIVE = "FALSE-POSITIVE"
    WONTFIX = "WONTFIX"
    

class HotspotPriority(Enum):
    """Security hotspot priority levels."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    

class HotspotStatus(Enum):
    """Security hotspot status values (new API)."""
    TO_REVIEW = "TO_REVIEW"
    REVIEWED = "REVIEWED"


class HotspotResolution(Enum):
    """Security hotspot resolution values (for REVIEWED status)."""
    FIXED = "FIXED"
    SAFE = "SAFE"
    ACKNOWLEDGED = "ACKNOWLEDGED"


class MetricCategory(Enum):
    """Metric categories for organization."""
    SIZE = "size"
    ISSUES = "issues"
    COVERAGE = "coverage"
    DUPLICATION = "duplication"
    COMPLEXITY = "complexity"
    MAINTAINABILITY = "maintainability"
    SECURITY = "security"
    RELIABILITY = "reliability"
    

# =====================================================================
# DATA MODELS
# =====================================================================

@dataclass
class SonarQubeConfig:
    """Configuration for SonarQube client."""
    base_url: str
    token: str
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.3
    verify_ssl: bool = True
    cache_ttl: int = 300  # Cache TTL in seconds
    

@dataclass
class Project:
    """SonarQube project model."""
    key: str
    name: str
    qualifier: str
    visibility: str
    last_analysis_date: Optional[datetime] = None
    
    def __post_init__(self):
        if isinstance(self.last_analysis_date, str):
            self.last_analysis_date = parser.parse(self.last_analysis_date)
            

@dataclass
class MetricValue:
    """Single metric value."""
    key: str
    value: Union[str, int, float]
    category: MetricCategory
    
    
@dataclass
class IssueBreakdown:
    """Issue breakdown by various dimensions."""
    by_type_severity: Dict[str, Dict[str, int]] = field(default_factory=dict)
    by_type_status: Dict[str, Dict[str, int]] = field(default_factory=dict)
    by_type_resolution: Dict[str, Dict[str, int]] = field(default_factory=dict)
    totals_by_type: Dict[str, int] = field(default_factory=dict)
    

@dataclass
class SecurityHotspotBreakdown:
    """Security hotspot breakdown."""
    by_priority: Dict[str, int] = field(default_factory=dict)
    by_status: Dict[str, int] = field(default_factory=dict)
    total: int = 0
    

@dataclass
class ProjectMetrics:
    """Complete metrics for a project."""
    project_key: str
    metric_date: str
    metrics: Dict[str, Union[str, int, float]]
    issue_breakdown: IssueBreakdown
    hotspot_breakdown: SecurityHotspotBreakdown
    new_code_issue_breakdown: Optional[IssueBreakdown] = None
    new_code_hotspot_breakdown: Optional[SecurityHotspotBreakdown] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    

# =====================================================================
# EXCEPTIONS
# =====================================================================

class SonarQubeError(Exception):
    """Base exception for SonarQube client errors."""
    pass


class SonarQubeAPIError(SonarQubeError):
    """API request failed."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code
        

class SonarQubeConfigError(SonarQubeError):
    """Configuration error."""
    pass


# =====================================================================
# METRIC DEFINITIONS
# =====================================================================

class MetricDefinitions:
    """Central repository of all metric definitions."""
    
    SIZE_METRICS = {
        'lines': MetricCategory.SIZE,
        'ncloc': MetricCategory.SIZE,
        'classes': MetricCategory.SIZE,
        'functions': MetricCategory.SIZE,
        'statements': MetricCategory.SIZE,
        'files': MetricCategory.SIZE,
        'directories': MetricCategory.SIZE,
    }
    
    ISSUE_METRICS = {
        'bugs': MetricCategory.RELIABILITY,
        'vulnerabilities': MetricCategory.SECURITY,
        'code_smells': MetricCategory.MAINTAINABILITY,
        'security_hotspots': MetricCategory.SECURITY,
        'violations': MetricCategory.ISSUES,
        'open_issues': MetricCategory.ISSUES,
        'confirmed_issues': MetricCategory.ISSUES,
        'false_positive_issues': MetricCategory.ISSUES,
        'accepted_issues': MetricCategory.ISSUES,
    }
    
    RATING_METRICS = {
        'reliability_rating': MetricCategory.RELIABILITY,
        'security_rating': MetricCategory.SECURITY,
        'sqale_rating': MetricCategory.MAINTAINABILITY,
        'security_review_rating': MetricCategory.SECURITY,
    }
    
    REMEDIATION_METRICS = {
        'reliability_remediation_effort': MetricCategory.RELIABILITY,
        'security_remediation_effort': MetricCategory.SECURITY,
        'effort_to_reach_maintainability_rating_a': MetricCategory.MAINTAINABILITY,
    }
    
    TECHNICAL_DEBT_METRICS = {
        'sqale_index': MetricCategory.MAINTAINABILITY,
        'sqale_debt_ratio': MetricCategory.MAINTAINABILITY,
    }
    
    SECURITY_REVIEW_METRICS = {
        'security_hotspots_reviewed': MetricCategory.SECURITY,
    }
    
    COVERAGE_METRICS = {
        'coverage': MetricCategory.COVERAGE,
        'line_coverage': MetricCategory.COVERAGE,
        'branch_coverage': MetricCategory.COVERAGE,
        'covered_lines': MetricCategory.COVERAGE,
        'uncovered_lines': MetricCategory.COVERAGE,
        'covered_conditions': MetricCategory.COVERAGE,
        'uncovered_conditions': MetricCategory.COVERAGE,
        'lines_to_cover': MetricCategory.COVERAGE,
        'conditions_to_cover': MetricCategory.COVERAGE,
    }
    
    DUPLICATION_METRICS = {
        'duplicated_lines_density': MetricCategory.DUPLICATION,
        'duplicated_lines': MetricCategory.DUPLICATION,
        'duplicated_blocks': MetricCategory.DUPLICATION,
        'duplicated_files': MetricCategory.DUPLICATION,
    }
    
    COMPLEXITY_METRICS = {
        'complexity': MetricCategory.COMPLEXITY,
        'cognitive_complexity': MetricCategory.COMPLEXITY,
    }
    
    COMMENT_METRICS = {
        'comment_lines': MetricCategory.SIZE,
        'comment_lines_density': MetricCategory.SIZE,
    }
    
    QUALITY_GATE_METRICS = {
        'alert_status': MetricCategory.ISSUES,
        'quality_gate_details': MetricCategory.ISSUES,
    }
    
    @classmethod
    def get_all_metrics(cls) -> Dict[str, MetricCategory]:
        """Get all metric definitions."""
        all_metrics = {}
        for attr_name in dir(cls):
            if attr_name.endswith('_METRICS') and not attr_name.startswith('_'):
                metrics_dict = getattr(cls, attr_name)
                all_metrics.update(metrics_dict)
        return all_metrics
    
    @classmethod
    def get_new_code_metrics(cls) -> Set[str]:
        """Get all new code metric keys."""
        base_metrics = cls.get_all_metrics()
        new_code_metrics = set()
        
        for metric in base_metrics:
            if metric not in ['alert_status', 'quality_gate_details']:
                new_code_metrics.add(f'new_{metric}')
                
        # Special cases
        new_code_metrics.update([
            'new_lines',  # Not 'new_lines' but 'new_lines'
            'new_technical_debt',  # Different from sqale_index
            'new_violations',
            'new_accepted_issues',
            'new_confirmed_issues',
        ])
        
        return new_code_metrics


# =====================================================================
# BASE CLASSES
# =====================================================================

class BaseMetricFetcher(ABC):
    """Base class for metric fetchers."""
    
    def __init__(self, client: 'SonarQubeClient'):
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    @abstractmethod
    async def fetch(self, project_key: str, **kwargs) -> Dict[str, Any]:
        """Fetch metrics for a project."""
        pass


# =====================================================================
# METRIC FETCHERS
# =====================================================================

class StandardMetricsFetcher(BaseMetricFetcher):
    """Fetches standard project metrics."""
    
    # Define a safe subset of metrics that are commonly available
    SAFE_BASE_METRICS = {
        'lines', 'ncloc', 'classes', 'functions', 'files',
        'bugs', 'vulnerabilities', 'code_smells', 'security_hotspots',
        'violations', 'blocker_violations', 'critical_violations',
        'major_violations', 'minor_violations', 'info_violations',
        'reliability_rating', 'security_rating', 'sqale_rating',
        'coverage', 'line_coverage', 'branch_coverage',
        'duplicated_lines_density', 'duplicated_lines',
        'complexity', 'cognitive_complexity',
        'comment_lines', 'comment_lines_density',
        'sqale_index', 'sqale_debt_ratio',
        'alert_status'
    }
    
    SAFE_NEW_CODE_METRICS = {
        'new_lines', 'new_coverage', 'new_line_coverage',
        'new_bugs', 'new_vulnerabilities', 'new_code_smells',
        'new_security_hotspots', 'new_violations',
        'new_duplicated_lines_density', 'new_duplicated_lines',
        'new_reliability_rating', 'new_security_rating',
        'new_maintainability_rating'
    }
    
    async def fetch(self, project_key: str, **kwargs) -> Dict[str, Any]:
        """Fetch current metrics for a project."""
        # Use safe subset of metrics to avoid 404 errors
        safe_metrics = list(self.SAFE_BASE_METRICS)
        safe_metrics.extend(self.SAFE_NEW_CODE_METRICS)
        
        try:
            response = await self.client._make_request(
                'GET',
                '/api/measures/component',
                params={
                    'component': project_key,
                    'metricKeys': ','.join(safe_metrics)
                }
            )
            
            measures = response.get('component', {}).get('measures', [])
            metrics = {m['metric']: m.get('value', 0) for m in measures}
            
            # Extract new code period information
            component_data = response.get('component', {})
            if 'period' in component_data:
                period = component_data['period']
                metrics['new_code_period_date'] = period.get('date')
                metrics['new_code_period_mode'] = period.get('mode', 'days')
                metrics['new_code_period_value'] = period.get('value', '30')
                
            return metrics
            
        except Exception as e:
            if "404" in str(e) and "metric keys are not found" in str(e):
                # Handle case where some metrics don't exist
                # Try with minimal set of metrics
                logger.warning(f"Some metrics not found, trying minimal set: {e}")
                
                minimal_metrics = [
                    'bugs', 'vulnerabilities', 'code_smells',
                    'coverage', 'duplicated_lines_density',
                    'ncloc', 'complexity'
                ]
                
                response = await self.client._make_request(
                    'GET',
                    '/api/measures/component',
                    params={
                        'component': project_key,
                        'metricKeys': ','.join(minimal_metrics)
                    }
                )
                
                measures = response.get('component', {}).get('measures', [])
                return {m['metric']: m.get('value', 0) for m in measures}
            else:
                raise


class HistoricalMetricsFetcher(BaseMetricFetcher):
    """Fetches historical metrics."""
    
    async def fetch(self, project_key: str, metric_date: str, **kwargs) -> Dict[str, Any]:
        """Fetch historical metrics for a specific date."""
        metrics = {}
        all_metrics = list(MetricDefinitions.get_all_metrics().keys())
        all_metrics.extend(MetricDefinitions.get_new_code_metrics())
        
        # Remove metrics without history
        metrics_with_history = [
            m for m in all_metrics 
            if m not in ['quality_gate_details', 'alert_status']
        ]
        
        # Batch fetch metrics to reduce API calls
        batch_size = 10
        for i in range(0, len(metrics_with_history), batch_size):
            batch = metrics_with_history[i:i + batch_size]
            
            try:
                response = await self.client._make_request(
                    'GET',
                    '/api/measures/search_history',
                    params={
                        'component': project_key,
                        'metrics': ','.join(batch),
                        'from': metric_date,
                        'to': metric_date,
                        'ps': 1000
                    }
                )
                
                for measure in response.get('measures', []):
                    metric_key = measure['metric']
                    if measure.get('history'):
                        for hist_point in measure['history']:
                            if hist_point.get('date', '').startswith(metric_date):
                                metrics[metric_key] = hist_point.get('value', 0)
                                break
                        if metric_key not in metrics and measure['history']:
                            metrics[metric_key] = measure['history'][-1].get('value', 0)
                            
            except Exception as e:
                self.logger.warning(f"Failed to fetch historical batch {batch}: {str(e)}")
                
        return metrics


class IssueFetcher(BaseMetricFetcher):
    """Fetches detailed issue breakdowns."""
    
    async def fetch(self, project_key: str, is_new_code: bool = False, **kwargs) -> IssueBreakdown:
        """Fetch issue breakdown for a project."""
        breakdown = IssueBreakdown()
        
        # Fetch issues by type and severity
        for issue_type in IssueType:
            type_key = issue_type.value.lower()
            breakdown.by_type_severity[type_key] = {}
            breakdown.by_type_status[type_key] = {}
            breakdown.by_type_resolution[type_key] = {}
            type_total = 0
            
            # By severity
            for severity in Severity:
                count = await self._fetch_issue_count(
                    project_key, issue_type, severity, is_new_code
                )
                severity_key = f"{type_key}_{severity.value.lower()}"
                breakdown.by_type_severity[type_key][severity_key] = count
                type_total += count
                
            breakdown.totals_by_type[type_key] = type_total
            
            # By status (only for overall code)
            if not is_new_code:
                for status in IssueStatus:
                    count = await self._fetch_issue_count_by_status(
                        project_key, issue_type, status
                    )
                    status_key = f"{type_key}_{status.value.lower()}"
                    breakdown.by_type_status[type_key][status_key] = count
                    
                # By resolution
                for resolution in Resolution:
                    count = await self._fetch_issue_count_by_resolution(
                        project_key, issue_type, resolution
                    )
                    resolution_key = f"{type_key}_{resolution.value.lower()}"
                    breakdown.by_type_resolution[type_key][resolution_key] = count
                    
        return breakdown
    
    async def _fetch_issue_count(
        self, 
        project_key: str, 
        issue_type: IssueType, 
        severity: Severity, 
        is_new_code: bool
    ) -> int:
        """Fetch count of issues by type and severity."""
        params = {
            'componentKeys': project_key,
            'types': issue_type.value,
            'severities': severity.value,
            'resolved': 'false',
            'ps': 1
        }
        if is_new_code:
            params['sinceLeakPeriod'] = 'true'
            
        try:
            response = await self.client._make_request(
                'GET', '/api/issues/search', params=params
            )
            return response.get('total', 0)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch {issue_type.value}/{severity.value} issues: {str(e)}"
            )
            return 0
            
    async def _fetch_issue_count_by_status(
        self,
        project_key: str,
        issue_type: IssueType,
        status: IssueStatus
    ) -> int:
        """Fetch count of issues by type and status."""
        params = {
            'componentKeys': project_key,
            'types': issue_type.value,
            'statuses': status.value,
            'ps': 1
        }
        
        try:
            response = await self.client._make_request(
                'GET', '/api/issues/search', params=params
            )
            return response.get('total', 0)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch {issue_type.value}/{status.value} issues: {str(e)}"
            )
            return 0
            
    async def _fetch_issue_count_by_resolution(
        self,
        project_key: str,
        issue_type: IssueType,
        resolution: Resolution
    ) -> int:
        """Fetch count of issues by type and resolution."""
        params = {
            'componentKeys': project_key,
            'types': issue_type.value,
            'resolutions': resolution.value,
            'ps': 1
        }
        
        try:
            response = await self.client._make_request(
                'GET', '/api/issues/search', params=params
            )
            return response.get('total', 0)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch {issue_type.value}/{resolution.value} issues: {str(e)}"
            )
            return 0


class SecurityHotspotFetcher(BaseMetricFetcher):
    """Fetches security hotspot breakdowns."""
    
    async def fetch(
        self, 
        project_key: str, 
        is_new_code: bool = False, 
        **kwargs
    ) -> SecurityHotspotBreakdown:
        """Fetch security hotspot breakdown."""
        breakdown = SecurityHotspotBreakdown()
        
        # Fetch by priority
        for priority in HotspotPriority:
            count = await self._fetch_hotspot_count_by_priority(
                project_key, priority, is_new_code
            )
            breakdown.by_priority[priority.value.lower()] = count
            breakdown.total += count
            
        # Fetch by status
        for status in HotspotStatus:
            count = await self._fetch_hotspot_count_by_status(
                project_key, status, is_new_code
            )
            breakdown.by_status[status.value.lower()] = count
            
        return breakdown
    
    async def _fetch_hotspot_count_by_priority(
        self,
        project_key: str,
        priority: HotspotPriority,
        is_new_code: bool
    ) -> int:
        """Fetch hotspot count by priority."""
        params = {
            'projectKey': project_key,
            'vulnerabilityProbability': priority.value,
            'ps': 1
        }
        if is_new_code:
            params['sinceLeakPeriod'] = 'true'
            
        try:
            response = await self.client._make_request(
                'GET', '/api/hotspots/search', params=params
            )
            return response.get('paging', {}).get('total', 0)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch hotspots with priority {priority.value}: {str(e)}"
            )
            return 0
            
    async def _fetch_hotspot_count_by_status(
        self,
        project_key: str,
        status: HotspotStatus,
        is_new_code: bool
    ) -> int:
        """Fetch hotspot count by status."""
        params = {
            'projectKey': project_key,
            'status': status.value,
            'ps': 1
        }
        if is_new_code:
            params['sinceLeakPeriod'] = 'true'
            
        try:
            response = await self.client._make_request(
                'GET', '/api/hotspots/search', params=params
            )
            return response.get('paging', {}).get('total', 0)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch hotspots with status {status.value}: {str(e)}"
            )
            return 0


# =====================================================================
# MAIN CLIENT
# =====================================================================

class SonarQubeClient:
    """Enhanced SonarQube API client with modular architecture."""
    
    def __init__(self, config: Union[Dict[str, Any], SonarQubeConfig]):
        """Initialize the client with configuration."""
        if isinstance(config, dict):
            self.config = SonarQubeConfig(**config)
        else:
            self.config = config
            
        self._session = self._create_session()
        self._cache = {}
        
        # Initialize fetchers
        self.metrics_fetcher = StandardMetricsFetcher(self)
        self.historical_fetcher = HistoricalMetricsFetcher(self)
        self.issue_fetcher = IssueFetcher(self)
        self.hotspot_fetcher = SecurityHotspotFetcher(self)
        
        logger.info(f"Initialized SonarQube client for {self.config.base_url}")
        
    def _create_session(self) -> requests.Session:
        """Create a configured requests session."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set authentication
        session.auth = (self.config.token, '')
        
        return session
        
    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        **kwargs
    ) -> Dict[str, Any]:
        """Make an API request with error handling."""
        url = f"{self.config.base_url}{endpoint}"
        
        # Check cache for GET requests
        cache_key = f"{method}:{endpoint}:{str(kwargs)}"
        if method == 'GET' and cache_key in self._cache:
            cached_data, cached_time = self._cache[cache_key]
            if datetime.now() - cached_time < timedelta(seconds=self.config.cache_ttl):
                return cached_data
                
        try:
            response = self._session.request(
                method,
                url,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
                **kwargs
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Cache successful GET responses
            if method == 'GET':
                self._cache[cache_key] = (data, datetime.now())
                
            return data
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            logger.error(error_msg)
            raise SonarQubeAPIError(error_msg, e.response.status_code)
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(error_msg)
            raise SonarQubeAPIError(error_msg)
            
    async def fetch_all_projects(self) -> List[Project]:
        """Fetch all projects with pagination."""
        logger.info("Fetching all projects")
        projects = []
        page = 1
        page_size = 100
        
        while True:
            response = await self._make_request(
                'GET',
                '/api/projects/search',
                params={'p': page, 'ps': page_size}
            )
            
            components = response.get('components', [])
            projects.extend([
                Project(
                    key=comp['key'],
                    name=comp['name'],
                    qualifier=comp.get('qualifier', 'TRK'),
                    visibility=comp.get('visibility', 'public'),
                    last_analysis_date=comp.get('lastAnalysisDate')
                )
                for comp in components
            ])
            
            if len(projects) >= response['paging']['total']:
                break
                
            page += 1
            
        logger.info(f"Found {len(projects)} projects")
        return projects
        
    async def fetch_project_metrics(
        self,
        project_key: str,
        metric_date: Optional[str] = None,
        include_issues: bool = True,
        include_new_code: bool = True
    ) -> ProjectMetrics:
        """Fetch comprehensive metrics for a project."""
        logger.info(f"Fetching metrics for {project_key} on {metric_date or 'current'}")
        
        # Determine whether to use current or historical API
        use_historical = False
        if metric_date:
            requested_date = parser.parse(metric_date).date()
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            use_historical = requested_date < yesterday
            
        # Fetch base metrics
        if use_historical and metric_date:
            metrics = await self.historical_fetcher.fetch(project_key, metric_date)
        else:
            metrics = await self.metrics_fetcher.fetch(project_key)
            
        # Initialize result
        result = ProjectMetrics(
            project_key=project_key,
            metric_date=metric_date or datetime.now().strftime('%Y-%m-%d'),
            metrics=metrics,
            issue_breakdown=IssueBreakdown(),
            hotspot_breakdown=SecurityHotspotBreakdown()
        )
        
        # Fetch issue breakdowns if requested
        if include_issues:
            result.issue_breakdown = await self.issue_fetcher.fetch(project_key, False)
            result.hotspot_breakdown = await self.hotspot_fetcher.fetch(project_key, False)
            
            # Fetch new code issues if there are new lines
            if include_new_code and int(metrics.get('new_lines', 0)) > 0:
                result.new_code_issue_breakdown = await self.issue_fetcher.fetch(
                    project_key, True
                )
                result.new_code_hotspot_breakdown = await self.hotspot_fetcher.fetch(
                    project_key, True
                )
                
        # Add metadata
        result.metadata = {
            'fetched_at': datetime.now().isoformat(),
            'use_historical_api': use_historical,
            'cache_hits': len(self._cache)  # Total cache size instead of specific key
        }
        
        return result
        
    def clear_cache(self):
        """Clear the internal cache."""
        self._cache.clear()
        logger.info("Cache cleared")
        
    def close(self):
        """Close the client session."""
        self._session.close()
        logger.info("Client session closed")
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    # Synchronous wrappers for backward compatibility
    def fetch_metrics_smart(self, project_key: str, metric_date: str) -> Dict[str, Any]:
        """Synchronous wrapper for fetch_project_metrics."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                self.fetch_project_metrics(project_key, metric_date)
            )
            
            # Convert to legacy format for backward compatibility
            legacy_format = {
                'project_key': result.project_key,
                'metric_date': result.metric_date,
                'metrics': result.metrics,
                'issues_breakdown': self._flatten_issue_breakdown(result.issue_breakdown),
                'new_code_issues_breakdown': {}
            }
            
            if result.new_code_issue_breakdown:
                legacy_format['new_code_issues_breakdown'] = self._flatten_issue_breakdown(
                    result.new_code_issue_breakdown, prefix='new_code_'
                )
                
            # Add hotspot data to issues breakdown
            legacy_format['issues_breakdown'].update(
                self._flatten_hotspot_breakdown(result.hotspot_breakdown)
            )
            
            if result.new_code_hotspot_breakdown:
                legacy_format['new_code_issues_breakdown'].update(
                    self._flatten_hotspot_breakdown(
                        result.new_code_hotspot_breakdown, prefix='new_code_'
                    )
                )
                
            return legacy_format
            
        finally:
            loop.close()
            
    def _flatten_issue_breakdown(
        self, 
        breakdown: IssueBreakdown, 
        prefix: str = ''
    ) -> Dict[str, int]:
        """Flatten issue breakdown to legacy format."""
        flat = {}
        
        # Flatten by type and severity
        for issue_type, severities in breakdown.by_type_severity.items():
            for severity_key, count in severities.items():
                flat[f"{prefix}{severity_key}"] = count
                
        # Flatten by type and status
        for issue_type, statuses in breakdown.by_type_status.items():
            for status_key, count in statuses.items():
                flat[f"{prefix}{status_key}"] = count
                
        # Flatten by type and resolution
        for issue_type, resolutions in breakdown.by_type_resolution.items():
            for resolution_key, count in resolutions.items():
                flat[f"{prefix}{resolution_key}"] = count
                
        # Add calculated totals
        for issue_type, total in breakdown.totals_by_type.items():
            flat[f"{prefix}{issue_type}_calculated_total"] = total
            
        return flat
        
    def _flatten_hotspot_breakdown(
        self,
        breakdown: SecurityHotspotBreakdown,
        prefix: str = ''
    ) -> Dict[str, int]:
        """Flatten hotspot breakdown to legacy format."""
        flat = {}
        
        # By priority
        for priority, count in breakdown.by_priority.items():
            flat[f"{prefix}security_hotspot_{priority}"] = count
            
        # By status
        for status, count in breakdown.by_status.items():
            flat[f"{prefix}security_hotspot_{status}"] = count
            
        # Total
        flat[f"{prefix}security_hotspot_calculated_total"] = breakdown.total
        
        return flat


# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def create_client(config: Dict[str, Any]) -> SonarQubeClient:
    """Factory function to create a SonarQube client."""
    return SonarQubeClient(config)


def convert_rating_to_letter(rating_value: Union[str, int, float, None]) -> Optional[str]:
    """Convert numeric rating to letter grade."""
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
        if isinstance(rating_value, str) and rating_value in ['A', 'B', 'C', 'D', 'E']:
            return rating_value
        return None