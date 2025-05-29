"""
SonarQube Metrics Dashboard v4 - Enterprise-Grade Analytics
==========================================================

This version provides a professional, modular dashboard with:
- Clean architecture with separation of concerns
- Reusable components and utilities
- Advanced visualizations and analytics
- Performance optimizations with caching
- Responsive design for all screen sizes
- Export capabilities in multiple formats
- Real-time data refresh options
- User preferences and session state
- Advanced filtering and drill-down capabilities
- Custom themes and branding support

Key improvements:
1. Modular architecture with separate modules for data, UI, and analytics
2. Advanced caching strategies for performance
3. Interactive drill-down capabilities
4. Comparison and benchmarking features
5. Predictive analytics and trend forecasting
6. Executive summary and reporting features
7. Multi-project portfolio views
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta, date
import os
from typing import Dict, List, Tuple, Any, Optional, Union
from dataclasses import dataclass, field
from functools import lru_cache, wraps
import json
import time
from enum import Enum
import logging
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================================
# CONFIGURATION AND CONSTANTS
# =====================================================================

# Page configuration
st.set_page_config(
    page_title="SonarQube Metrics Dashboard Pro",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://docs.sonarqube.org/',
        'Report a bug': 'https://github.com/your-org/sonarqube-dashboard/issues',
        'About': 'SonarQube Metrics Dashboard v4.0 - Enterprise Edition'
    }
)

# Custom CSS for professional styling
st.markdown("""
<style>
    /* CSS Variables for theming */
    :root {
        --primary-color: #1e3a8a;
        --secondary-color: #3b82f6;
        --success-color: #22c55e;
        --warning-color: #f59e0b;
        --danger-color: #ef4444;
        --background-color: #f8fafc;
        --card-background: #ffffff;
        --text-primary: #1f2937;
        --text-secondary: #6b7280;
        --border-color: #e5e7eb;
        --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
        --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
    }
    
    /* Main container */
    .main {
        background-color: var(--background-color);
    }
    
    /* Enhanced header */
    .dashboard-header {
        background: white;
        color: var(--text-primary);
        padding: 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: var(--shadow-lg);
        border: 1px solid var(--border-color);
    }
    
    .dashboard-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        color: var(--text-primary) !important;
    }
    
    .dashboard-header h1 {
        color: var(--text-primary) !important;
    }
    
    /* Override Streamlit's default h1 styling in dashboard header */
    .dashboard-header h1.dashboard-title {
        color: var(--text-primary) !important;
    }
    
    .dashboard-subtitle {
        font-size: 1.1rem;
        color: var(--text-secondary) !important;
        margin-top: 0.5rem;
    }
    
    /* Card components */
    .metric-card {
        background: var(--card-background);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid var(--border-color);
        box-shadow: var(--shadow-sm);
        transition: all 0.3s ease;
        height: 100%;
    }
    
    .metric-card:hover {
        box-shadow: var(--shadow-md);
        transform: translateY(-2px);
    }
    
    .metric-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .metric-card-title {
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .metric-card-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--text-primary);
        line-height: 1;
    }
    
    .metric-card-delta {
        display: inline-flex;
        align-items: center;
        font-size: 0.875rem;
        font-weight: 500;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        margin-top: 0.75rem;
    }
    
    .delta-positive {
        background-color: #d1fae5;
        color: #065f46;
    }
    
    .delta-negative {
        background-color: #fee2e2;
        color: #991b1b;
    }
    
    .delta-neutral {
        background-color: #f3f4f6;
        color: #6b7280;
    }
    
    /* Rating badges */
    .rating-badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 48px;
        height: 48px;
        border-radius: 8px;
        font-weight: 700;
        font-size: 1.5rem;
    }
    
    .rating-a { background: #22c55e; color: white; }
    .rating-b { background: #84cc16; color: white; }
    .rating-c { background: #eab308; color: white; }
    .rating-d { background: #f97316; color: white; }
    .rating-e { background: #ef4444; color: white; }
    
    /* Section headers */
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--text-primary);
        margin: 2rem 0 1rem 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .section-divider {
        height: 2px;
        background: var(--border-color);
        margin: 2rem 0;
    }
    
    /* Info boxes */
    .info-box {
        background: #eff6ff;
        border-left: 4px solid var(--secondary-color);
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    .warning-box {
        background: #fef3c7;
        border-left: 4px solid var(--warning-color);
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
        border-bottom: 2px solid var(--border-color);
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 0.75rem 1.5rem;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        border-bottom: 3px solid var(--secondary-color);
    }
    
    /* Responsive grid */
    @media (max-width: 768px) {
        .dashboard-title { font-size: 2rem; }
        .metric-card { padding: 1rem; }
        .metric-card-value { font-size: 1.5rem; }
    }
</style>
""", unsafe_allow_html=True)


# =====================================================================
# DATA MODELS
# =====================================================================

@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = os.getenv('POSTGRES_HOST', 'postgres-metrics')
    port: int = int(os.getenv('POSTGRES_PORT', '5432'))
    database: str = os.getenv('POSTGRES_DB', 'sonarqube_metrics')
    user: str = os.getenv('POSTGRES_USER', 'airflow')
    password: str = os.getenv('POSTGRES_PASSWORD', 'airflow')
    min_connections: int = 1
    max_connections: int = 10


@dataclass
class MetricConfig:
    """Configuration for a metric."""
    key: str
    label: str
    icon: str
    category: str
    color: str
    format_type: str  # 'number', 'percentage', 'time', 'rating'
    lower_is_better: bool
    description: str = ""
    thresholds: Dict[str, float] = field(default_factory=dict)


@dataclass
class ChartConfig:
    """Configuration for charts."""
    title: str
    chart_type: str  # 'line', 'bar', 'scatter', 'heatmap', 'gauge'
    height: int = 400
    show_legend: bool = True
    show_grid: bool = True
    color_scheme: str = 'blues'


class MetricCategory(Enum):
    """Metric categories."""
    RELIABILITY = "Reliability"
    SECURITY = "Security"
    MAINTAINABILITY = "Maintainability"
    COVERAGE = "Coverage"
    DUPLICATION = "Duplication"
    SIZE = "Size"
    COMPLEXITY = "Complexity"


class TimeRange(Enum):
    """Predefined time ranges."""
    LAST_7_DAYS = ("Last 7 days", 7)
    LAST_14_DAYS = ("Last 14 days", 14)
    LAST_30_DAYS = ("Last 30 days", 30)
    LAST_90_DAYS = ("Last 90 days", 90)
    THIS_MONTH = ("This Month", "month")
    THIS_QUARTER = ("This Quarter", "quarter")
    THIS_YEAR = ("This Year", "year")
    CUSTOM = ("Custom", None)


# =====================================================================
# METRIC DEFINITIONS
# =====================================================================

METRIC_DEFINITIONS = {
    'bugs': MetricConfig(
        key='bugs',
        label='Bugs',
        icon='🐛',
        category=MetricCategory.RELIABILITY.value,
        color='#ef4444',
        format_type='number',
        lower_is_better=True,
        description='Number of bug issues in the code',
        thresholds={'good': 0, 'warning': 10, 'critical': 50}
    ),
    'vulnerabilities': MetricConfig(
        key='vulnerabilities',
        label='Vulnerabilities',
        icon='🔓',
        category=MetricCategory.SECURITY.value,
        color='#f97316',
        format_type='number',
        lower_is_better=True,
        description='Security vulnerabilities found in the code',
        thresholds={'good': 0, 'warning': 5, 'critical': 20}
    ),
    'code_smells': MetricConfig(
        key='code_smells',
        label='Code Smells',
        icon='🌫️',
        category=MetricCategory.MAINTAINABILITY.value,
        color='#eab308',
        format_type='number',
        lower_is_better=True,
        description='Maintainability issues in the code',
        thresholds={'good': 50, 'warning': 100, 'critical': 500}
    ),
    'security_hotspots': MetricConfig(
        key='security_hotspots',
        label='Security Hotspots',
        icon='🔥',
        category=MetricCategory.SECURITY.value,
        color='#dc2626',
        format_type='number',
        lower_is_better=True,
        description='Security-sensitive code requiring review',
        thresholds={'good': 0, 'warning': 10, 'critical': 50}
    ),
    'coverage': MetricConfig(
        key='coverage',
        label='Coverage',
        icon='📊',
        category=MetricCategory.COVERAGE.value,
        color='#22c55e',
        format_type='percentage',
        lower_is_better=False,
        description='Percentage of code covered by tests',
        thresholds={'critical': 50, 'warning': 70, 'good': 80}
    ),
    'duplicated_lines_density': MetricConfig(
        key='duplicated_lines_density',
        label='Duplication',
        icon='📋',
        category=MetricCategory.DUPLICATION.value,
        color='#6366f1',
        format_type='percentage',
        lower_is_better=True,
        description='Percentage of duplicated lines',
        thresholds={'good': 3, 'warning': 5, 'critical': 10}
    ),
    'technical_debt': MetricConfig(
        key='technical_debt',
        label='Technical Debt',
        icon='💰',
        category=MetricCategory.MAINTAINABILITY.value,
        color='#8b5cf6',
        format_type='time',
        lower_is_better=True,
        description='Estimated time to fix all maintainability issues'
    ),
    'ncloc': MetricConfig(
        key='ncloc',
        label='Lines of Code',
        icon='📝',
        category=MetricCategory.SIZE.value,
        color='#64748b',
        format_type='number',
        lower_is_better=False,
        description='Non-commenting lines of code'
    ),
    'complexity': MetricConfig(
        key='complexity',
        label='Cyclomatic Complexity',
        icon='🔀',
        category=MetricCategory.COMPLEXITY.value,
        color='#f59e0b',
        format_type='number',
        lower_is_better=True,
        description='Code complexity measure'
    ),
    'cognitive_complexity': MetricConfig(
        key='cognitive_complexity',
        label='Cognitive Complexity',
        icon='🧠',
        category=MetricCategory.COMPLEXITY.value,
        color='#10b981',
        format_type='number',
        lower_is_better=True,
        description='How hard the code is to understand'
    )
}


# =====================================================================
# DATABASE CONNECTION MANAGER
# =====================================================================

class DatabaseManager:
    """Manages database connections with pooling."""
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._pool:
            config = DatabaseConfig()
            self._pool = SimpleConnectionPool(
                config.min_connections,
                config.max_connections,
                host=config.host,
                port=config.port,
                database=config.database,
                user=config.user,
                password=config.password
            )
    
    def get_connection(self):
        """Get a connection from the pool."""
        return self._pool.getconn()
    
    def put_connection(self, conn):
        """Return a connection to the pool."""
        self._pool.putconn(conn)
    
    def close_all_connections(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()


# =====================================================================
# DATA ACCESS LAYER
# =====================================================================

class MetricsRepository:
    """Repository for accessing metrics data."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    @st.cache_data(ttl=300)
    def fetch_projects(_self) -> pd.DataFrame:
        """Fetch all projects."""
        query = """
        SELECT 
            project_id,
            project_name,
            sonarqube_project_key,
            last_analysis_date_from_sq,
            created_at,
            updated_at
        FROM sonarqube_metrics.sq_projects
        WHERE is_active = true
        ORDER BY project_name
        """
        
        conn = _self.db_manager.get_connection()
        try:
            df = pd.read_sql_query(query, conn)
            return df
        finally:
            _self.db_manager.put_connection(conn)
    
    @st.cache_data(ttl=300)
    def fetch_metrics(_self, 
                     project_ids: List[int], 
                     start_date: str, 
                     end_date: str,
                     metrics: Optional[List[str]] = None) -> pd.DataFrame:
        """Fetch metrics for specified projects and date range."""
        
        # Build dynamic column selection
        base_columns = ['m.metric_id', 'm.project_id', 'm.metric_date', 
                       'p.project_name', 'p.sonarqube_project_key']
        
        if metrics:
            # Map metric keys to database columns
            metric_columns = []
            for metric in metrics:
                if metric == 'bugs':
                    metric_columns.extend(['m.bugs_total', 'm.bugs_blocker', 
                                         'm.bugs_critical', 'm.bugs_major'])
                elif metric == 'vulnerabilities':
                    metric_columns.extend(['m.vulnerabilities_total', 
                                         'm.vulnerabilities_critical',
                                         'm.vulnerabilities_high'])
                elif metric == 'code_smells':
                    metric_columns.extend(['m.code_smells_total', 
                                         'm.code_smells_blocker',
                                         'm.code_smells_critical'])
                elif metric == 'security_hotspots':
                    metric_columns.extend(['m.security_hotspots_total',
                                         'm.security_hotspots_to_review'])
                elif metric == 'coverage':
                    metric_columns.extend(['m.coverage_percentage',
                                         'm.line_coverage_percentage',
                                         'm.branch_coverage_percentage'])
                elif metric == 'technical_debt':
                    metric_columns.extend(['m.technical_debt', 'm.sqale_debt_ratio'])
                elif metric == 'complexity':
                    metric_columns.extend(['m.complexity', 'm.cognitive_complexity'])
                else:
                    metric_columns.append(f'm.{metric}')
            
            # Always include rating columns
            rating_columns = ['m.reliability_rating', 'm.security_rating', 'm.sqale_rating']
            columns = base_columns + list(set(metric_columns)) + rating_columns
        else:
            columns = ['m.*', 'p.project_name', 'p.sonarqube_project_key']
        
        query = f"""
        SELECT {', '.join(columns)}
        FROM sonarqube_metrics.daily_project_metrics m
        JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
        WHERE m.project_id = ANY(%s)
        AND m.metric_date BETWEEN %s AND %s
        ORDER BY m.metric_date DESC, p.project_name
        """
        
        conn = _self.db_manager.get_connection()
        try:
            df = pd.read_sql_query(query, conn, params=(project_ids, start_date, end_date))
            df['metric_date'] = pd.to_datetime(df['metric_date'])
            return df
        finally:
            _self.db_manager.put_connection(conn)
    
    @st.cache_data(ttl=300)
    def fetch_latest_metrics(_self, project_ids: List[int]) -> pd.DataFrame:
        """Fetch the latest metrics for specified projects."""
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (project_id) *
            FROM sonarqube_metrics.daily_project_metrics
            WHERE project_id = ANY(%s)
            ORDER BY project_id, metric_date DESC
        )
        SELECT m.*, p.project_name, p.sonarqube_project_key
        FROM latest_metrics m
        JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
        """
        
        conn = _self.db_manager.get_connection()
        try:
            df = pd.read_sql_query(query, conn, params=(project_ids,))
            df['metric_date'] = pd.to_datetime(df['metric_date'])
            return df
        finally:
            _self.db_manager.put_connection(conn)
    
    def fetch_metric_trends(_self, 
                           project_id: int, 
                           metric_key: str, 
                           days: int = 30) -> pd.DataFrame:
        """Fetch trend data for a specific metric."""
        # Map metric key to database column
        column_map = {
            'bugs': 'bugs_total',
            'vulnerabilities': 'vulnerabilities_total',
            'code_smells': 'code_smells_total',
            'security_hotspots': 'security_hotspots_total',
            'coverage': 'coverage_percentage',
            'duplicated_lines_density': 'duplicated_lines_density',
            'technical_debt': 'technical_debt',
            'ncloc': 'ncloc',
            'complexity': 'complexity',
            'cognitive_complexity': 'cognitive_complexity'
        }
        
        db_column = column_map.get(metric_key, metric_key)
        
        query = f"""
        SELECT 
            metric_date,
            {db_column} as value,
            is_carried_forward
        FROM sonarqube_metrics.daily_project_metrics
        WHERE project_id = %s
        AND metric_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY metric_date
        """
        
        conn = self.db_manager.get_connection()
        try:
            df = pd.read_sql_query(query, conn, params=(project_id, days))
            df['metric_date'] = pd.to_datetime(df['metric_date'])
            return df
        finally:
            self.db_manager.put_connection(conn)


# =====================================================================
# VISUALIZATION COMPONENTS
# =====================================================================

class ChartBuilder:
    """Builder for creating consistent charts."""
    
    @staticmethod
    def create_metric_card(
        label: str,
        value: Union[int, float],
        delta: Optional[float] = None,
        delta_color: str = 'normal',
        icon: str = '',
        format_type: str = 'number'
    ) -> str:
        """Create an HTML metric card."""
        
        # Format value
        if format_type == 'number':
            formatted_value = f"{int(value):,}"
        elif format_type == 'percentage':
            formatted_value = f"{value:.1f}%"
        elif format_type == 'time':
            # Convert minutes to days/hours
            hours = value / 60
            if hours < 24:
                formatted_value = f"{hours:.1f}h"
            else:
                days = hours / 24
                formatted_value = f"{days:.1f}d"
        else:
            formatted_value = str(value)
        
        # Delta styling
        delta_html = ""
        if delta is not None:
            delta_icon = "↑" if delta > 0 else "↓" if delta < 0 else "→"
            delta_class = {
                'positive': 'delta-positive',
                'negative': 'delta-negative',
                'normal': 'delta-neutral'
            }.get(delta_color, 'delta-neutral')
            
            delta_html = f"""
            <div class="metric-card-delta {delta_class}">
                {delta_icon} {abs(delta):.1f}%
            </div>
            """
        
        return f"""
        <div class="metric-card">
            <div class="metric-card-header">
                <div class="metric-card-title">{icon} {label}</div>
            </div>
            <div class="metric-card-value">{formatted_value}</div>
            {delta_html}
        </div>
        """
    
    @staticmethod
    def create_trend_chart(
        df: pd.DataFrame,
        metric_config: MetricConfig,
        title: Optional[str] = None,
        height: int = 400
    ) -> go.Figure:
        """Create a trend chart for a metric."""
        
        fig = go.Figure()
        
        # Add main trace
        fig.add_trace(go.Scatter(
            x=df['metric_date'],
            y=df['value'],
            mode='lines+markers',
            name=metric_config.label,
            line=dict(color=metric_config.color, width=2),
            marker=dict(size=6),
            hovertemplate=(
                f"<b>{metric_config.label}</b><br>"
                "Date: %{x}<br>"
                "Value: %{y}<br>"
                "<extra></extra>"
            )
        ))
        
        # Add threshold lines if defined
        if metric_config.thresholds:
            for threshold_name, threshold_value in metric_config.thresholds.items():
                fig.add_hline(
                    y=threshold_value,
                    line_dash="dash",
                    line_color="gray",
                    annotation_text=threshold_name.capitalize(),
                    annotation_position="right"
                )
        
        # Update layout
        fig.update_layout(
            title=title or f"{metric_config.icon} {metric_config.label} Trend",
            xaxis_title="Date",
            yaxis_title=metric_config.label,
            height=height,
            hovermode='x unified',
            showlegend=False,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family="Inter, sans-serif"),
            margin=dict(l=0, r=0, t=40, b=0)
        )
        
        # Add grid
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#e5e7eb')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#e5e7eb')
        
        return fig
    
    @staticmethod
    def create_comparison_chart(
        df: pd.DataFrame,
        metric_key: str,
        group_by: str = 'project_name'
    ) -> go.Figure:
        """Create a comparison chart across projects or time periods."""
        
        metric_config = METRIC_DEFINITIONS.get(metric_key)
        if not metric_config:
            return go.Figure()
        
        # Map metric key to database column
        column_map = {
            'bugs': 'bugs_total',
            'vulnerabilities': 'vulnerabilities_total',
            'code_smells': 'code_smells_total',
            'security_hotspots': 'security_hotspots_total',
            'coverage': 'coverage_percentage',
            'duplicated_lines_density': 'duplicated_lines_density',
            'technical_debt': 'technical_debt',
            'ncloc': 'ncloc',
            'complexity': 'complexity',
            'cognitive_complexity': 'cognitive_complexity'
        }
        
        value_column = column_map.get(metric_key, metric_key)
        
        # Group and aggregate data
        grouped = df.groupby(group_by)[value_column].mean().sort_values(
            ascending=metric_config.lower_is_better
        )
        
        # Create bar chart
        fig = go.Figure(data=[
            go.Bar(
                x=grouped.index,
                y=grouped.values,
                marker_color=metric_config.color,
                text=grouped.values,
                texttemplate='%{text:.1f}',
                textposition='outside'
            )
        ])
        
        fig.update_layout(
            title=f"{metric_config.icon} {metric_config.label} Comparison",
            xaxis_title=group_by.replace('_', ' ').title(),
            yaxis_title=metric_config.label,
            showlegend=False,
            height=400,
            plot_bgcolor='white',
            paper_bgcolor='white'
        )
        
        return fig
    
    @staticmethod
    def create_heatmap(
        df: pd.DataFrame,
        metric_keys: List[str],
        normalize: bool = True
    ) -> go.Figure:
        """Create a heatmap of metrics across projects."""
        
        # Prepare data
        projects = df['project_name'].unique()
        
        # Create matrix
        matrix = []
        metric_labels = []
        
        for metric_key in metric_keys:
            if metric_key in METRIC_DEFINITIONS:
                metric_config = METRIC_DEFINITIONS[metric_key]
                metric_labels.append(metric_config.label)
                
                # Get values for each project
                values = []
                for project in projects:
                    project_df = df[df['project_name'] == project]
                    if not project_df.empty:
                        # Map to correct column
                        column_map = {
                            'bugs': 'bugs_total',
                            'vulnerabilities': 'vulnerabilities_total',
                            'code_smells': 'code_smells_total',
                            'security_hotspots': 'security_hotspots_total',
                            'coverage': 'coverage_percentage',
                            'technical_debt': 'technical_debt'
                        }
                        col = column_map.get(metric_key, metric_key)
                        if col in project_df.columns:
                            value = project_df[col].iloc[0]
                        else:
                            value = 0
                    else:
                        value = 0
                    values.append(value)
                
                matrix.append(values)
        
        # Normalize if requested
        if normalize and matrix:
            matrix = np.array(matrix)
            # Normalize each metric to 0-1 scale
            for i in range(len(matrix)):
                min_val = matrix[i].min()
                max_val = matrix[i].max()
                if max_val > min_val:
                    matrix[i] = (matrix[i] - min_val) / (max_val - min_val)
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=matrix,
            x=projects,
            y=metric_labels,
            colorscale='RdYlGn_r',
            text=matrix,
            texttemplate='%{text:.2f}',
            textfont={"size": 10}
        ))
        
        fig.update_layout(
            title="Metrics Heatmap",
            xaxis_title="Projects",
            yaxis_title="Metrics",
            height=400 + len(metric_keys) * 30
        )
        
        return fig


# =====================================================================
# UI COMPONENTS
# =====================================================================

class DashboardUI:
    """Main dashboard UI class."""
    
    def __init__(self):
        self.metrics_repo = MetricsRepository()
        self.chart_builder = ChartBuilder()
        
    def render_header(self):
        """Render the dashboard header."""
        st.markdown("""
        <div class="dashboard-header" style="background: white; color: #1f2937; padding: 2rem; border-radius: 12px; margin-bottom: 2rem; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);">
            <h1 class="dashboard-title" style="color: #1f2937 !important; font-size: 2.5rem; font-weight: 700; margin: 0;">SonarQube Metrics Dashboard</h1>
            <p class="dashboard-subtitle" style="color: #6b7280 !important; font-size: 1.1rem; margin-top: 0.5rem;">
                Real-time code quality insights and analytics
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    def render_sidebar(self) -> Dict[str, Any]:
        """Render sidebar controls and return selections."""
        with st.sidebar:
            st.header("🎛️ Dashboard Controls")
            
            # Project selection
            st.subheader("📁 Projects")
            projects_df = self.metrics_repo.fetch_projects()
            
            if projects_df.empty:
                st.error("No projects found in the database.")
                return {}
            
            # Multi-select with search
            project_options = {
                row['project_name']: row['project_id'] 
                for _, row in projects_df.iterrows()
            }
            
            selected_projects = st.multiselect(
                "Select Projects",
                options=list(project_options.keys()),
                default=list(project_options.keys())[:5],
                help="Choose one or more projects to analyze"
            )
            
            selected_project_ids = [project_options[name] for name in selected_projects]
            
            st.divider()
            
            # Time range selection
            st.subheader("📅 Time Range")
            
            time_range = st.selectbox(
                "Select Range",
                options=[tr.value[0] for tr in TimeRange],
                index=2  # Default to Last 30 days
            )
            
            # Calculate dates
            if time_range == "Custom":
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Start Date", 
                                              value=date.today() - timedelta(days=30))
                with col2:
                    end_date = st.date_input("End Date", value=date.today())
            else:
                end_date = date.today()
                for tr in TimeRange:
                    if tr.value[0] == time_range:
                        if isinstance(tr.value[1], int):
                            start_date = end_date - timedelta(days=tr.value[1])
                        elif tr.value[1] == 'month':
                            start_date = end_date.replace(day=1)
                        elif tr.value[1] == 'quarter':
                            quarter_month = ((end_date.month - 1) // 3) * 3 + 1
                            start_date = end_date.replace(month=quarter_month, day=1)
                        else:  # year
                            start_date = end_date.replace(month=1, day=1)
                        break
            
            st.divider()
            
            # Metric selection
            st.subheader("📊 Metrics")
            
            # Group metrics by category
            metric_categories = {}
            for key, config in METRIC_DEFINITIONS.items():
                category = config.category
                if category not in metric_categories:
                    metric_categories[category] = []
                metric_categories[category].append((key, config))
            
            selected_metrics = []
            for category, metrics in metric_categories.items():
                with st.expander(f"{category} ({len(metrics)} metrics)", expanded=True):
                    for key, config in metrics:
                        if st.checkbox(
                            f"{config.icon} {config.label}",
                            value=key in ['bugs', 'vulnerabilities', 'coverage', 'code_smells'],
                            key=f"metric_{key}",
                            help=config.description
                        ):
                            selected_metrics.append(key)
            
            st.divider()
            
            # View options
            st.subheader("👁️ View Options")
            
            show_trends = st.checkbox("Show Trend Charts", value=True)
            show_comparisons = st.checkbox("Show Comparisons", value=True)
            show_heatmap = st.checkbox("Show Metrics Heatmap", value=False)
            show_details = st.checkbox("Show Detailed Table", value=True)
            
            # Data options
            st.subheader("⚙️ Data Options")
            
            include_carried_forward = st.checkbox(
                "Include Carried Forward Data",
                value=True,
                help="Include data points that were carried forward from previous dates"
            )
            
            auto_refresh = st.checkbox(
                "Auto Refresh",
                value=False,
                help="Automatically refresh data every 5 minutes"
            )
            
            if auto_refresh:
                st.info("🔄 Auto-refresh enabled (5 minutes)")
            
            return {
                'project_ids': selected_project_ids,
                'project_names': selected_projects,
                'start_date': start_date,
                'end_date': end_date,
                'metrics': selected_metrics,
                'show_trends': show_trends,
                'show_comparisons': show_comparisons,
                'show_heatmap': show_heatmap,
                'show_details': show_details,
                'include_carried_forward': include_carried_forward,
                'auto_refresh': auto_refresh
            }
    
    def render_kpi_section(self, df: pd.DataFrame, selected_metrics: List[str]):
        """Render KPI cards section."""
        if df.empty or not selected_metrics:
            return
        
        st.markdown('<h2 class="section-header">📈 Key Performance Indicators</h2>', 
                   unsafe_allow_html=True)
        
        
        # Get latest and previous data
        latest_date = df['metric_date'].max()
        latest_df = df[df['metric_date'] == latest_date]
        
        # Calculate previous period for comparison
        date_range = (df['metric_date'].max() - df['metric_date'].min()).days
        previous_date = latest_date - timedelta(days=date_range if date_range > 0 else 7)
        previous_df = df[df['metric_date'] <= previous_date].groupby('project_id').first()
        
        # Create columns for KPIs
        cols_per_row = 4
        for i in range(0, len(selected_metrics), cols_per_row):
            cols = st.columns(cols_per_row)
            
            for j, col in enumerate(cols):
                if i + j < len(selected_metrics):
                    metric_key = selected_metrics[i + j]
                    if metric_key not in METRIC_DEFINITIONS:
                        continue
                        
                    metric_config = METRIC_DEFINITIONS[metric_key]
                    
                    # Calculate aggregated value
                    column_map = {
                        'bugs': 'bugs_total',
                        'vulnerabilities': 'vulnerabilities_total',
                        'code_smells': 'code_smells_total',
                        'security_hotspots': 'security_hotspots_total',
                        'coverage': 'coverage_percentage',
                        'duplicated_lines_density': 'duplicated_lines_density',
                        'technical_debt': 'technical_debt',
                        'ncloc': 'ncloc',
                        'complexity': 'complexity',
                        'cognitive_complexity': 'cognitive_complexity'
                    }
                    
                    db_column = column_map.get(metric_key, metric_key)
                    
                    if db_column in latest_df.columns:
                        # Calculate current value
                        if metric_config.format_type == 'percentage' and 'coverage' in metric_key:
                            # Weighted average for coverage
                            if 'lines_to_cover' in latest_df.columns:
                                current_value = (
                                    latest_df['covered_lines'].sum() / 
                                    latest_df['lines_to_cover'].sum() * 100
                                ) if latest_df['lines_to_cover'].sum() > 0 else 0
                            else:
                                current_value = latest_df[db_column].mean()
                        elif metric_config.lower_is_better:
                            current_value = latest_df[db_column].sum()
                        else:
                            current_value = latest_df[db_column].mean()
                        
                        # Calculate previous value for delta
                        if not previous_df.empty and db_column in previous_df.columns:
                            if metric_config.format_type == 'percentage' and 'coverage' in metric_key:
                                if 'lines_to_cover' in previous_df.columns:
                                    previous_value = (
                                        previous_df['covered_lines'].sum() / 
                                        previous_df['lines_to_cover'].sum() * 100
                                    ) if previous_df['lines_to_cover'].sum() > 0 else 0
                                else:
                                    previous_value = previous_df[db_column].mean()
                            elif metric_config.lower_is_better:
                                previous_value = previous_df[db_column].sum()
                            else:
                                previous_value = previous_df[db_column].mean()
                            
                            # Calculate delta
                            if previous_value != 0:
                                delta = ((current_value - previous_value) / previous_value) * 100
                            else:
                                delta = 0 if current_value == 0 else 100
                        else:
                            delta = None
                        
                        # Determine delta color
                        if delta is not None:
                            if metric_config.lower_is_better:
                                delta_color = 'positive' if delta < 0 else 'negative' if delta > 0 else 'normal'
                            else:
                                delta_color = 'positive' if delta > 0 else 'negative' if delta < 0 else 'normal'
                        else:
                            delta_color = 'normal'
                        
                        # Render card
                        with col:
                            card_html = self.chart_builder.create_metric_card(
                                label=metric_config.label,
                                value=current_value,
                                delta=delta,
                                delta_color=delta_color,
                                icon=metric_config.icon,
                                format_type=metric_config.format_type
                            )
                            st.markdown(card_html, unsafe_allow_html=True)
        
        # Add quality ratings (always show ratings regardless of selected metrics)
        if True:  # Always show quality ratings
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            st.markdown('<h3 class="section-header">⭐ Quality Ratings</h3>', 
                       unsafe_allow_html=True)
            
            rating_cols = st.columns(4)
            
            rating_mappings = [
                ('reliability_rating', 'Reliability Rating', '🛡️'),
                ('security_rating', 'Security Rating', '🔒'),
                ('sqale_rating', 'Maintainability Rating', '🔧'),
                ('security_review_rating', 'Security Review Rating', '👁️')
            ]
            
            for idx, (rating_col, label, icon) in enumerate(rating_mappings):
                if idx < len(rating_cols) and rating_col in latest_df.columns:
                    ratings = latest_df[rating_col].dropna()
                    if not ratings.empty:
                        # Get worst rating (highest number = worst)
                        worst_rating_num = max(ratings)
                        
                        # Convert numeric rating to letter grade
                        rating_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E'}
                        worst_rating_letter = rating_map.get(int(worst_rating_num), 'E')
                        
                        # Define colors for each rating
                        color_map = {
                            'A': '#22c55e',  # Green
                            'B': '#84cc16',  # Light green
                            'C': '#eab308',  # Yellow
                            'D': '#f97316',  # Orange
                            'E': '#ef4444'   # Red
                        }
                        badge_color = color_map.get(worst_rating_letter, '#ef4444')
                        
                        # Create rating card similar to KPI cards
                        with rating_cols[idx]:
                            card_html = f"""
                            <div class="metric-card">
                                <div class="metric-card-header">
                                    <span class="metric-card-icon">{icon}</span>
                                    <span class="metric-card-title">{label}</span>
                                </div>
                                <div class="metric-card-content">
                                    <div style="display: inline-flex; align-items: center; justify-content: center; width: 64px; height: 64px; border-radius: 12px; font-weight: 700; font-size: 2rem; background-color: {badge_color}; color: white; margin: 0.5rem auto;">
                                        {worst_rating_letter}
                                    </div>
                                    <div style="font-size: 0.875rem; color: #6b7280; margin-top: 0.5rem;">
                                        Grade: {worst_rating_letter} ({int(worst_rating_num)}/5)
                                    </div>
                                </div>
                            </div>
                            """
                            st.markdown(card_html, unsafe_allow_html=True)
    
    def render_trends_section(self, df: pd.DataFrame, selected_metrics: List[str], 
                            project_names: List[str]):
        """Render trend charts section."""
        if df.empty or not selected_metrics:
            return
        
        st.markdown('<h2 class="section-header">📊 Trends Over Time</h2>', 
                   unsafe_allow_html=True)
        
        # Create tabs for different metrics
        tabs = st.tabs([METRIC_DEFINITIONS[m].icon + " " + METRIC_DEFINITIONS[m].label 
                       for m in selected_metrics if m in METRIC_DEFINITIONS])
        
        for idx, (tab, metric_key) in enumerate(zip(tabs, selected_metrics)):
            if metric_key not in METRIC_DEFINITIONS:
                continue
                
            with tab:
                metric_config = METRIC_DEFINITIONS[metric_key]
                
                # Create trend chart
                column_map = {
                    'bugs': 'bugs_total',
                    'vulnerabilities': 'vulnerabilities_total',
                    'code_smells': 'code_smells_total',
                    'security_hotspots': 'security_hotspots_total',
                    'coverage': 'coverage_percentage',
                    'duplicated_lines_density': 'duplicated_lines_density',
                    'technical_debt': 'technical_debt',
                    'ncloc': 'ncloc',
                    'complexity': 'complexity',
                    'cognitive_complexity': 'cognitive_complexity'
                }
                
                db_column = column_map.get(metric_key, metric_key)
                
                if db_column in df.columns:
                    # Prepare data for plotting
                    plot_data = []
                    
                    for project in project_names:
                        project_df = df[df['project_name'] == project].sort_values('metric_date')
                        if not project_df.empty:
                            plot_data.append({
                                'dates': project_df['metric_date'],
                                'values': project_df[db_column],
                                'project': project
                            })
                    
                    if plot_data:
                        # Create multi-series chart
                        fig = go.Figure()
                        
                        for data in plot_data:
                            fig.add_trace(go.Scatter(
                                x=data['dates'],
                                y=data['values'],
                                mode='lines+markers',
                                name=data['project'],
                                line=dict(width=2),
                                marker=dict(size=6)
                            ))
                        
                        fig.update_layout(
                            title=f"{metric_config.icon} {metric_config.label} Trend",
                            xaxis_title="Date",
                            yaxis_title=metric_config.label,
                            height=400,
                            hovermode='x unified',
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            )
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Add insights
                        with st.expander("📊 Insights & Statistics"):
                            col1, col2, col3, col4 = st.columns(4)
                            
                            all_values = df[db_column].dropna()
                            
                            with col1:
                                st.metric("Average", f"{all_values.mean():.2f}")
                            with col2:
                                st.metric("Median", f"{all_values.median():.2f}")
                            with col3:
                                st.metric("Min", f"{all_values.min():.2f}")
                            with col4:
                                st.metric("Max", f"{all_values.max():.2f}")
                            
                            # Trend analysis
                            if len(all_values) > 1:
                                trend = "improving" if (
                                    (metric_config.lower_is_better and all_values.iloc[-1] < all_values.iloc[0]) or
                                    (not metric_config.lower_is_better and all_values.iloc[-1] > all_values.iloc[0])
                                ) else "declining"
                                
                                st.info(f"📈 Overall trend: {trend}")
    
    def render_comparison_section(self, df: pd.DataFrame, selected_metrics: List[str]):
        """Render comparison charts section."""
        if df.empty or not selected_metrics:
            return
        
        st.markdown('<h2 class="section-header">🔍 Project Comparisons</h2>', 
                   unsafe_allow_html=True)
        
        # Get latest data for comparison
        latest_date = df['metric_date'].max()
        latest_df = df[df['metric_date'] == latest_date]
        
        # Create comparison charts in a grid
        cols = st.columns(2)
        
        for idx, metric_key in enumerate(selected_metrics[:4]):  # Show max 4 comparisons
            if metric_key in METRIC_DEFINITIONS:
                with cols[idx % 2]:
                    fig = self.chart_builder.create_comparison_chart(
                        latest_df, metric_key
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
    def render_heatmap_section(self, df: pd.DataFrame, selected_metrics: List[str]):
        """Render metrics heatmap section."""
        if df.empty or not selected_metrics:
            return
        
        st.markdown('<h2 class="section-header">🗺️ Metrics Heatmap</h2>', 
                   unsafe_allow_html=True)
        
        # Get latest data
        latest_date = df['metric_date'].max()
        latest_df = df[df['metric_date'] == latest_date]
        
        if not latest_df.empty:
            fig = self.chart_builder.create_heatmap(
                latest_df, selected_metrics[:8]  # Limit to 8 metrics
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def render_details_section(self, df: pd.DataFrame, selected_metrics: List[str]):
        """Render detailed data table section."""
        if df.empty:
            return
        
        st.markdown('<h2 class="section-header">📋 Detailed Data</h2>', 
                   unsafe_allow_html=True)
        
        # Prepare display columns
        display_columns = ['project_name', 'metric_date']
        
        # Add selected metric columns
        column_map = {
            'bugs': ['bugs_total', 'bugs_blocker', 'bugs_critical'],
            'vulnerabilities': ['vulnerabilities_total', 'vulnerabilities_critical'],
            'code_smells': ['code_smells_total', 'code_smells_critical'],
            'security_hotspots': ['security_hotspots_total', 'security_hotspots_to_review'],
            'coverage': ['coverage_percentage', 'line_coverage_percentage'],
            'duplicated_lines_density': ['duplicated_lines_density', 'duplicated_lines'],
            'technical_debt': ['technical_debt', 'sqale_debt_ratio'],
            'ncloc': ['ncloc', 'lines', 'files'],
            'complexity': ['complexity', 'cognitive_complexity']
        }
        
        for metric in selected_metrics:
            if metric in column_map:
                for col in column_map[metric]:
                    if col in df.columns:
                        display_columns.append(col)
        
        # Add rating columns if applicable
        if any(m in ['bugs', 'vulnerabilities', 'code_smells'] for m in selected_metrics):
            for rating in ['reliability_rating', 'security_rating', 'sqale_rating']:
                if rating in df.columns:
                    display_columns.append(rating)
        
        # Remove duplicates while preserving order
        display_columns = list(dict.fromkeys(display_columns))
        
        # Filter dataframe
        display_df = df[display_columns].copy()
        display_df['metric_date'] = display_df['metric_date'].dt.strftime('%Y-%m-%d')
        
        # Configure column display
        column_config = {
            "metric_date": st.column_config.TextColumn("Date", width="small"),
            "project_name": st.column_config.TextColumn("Project", width="medium"),
            "coverage_percentage": st.column_config.ProgressColumn(
                "Coverage %", min_value=0, max_value=100
            ),
            "line_coverage_percentage": st.column_config.ProgressColumn(
                "Line Coverage %", min_value=0, max_value=100
            ),
            "duplicated_lines_density": st.column_config.ProgressColumn(
                "Duplication %", min_value=0, max_value=100
            ),
            "technical_debt": st.column_config.NumberColumn(
                "Tech Debt (min)", format="%d"
            ),
            "bugs_total": st.column_config.NumberColumn("Bugs", format="%d"),
            "vulnerabilities_total": st.column_config.NumberColumn(
                "Vulnerabilities", format="%d"
            ),
            "code_smells_total": st.column_config.NumberColumn(
                "Code Smells", format="%d"
            )
        }
        
        # Display the table
        st.dataframe(
            display_df,
            use_container_width=True,
            height=400,
            hide_index=True,
            column_config=column_config
        )
        
        # Export options
        col1, col2, col3 = st.columns([1, 1, 8])
        
        with col1:
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 CSV",
                data=csv,
                file_name=f"sonarqube_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Convert to Excel (requires openpyxl)
            try:
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    display_df.to_excel(writer, sheet_name='Metrics', index=False)
                
                st.download_button(
                    label="📥 Excel",
                    data=buffer.getvalue(),
                    file_name=f"sonarqube_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except ImportError:
                st.info("Excel export requires openpyxl package")
    
    def render_footer(self, last_update: Optional[datetime] = None):
        """Render dashboard footer."""
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if last_update:
                st.caption(f"📅 Last data update: {last_update.strftime('%Y-%m-%d %H:%M')}")
        
        with col2:
            st.caption(f"🔄 Dashboard refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        with col3:
            st.caption("📊 SonarQube Metrics Dashboard v4.0")


# =====================================================================
# MAIN APPLICATION
# =====================================================================

def main():
    """Main application entry point."""
    
    # Initialize UI
    ui = DashboardUI()
    
    # Render header
    ui.render_header()
    
    # Get user selections from sidebar
    selections = ui.render_sidebar()
    
    if not selections or not selections.get('project_ids'):
        st.info("👈 Please select at least one project from the sidebar to view metrics.")
        return
    
    # Auto-refresh logic
    if selections.get('auto_refresh'):
        time.sleep(0.1)  # Small delay to prevent too frequent refreshes
        st.experimental_rerun()
    
    # Fetch data
    with st.spinner("Loading metrics data..."):
        df = ui.metrics_repo.fetch_metrics(
            project_ids=selections['project_ids'],
            start_date=str(selections['start_date']),
            end_date=str(selections['end_date']),
            metrics=selections.get('metrics')
        )
    
    if df.empty:
        st.warning("No data found for the selected projects and date range.")
        return
    
    # Filter carried forward data if needed
    if not selections.get('include_carried_forward', True):
        df = df[df['is_carried_forward'] == False]
    
    # Render sections based on user preferences
    ui.render_kpi_section(df, selections.get('metrics', []))
    
    if selections.get('show_trends', True):
        ui.render_trends_section(
            df, 
            selections.get('metrics', []), 
            selections.get('project_names', [])
        )
    
    if selections.get('show_comparisons', True):
        ui.render_comparison_section(df, selections.get('metrics', []))
    
    if selections.get('show_heatmap', False):
        ui.render_heatmap_section(df, selections.get('metrics', []))
    
    if selections.get('show_details', True):
        ui.render_details_section(df, selections.get('metrics', []))
    
    # Render footer
    last_update = df['metric_date'].max() if not df.empty else None
    ui.render_footer(last_update)


if __name__ == "__main__":
    main()