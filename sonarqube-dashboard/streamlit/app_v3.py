import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple, Any
import numpy as np

# Page configuration
st.set_page_config(
    page_title="SonarQube Metrics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Main container styling */
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(90deg, #1e3a8a 0%, #3b82f6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0 2rem 0;
    }
    
    /* KPI Card styling */
    .kpi-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border: 1px solid #e5e7eb;
        transition: all 0.3s ease;
    }
    
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.12);
    }
    
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin: 0.5rem 0;
    }
    
    .kpi-label {
        font-size: 0.875rem;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .kpi-delta {
        font-size: 0.875rem;
        font-weight: 500;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        display: inline-block;
        margin-top: 0.5rem;
    }
    
    .kpi-delta-positive {
        background: #d1fae5;
        color: #065f46;
    }
    
    .kpi-delta-negative {
        background: #fee2e2;
        color: #991b1b;
    }
    
    /* Rating badge styling */
    .rating-badge {
        display: inline-block;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 600;
        font-size: 1.25rem;
        text-align: center;
        min-width: 60px;
    }
    
    .rating-a { background: #22c55e; color: white; }
    .rating-b { background: #84cc16; color: white; }
    .rating-c { background: #eab308; color: white; }
    .rating-d { background: #f97316; color: white; }
    .rating-e { background: #ef4444; color: white; }
    
    /* Section headers */
    .section-header {
        font-size: 1.75rem;
        font-weight: 600;
        color: #1f2937;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e5e7eb;
    }
    
    /* Info box styling */
    .info-box {
        background: #f3f4f6;
        border-left: 4px solid #3b82f6;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    /* Metric trend indicator */
    .trend-up { color: #ef4444; }
    .trend-down { color: #22c55e; }
    .trend-neutral { color: #6b7280; }
</style>
""", unsafe_allow_html=True)

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres-metrics'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'sonarqube_metrics'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
}

# Metric configurations
METRIC_CONFIGS = {
    'bugs': {
        'label': 'Bugs',
        'icon': '🐛',
        'color': '#ef4444',
        'format': 'number',
        'lower_is_better': True
    },
    'vulnerabilities': {
        'label': 'Vulnerabilities',
        'icon': '🔓',
        'color': '#f97316',
        'format': 'number',
        'lower_is_better': True
    },
    'code_smells': {
        'label': 'Code Smells',
        'icon': '🌫️',
        'color': '#eab308',
        'format': 'number',
        'lower_is_better': True
    },
    'security_hotspots': {
        'label': 'Security Hotspots',
        'icon': '🔥',
        'color': '#dc2626',
        'format': 'number',
        'lower_is_better': True
    },
    'coverage': {
        'label': 'Coverage',
        'icon': '📊',
        'color': '#22c55e',
        'format': 'percentage',
        'lower_is_better': False
    },
    'duplicated_lines_density': {
        'label': 'Duplication',
        'icon': '📋',
        'color': '#6366f1',
        'format': 'percentage',
        'lower_is_better': True
    },
    'technical_debt': {
        'label': 'Technical Debt',
        'icon': '💰',
        'color': '#8b5cf6',
        'format': 'time',
        'lower_is_better': True
    },
    'ncloc': {
        'label': 'Lines of Code',
        'icon': '📝',
        'color': '#64748b',
        'format': 'number',
        'lower_is_better': False
    }
}

# Date range presets
DATE_RANGES = {
    'Last 7 days': 7,
    'Last 14 days': 14,
    'Last 30 days': 30,
    'Last 90 days': 90,
    'This Month': 'month',
    'This Quarter': 'quarter',
    'This Year': 'year'
}

def get_db_connection():
    """Create a fresh database connection."""
    return psycopg2.connect(**DB_PARAMS)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_projects():
    """Fetch all projects from the database."""
    query = """
    SELECT project_id, project_name, sonarqube_project_key, last_analysis_date_from_sq
    FROM sonarqube_metrics.sq_projects
    ORDER BY project_name
    """
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def fetch_metrics_data(project_ids: List[int], start_date: str, end_date: str):
    """Fetch metrics data for selected projects and date range."""
    query = """
    SELECT 
        m.*,
        p.project_name,
        p.sonarqube_project_key
    FROM sonarqube_metrics.daily_project_metrics m
    JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
    WHERE m.project_id = ANY(%s)
    AND m.metric_date BETWEEN %s AND %s
    ORDER BY m.metric_date DESC, p.project_name
    """
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn, params=(project_ids, start_date, end_date))
    conn.close()
    
    # Convert metric_date to datetime
    df['metric_date'] = pd.to_datetime(df['metric_date'])
    
    return df

def format_metric_value(value: float, format_type: str) -> str:
    """Format metric value based on type."""
    if pd.isna(value):
        return "N/A"
    
    if format_type == 'number':
        return f"{int(value):,}"
    elif format_type == 'percentage':
        return f"{value:.1f}%"
    elif format_type == 'time':
        # Convert minutes to hours/days
        hours = value / 60
        if hours < 24:
            return f"{hours:.1f}h"
        else:
            days = hours / 24
            return f"{days:.1f}d"
    else:
        return str(value)

def calculate_trend(current: float, previous: float, lower_is_better: bool) -> Tuple[float, str]:
    """Calculate trend percentage and direction."""
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return 0, 'neutral'
    
    change = ((current - previous) / previous) * 100
    
    if change > 0:
        direction = 'up' if lower_is_better else 'down'
    elif change < 0:
        direction = 'down' if lower_is_better else 'up'
    else:
        direction = 'neutral'
    
    return change, direction

def render_kpi_card(label: str, value: str, delta: float, direction: str, icon: str):
    """Render a KPI card with custom styling."""
    trend_icon = "↑" if direction == 'up' else "↓" if direction == 'down' else "→"
    trend_color = "negative" if direction == 'up' else "positive" if direction == 'down' else "neutral"
    
    html = f"""
    <div class="kpi-card">
        <div class="kpi-label">{icon} {label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-delta kpi-delta-{trend_color}">
            {trend_icon} {abs(delta):.1f}%
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_rating_badge(rating: str, label: str):
    """Render a rating badge."""
    if pd.isna(rating) or rating == 'N/A':
        rating_class = ''
        display_rating = 'N/A'
    else:
        rating_class = f'rating-{rating.lower()}'
        display_rating = rating
    
    html = f"""
    <div style="text-align: center;">
        <div class="kpi-label">{label}</div>
        <div class="rating-badge {rating_class}">{display_rating}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def create_trend_chart(df: pd.DataFrame, metric: str, config: Dict[str, Any]) -> go.Figure:
    """Create an interactive trend chart for a metric."""
    fig = go.Figure()
    
    # Add a trace for each project
    for project in df['project_name'].unique():
        project_df = df[df['project_name'] == project].sort_values('metric_date')
        
        # Handle metric column mapping
        if metric == 'bugs':
            metric_col = 'bugs_total'
        elif metric == 'vulnerabilities':
            metric_col = 'vulnerabilities_total'
        elif metric == 'code_smells':
            metric_col = 'code_smells_total'
        elif metric == 'security_hotspots':
            metric_col = 'security_hotspots_total'
        elif metric == 'coverage':
            metric_col = 'coverage_percentage'
        elif metric == 'technical_debt':
            metric_col = 'technical_debt'
        elif metric == 'ncloc':
            metric_col = 'ncloc'
        else:
            metric_col = metric
        
        fig.add_trace(go.Scatter(
            x=project_df['metric_date'],
            y=project_df[metric_col],
            mode='lines+markers',
            name=project,
            line=dict(width=2),
            marker=dict(size=6),
            hovertemplate=f"<b>{project}</b><br>" +
                         "Date: %{x}<br>" +
                         f"{config['label']}: %{{y}}<br>" +
                         "<extra></extra>"
        ))
    
    # Update layout
    fig.update_layout(
        title=f"{config['icon']} {config['label']} Trend",
        xaxis_title="Date",
        yaxis_title=config['label'],
        hovermode='x unified',
        height=400,
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#e5e7eb')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#e5e7eb')
    
    return fig

def main():
    # Header
    st.markdown('<h1 class="main-header">SonarQube Metrics Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # Project selection
        projects_df = fetch_projects()
        
        if projects_df.empty:
            st.error("No projects found in the database.")
            return
        
        # Create project options
        project_options = {
            row['project_name']: row['project_id'] 
            for _, row in projects_df.iterrows()
        }
        
        selected_projects = st.multiselect(
            "Select Projects",
            options=list(project_options.keys()),
            default=list(project_options.keys())[:3] if len(project_options) >= 3 else list(project_options.keys()),
            help="Choose one or more projects to analyze"
        )
        
        selected_project_ids = [project_options[name] for name in selected_projects]
        
        st.divider()
        
        # Date range selection
        st.subheader("📅 Date Range")
        
        date_preset = st.selectbox(
            "Quick Select",
            options=list(DATE_RANGES.keys()),
            index=2  # Default to Last 30 days
        )
        
        # Calculate dates based on preset
        end_date = datetime.now().date()
        if isinstance(DATE_RANGES[date_preset], int):
            start_date = end_date - timedelta(days=DATE_RANGES[date_preset])
        else:
            if DATE_RANGES[date_preset] == 'month':
                start_date = end_date.replace(day=1)
            elif DATE_RANGES[date_preset] == 'quarter':
                quarter_month = ((end_date.month - 1) // 3) * 3 + 1
                start_date = end_date.replace(month=quarter_month, day=1)
            else:  # year
                start_date = end_date.replace(month=1, day=1)
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=start_date)
        with col2:
            end_date = st.date_input("End Date", value=end_date)
        
        st.divider()
        
        # Metric selection
        st.subheader("📊 Metrics")
        
        selected_metrics = st.multiselect(
            "Select Metrics to Display",
            options=list(METRIC_CONFIGS.keys()),
            default=['bugs', 'vulnerabilities', 'code_smells', 'coverage'],
            format_func=lambda x: f"{METRIC_CONFIGS[x]['icon']} {METRIC_CONFIGS[x]['label']}",
            help="Choose which metrics to display in the dashboard"
        )
        
        st.divider()
        
        # Additional filters
        st.subheader("🎯 Filters")
        
        code_scope = st.radio(
            "Code Scope",
            options=["Overall Code", "New Code", "Both"],
            index=0,
            help="View metrics for overall code, new code, or both"
        )
        
        show_carried_forward = st.checkbox(
            "Show Carried Forward Data",
            value=True,
            help="Include carried forward metrics when actual data is not available"
        )
    
    # Main content area
    if not selected_project_ids:
        st.info("👈 Please select at least one project from the sidebar to view metrics.")
        return
    
    # Fetch data
    with st.spinner("Loading metrics data..."):
        df = fetch_metrics_data(selected_project_ids, str(start_date), str(end_date))
    
    if df.empty:
        st.warning("No data found for the selected projects and date range.")
        return
    
    # Filter out carried forward data if requested
    if not show_carried_forward:
        df = df[df['is_carried_forward'] == False]
    
    # Get latest data for KPIs
    latest_date = df['metric_date'].max()
    latest_df = df[df['metric_date'] == latest_date]
    
    # Calculate comparison date for trends
    date_diff = (end_date - start_date).days
    comparison_date = start_date - timedelta(days=date_diff)
    previous_df = df[df['metric_date'] == pd.Timestamp(comparison_date)]
    
    # Section 1: KPI Cards
    st.markdown('<div class="section-header">📈 Key Performance Indicators</div>', unsafe_allow_html=True)
    
    # Create columns for KPI cards
    num_metrics = len(selected_metrics)
    cols_per_row = min(4, num_metrics)
    
    for i in range(0, num_metrics, cols_per_row):
        cols = st.columns(cols_per_row)
        
        for j, col in enumerate(cols):
            if i + j < num_metrics:
                metric = selected_metrics[i + j]
                config = METRIC_CONFIGS[metric]
                
                with col:
                    # Calculate aggregated values
                    if metric == 'coverage':
                        # For coverage, use weighted average
                        current_value = (latest_df['covered_lines'].sum() / 
                                       latest_df['lines_to_cover'].sum() * 100) if latest_df['lines_to_cover'].sum() > 0 else 0
                    elif metric == 'duplicated_lines_density':
                        # For duplication, use average
                        current_value = latest_df['duplicated_lines_density'].mean()
                    elif metric == 'bugs':
                        current_value = latest_df['bugs_total'].sum()
                    elif metric == 'vulnerabilities':
                        current_value = latest_df['vulnerabilities_total'].sum()
                    elif metric == 'code_smells':
                        current_value = latest_df['code_smells_total'].sum()
                    elif metric == 'security_hotspots':
                        current_value = latest_df['security_hotspots_total'].sum()
                    elif metric == 'technical_debt':
                        current_value = latest_df['technical_debt'].sum()
                    elif metric == 'ncloc':
                        current_value = latest_df['ncloc'].sum()
                    else:
                        current_value = 0
                    
                    # Calculate previous value
                    if not previous_df.empty:
                        if metric == 'coverage':
                            previous_value = (previous_df['covered_lines'].sum() / 
                                           previous_df['lines_to_cover'].sum() * 100) if previous_df['lines_to_cover'].sum() > 0 else 0
                        elif metric == 'duplicated_lines_density':
                            previous_value = previous_df['duplicated_lines_density'].mean()
                        elif metric == 'bugs':
                            previous_value = previous_df['bugs_total'].sum()
                        elif metric == 'vulnerabilities':
                            previous_value = previous_df['vulnerabilities_total'].sum()
                        elif metric == 'code_smells':
                            previous_value = previous_df['code_smells_total'].sum()
                        elif metric == 'security_hotspots':
                            previous_value = previous_df['security_hotspots_total'].sum()
                        elif metric == 'technical_debt':
                            previous_value = previous_df['technical_debt'].sum()
                        elif metric == 'ncloc':
                            previous_value = previous_df['ncloc'].sum()
                        else:
                            previous_value = 0
                    else:
                        previous_value = current_value
                    
                    # Format value
                    formatted_value = format_metric_value(current_value, config['format'])
                    
                    # Calculate trend
                    delta, direction = calculate_trend(current_value, previous_value, config['lower_is_better'])
                    
                    # Render KPI card
                    render_kpi_card(config['label'], formatted_value, delta, direction, config['icon'])
    
    # Add spacing
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Quality Ratings
    if len(selected_projects) > 0:
        st.markdown('<div class="section-header">⭐ Quality Ratings</div>', unsafe_allow_html=True)
        
        rating_cols = st.columns(4)
        
        with rating_cols[0]:
            # Aggregate rating (take worst rating)
            reliability_ratings = latest_df['reliability_rating'].dropna()
            if not reliability_ratings.empty:
                worst_rating = max(reliability_ratings)
                render_rating_badge(worst_rating, "Reliability")
            else:
                render_rating_badge("N/A", "Reliability")
        
        with rating_cols[1]:
            security_ratings = latest_df['security_rating'].dropna()
            if not security_ratings.empty:
                worst_rating = max(security_ratings)
                render_rating_badge(worst_rating, "Security")
            else:
                render_rating_badge("N/A", "Security")
        
        with rating_cols[2]:
            maintainability_ratings = latest_df['sqale_rating'].dropna()
            if not maintainability_ratings.empty:
                worst_rating = max(maintainability_ratings)
                render_rating_badge(worst_rating, "Maintainability")
            else:
                render_rating_badge("N/A", "Maintainability")
        
        with rating_cols[3]:
            security_review_ratings = latest_df['security_review_rating'].dropna()
            if not security_review_ratings.empty:
                worst_rating = max(security_review_ratings)
                render_rating_badge(worst_rating, "Security Review")
            else:
                render_rating_badge("N/A", "Security Review")
    
    # Section 2: Trends Over Time
    st.markdown('<div class="section-header">📊 Trends Over Time</div>', unsafe_allow_html=True)
    
    # Create tabs for different metric categories
    trend_tabs = st.tabs([f"{METRIC_CONFIGS[m]['icon']} {METRIC_CONFIGS[m]['label']}" for m in selected_metrics])
    
    for i, (tab, metric) in enumerate(zip(trend_tabs, selected_metrics)):
        with tab:
            config = METRIC_CONFIGS[metric]
            
            # Create the trend chart
            fig = create_trend_chart(df, metric, config)
            st.plotly_chart(fig, use_container_width=True)
            
            # Add insight box
            with st.expander("📊 Insights"):
                col1, col2, col3 = st.columns(3)
                
                # Get correct metric column
                if metric == 'bugs':
                    metric_col = 'bugs_total'
                elif metric == 'vulnerabilities':
                    metric_col = 'vulnerabilities_total'
                elif metric == 'code_smells':
                    metric_col = 'code_smells_total'
                elif metric == 'security_hotspots':
                    metric_col = 'security_hotspots_total'
                elif metric == 'coverage':
                    metric_col = 'coverage_percentage'
                elif metric == 'technical_debt':
                    metric_col = 'technical_debt'
                elif metric == 'ncloc':
                    metric_col = 'ncloc'
                else:
                    metric_col = metric
                
                with col1:
                    avg_value = df.groupby('metric_date')[metric_col].sum().mean()
                    st.metric("Average", format_metric_value(avg_value, config['format']))
                
                with col2:
                    max_value = df.groupby('metric_date')[metric_col].sum().max()
                    st.metric("Maximum", format_metric_value(max_value, config['format']))
                
                with col3:
                    min_value = df.groupby('metric_date')[metric_col].sum().min()
                    st.metric("Minimum", format_metric_value(min_value, config['format']))
    
    # Section 3: Detailed Data Table
    st.markdown('<div class="section-header">📋 Detailed Metrics Data</div>', unsafe_allow_html=True)
    
    # Prepare data for display
    display_df = df.copy()
    
    # Select relevant columns based on selected metrics and code scope
    base_columns = ['project_name', 'metric_date']
    metric_columns = []
    
    # Determine which columns to show based on code scope
    for metric in selected_metrics:
        if code_scope in ["Overall Code", "Both"]:
            # Add overall code metrics
            if metric == 'bugs':
                metric_columns.extend(['bugs_total', 'bugs_blocker', 'bugs_critical', 'bugs_major', 'bugs_minor', 'bugs_info'])
            elif metric == 'vulnerabilities':
                metric_columns.extend(['vulnerabilities_total', 'vulnerabilities_blocker', 'vulnerabilities_critical', 'vulnerabilities_high', 'vulnerabilities_medium', 'vulnerabilities_low'])
            elif metric == 'code_smells':
                metric_columns.extend(['code_smells_total', 'code_smells_blocker', 'code_smells_critical', 'code_smells_major', 'code_smells_minor', 'code_smells_info'])
            elif metric == 'security_hotspots':
                metric_columns.extend(['security_hotspots_total', 'security_hotspots_to_review', 'security_hotspots_acknowledged'])
            elif metric == 'coverage':
                metric_columns.extend(['coverage_percentage', 'line_coverage_percentage', 'branch_coverage_percentage'])
            elif metric == 'duplicated_lines_density':
                metric_columns.extend(['duplicated_lines_density', 'duplicated_lines', 'duplicated_blocks'])
            elif metric == 'technical_debt':
                metric_columns.extend(['technical_debt', 'sqale_debt_ratio'])
            elif metric == 'ncloc':
                metric_columns.extend(['ncloc', 'lines', 'classes', 'functions'])
        
        if code_scope in ["New Code", "Both"]:
            # Add new code metrics
            if metric == 'bugs':
                new_cols = ['new_code_bugs_total', 'new_code_bugs_blocker', 'new_code_bugs_critical', 'new_code_bugs_major', 'new_code_bugs_minor', 'new_code_bugs_info']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
            elif metric == 'vulnerabilities':
                new_cols = ['new_code_vulnerabilities_total', 'new_code_vulnerabilities_blocker', 'new_code_vulnerabilities_critical', 'new_code_vulnerabilities_high', 'new_code_vulnerabilities_medium', 'new_code_vulnerabilities_low']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
            elif metric == 'code_smells':
                new_cols = ['new_code_code_smells_total', 'new_code_code_smells_blocker', 'new_code_code_smells_critical', 'new_code_code_smells_major', 'new_code_code_smells_minor', 'new_code_code_smells_info']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
            elif metric == 'security_hotspots':
                new_cols = ['new_code_security_hotspots_total', 'new_code_security_hotspots_to_review']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
            elif metric == 'coverage':
                new_cols = ['new_code_coverage_percentage', 'new_code_line_coverage_percentage', 'new_code_branch_coverage_percentage']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
            elif metric == 'duplicated_lines_density':
                new_cols = ['new_code_duplicated_lines_density', 'new_code_duplicated_lines', 'new_code_duplicated_blocks']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
            elif metric == 'technical_debt':
                new_cols = ['new_code_technical_debt', 'new_code_sqale_debt_ratio']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
            elif metric == 'ncloc':
                new_cols = ['new_code_ncloc', 'new_code_lines']
                metric_columns.extend([col for col in new_cols if col in display_df.columns])
    
    # Add rating columns if any quality metric is selected
    if any(m in ['bugs', 'vulnerabilities', 'code_smells'] for m in selected_metrics) and code_scope in ["Overall Code", "Both"]:
        metric_columns.extend(['reliability_rating', 'security_rating', 'sqale_rating'])
    
    # Add carried forward indicator if show_carried_forward is True
    if show_carried_forward and 'is_carried_forward' in display_df.columns:
        metric_columns.append('is_carried_forward')
    
    # Filter columns that exist in the dataframe
    available_columns = [col for col in base_columns + metric_columns if col in display_df.columns]
    display_df = display_df[available_columns]
    
    # Format date column
    display_df['metric_date'] = display_df['metric_date'].dt.strftime('%Y-%m-%d')
    
    # Apply sidebar filters directly (no additional filters needed)
    filtered_df = display_df.copy()
    
    # Apply carried forward filter from sidebar
    if not show_carried_forward and 'is_carried_forward' in df.columns:
        carried_forward_mask = df[df.index.isin(filtered_df.index)]['is_carried_forward'] == False
        filtered_df = filtered_df[carried_forward_mask]
    
    # Display the data table
    st.dataframe(
        filtered_df,
        use_container_width=True,
        height=400,
        hide_index=True,
        column_config={
            "metric_date": st.column_config.TextColumn("Date", width="small"),
            "project_name": st.column_config.TextColumn("Project", width="medium"),
            # Overall code coverage metrics
            "coverage_percentage": st.column_config.ProgressColumn("Coverage %", min_value=0, max_value=100),
            "line_coverage_percentage": st.column_config.ProgressColumn("Line Coverage %", min_value=0, max_value=100),
            "branch_coverage_percentage": st.column_config.ProgressColumn("Branch Coverage %", min_value=0, max_value=100),
            "duplicated_lines_density": st.column_config.ProgressColumn("Duplication %", min_value=0, max_value=100),
            # New code coverage metrics
            "new_code_coverage_percentage": st.column_config.ProgressColumn("New Code Coverage %", min_value=0, max_value=100),
            "new_code_line_coverage_percentage": st.column_config.ProgressColumn("New Code Line Coverage %", min_value=0, max_value=100),
            "new_code_branch_coverage_percentage": st.column_config.ProgressColumn("New Code Branch Coverage %", min_value=0, max_value=100),
            "new_code_duplicated_lines_density": st.column_config.ProgressColumn("New Code Duplication %", min_value=0, max_value=100),
            # Technical debt formatting
            "technical_debt": st.column_config.NumberColumn("Tech Debt (min)", format="%d"),
            "new_code_technical_debt": st.column_config.NumberColumn("New Code Tech Debt (min)", format="%d"),
            # Carried forward indicator
            "is_carried_forward": st.column_config.CheckboxColumn("Carried Forward", width="small"),
        }
    )
    
    # Export options
    col1, col2 = st.columns([1, 5])
    with col1:
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"sonarqube_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # Footer with last update info
    st.divider()
    st.markdown(
        f"<div style='text-align: center; color: #6b7280; font-size: 0.875rem;'>"
        f"Last data update: {latest_date.strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Dashboard refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f"</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()