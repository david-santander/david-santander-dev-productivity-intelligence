import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
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

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'sonarqube_metrics'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# Metric categories
METRIC_CATEGORIES = {
    'Security': ['Vulnerabilities', 'Security Hotspots'],
    'Reliability': ['Bugs'],
    'Maintainability': ['Code Smells', 'Duplications'],
    'Coverage': ['Coverage']
}

# Date range presets
DATE_RANGES = {
    'Last 7 days': 7,
    'Last 30 days': 30,
    'This Week': 'week',
    'This Month': 'month',
    'This Quarter': 'quarter',
    'This Year': 'year'
}

@st.cache_resource
def get_connection_pool():
    """Create and return a connection pool"""
    return pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        **DB_CONFIG
    )

def get_db_connection():
    """Get connection from pool"""
    pool_obj = get_connection_pool()
    return pool_obj.getconn()

def return_connection(conn):
    """Return connection to pool"""
    pool_obj = get_connection_pool()
    pool_obj.putconn(conn)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_projects() -> List[Dict]:
    """Fetch all projects from database"""
    query = """
        SELECT project_id, sonarqube_project_key, project_name
        FROM sonarqube_metrics.sq_projects
        ORDER BY project_name
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()
    finally:
        return_connection(conn)

@st.cache_data(ttl=300)
def fetch_metrics_data(project_ids: List[int], start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch metrics data for selected projects and date range"""
    query = """
        SELECT 
            p.project_name,
            p.sonarqube_project_key,
            m.*
        FROM sonarqube_metrics.daily_project_metrics m
        JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
        WHERE m.project_id = ANY(%s)
        AND m.metric_date BETWEEN %s AND %s
        ORDER BY m.metric_date DESC, p.project_name
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn, params=[project_ids, start_date, end_date])
        return df
    finally:
        return_connection(conn)

def calculate_trend(current_value: float, previous_value: float, threshold: float = 2.0) -> Tuple[str, str]:
    """Calculate trend indicator and direction"""
    if previous_value == 0:
        if current_value > 0:
            return "↑", "up"
        else:
            return "→", "still"
    
    change_percent = ((current_value - previous_value) / previous_value) * 100
    
    if abs(change_percent) <= threshold:
        return "→", "still"
    elif change_percent > 0:
        return "↑", "up"
    else:
        return "↓", "down"

def get_metric_value(df: pd.DataFrame, metric_name: str, severity: str = None, status: str = None, code_scope: str = "Overall Code") -> float:
    """Get the latest metric value from dataframe"""
    if df.empty:
        return 0
    
    latest = df.iloc[0]
    
    # Determine column prefix based on code scope
    prefix = "new_code_" if code_scope == "New Code" else ""
    
    if metric_name == 'Bugs':
        if severity:
            return latest[f'{prefix}bugs_{severity.lower()}']
        elif status:
            return latest[f'bugs_{status.lower()}'] if code_scope == "Overall Code" else 0
        else:
            return latest[f'{prefix}bugs_total']
    elif metric_name == 'Vulnerabilities':
        if severity:
            return latest[f'{prefix}vulnerabilities_{severity.lower()}']
        elif status:
            return latest[f'vulnerabilities_{status.lower()}'] if code_scope == "Overall Code" else 0
        else:
            return latest[f'{prefix}vulnerabilities_total']
    elif metric_name == 'Code Smells':
        if severity:
            return latest[f'{prefix}code_smells_{severity.lower()}']
        else:
            return latest[f'{prefix}code_smells_total']
    elif metric_name == 'Security Hotspots':
        if severity:
            return latest[f'{prefix}security_hotspots_{severity.lower()}']
        elif status:
            col_name = f'{prefix}security_hotspots_{status.lower().replace(" ", "_")}'
            return latest[col_name] if col_name in latest else 0
        else:
            return latest[f'{prefix}security_hotspots_total']
    elif metric_name == 'Coverage':
        return latest[f'{prefix}coverage_percentage']
    elif metric_name == 'Duplications':
        return latest[f'{prefix}duplicated_lines_density']
    elif metric_name == 'New Lines' and code_scope == "New Code":
        return latest.get('new_code_lines', 0)
    
    return 0

def render_kpi_card(title: str, value: float, trend_indicator: str, trend_direction: str, is_percentage: bool = False):
    """Render a KPI card with value and trend"""
    color_map = {
        'up': '#ff4b4b',  # Red for metrics where increase is bad
        'down': '#00cc00',  # Green for metrics where decrease is good
        'still': '#ffa500'  # Orange for no change
    }
    
    # Reverse colors for coverage (higher is better)
    if title == 'Coverage':
        color_map['up'] = '#00cc00'
        color_map['down'] = '#ff4b4b'
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.metric(
            label=title,
            value=f"{value:.1f}%" if is_percentage else f"{int(value)}",
            delta=None  # We'll use custom trend display
        )
    with col2:
        st.markdown(
            f"<div style='font-size: 2em; color: {color_map[trend_direction]}; text-align: center; margin-top: 10px;'>{trend_indicator}</div>",
            unsafe_allow_html=True
        )

def main():
    st.title("🎯 SonarQube Metrics & Trends Dashboard")
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Code scope toggle
    code_scope = st.sidebar.radio(
        "📊 Metrics Scope",
        options=["Overall Code", "New Code", "Comparison"],
        help="Choose whether to view metrics for overall code, new code only, or a comparison"
    )
    
    # Fetch available projects
    projects = fetch_projects()
    project_options = {p['project_name']: p['project_id'] for p in projects}
    
    # Project selection
    selected_project_names = st.sidebar.multiselect(
        "Select Projects",
        options=list(project_options.keys()),
        default=list(project_options.keys())
    )
    
    selected_project_ids = [project_options[name] for name in selected_project_names]
    
    # Date range selection
    date_range_option = st.sidebar.selectbox(
        "Select Date Range",
        options=list(DATE_RANGES.keys()) + ['Custom Range']
    )
    
    if date_range_option == 'Custom Range':
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
    else:
        end_date = datetime.now().date()
        if date_range_option in ['Last 7 days', 'Last 30 days']:
            start_date = end_date - timedelta(days=DATE_RANGES[date_range_option])
        elif DATE_RANGES[date_range_option] == 'week':
            start_date = end_date - timedelta(days=end_date.weekday())
        elif DATE_RANGES[date_range_option] == 'month':
            start_date = end_date.replace(day=1)
        elif DATE_RANGES[date_range_option] == 'quarter':
            quarter_month = ((end_date.month - 1) // 3) * 3 + 1
            start_date = end_date.replace(month=quarter_month, day=1)
        elif DATE_RANGES[date_range_option] == 'year':
            start_date = end_date.replace(month=1, day=1)
    
    # Category filter
    selected_categories = st.sidebar.multiselect(
        "Select Metric Categories",
        options=list(METRIC_CATEGORIES.keys()),
        default=list(METRIC_CATEGORIES.keys())
    )
    
    # Get selected metrics based on categories
    selected_metrics = []
    for category in selected_categories:
        selected_metrics.extend(METRIC_CATEGORIES[category])
    
    # Metric-specific filters
    st.sidebar.header("📋 Metric-Specific Filters")
    
    # Bugs filters
    if 'Bugs' in selected_metrics:
        st.sidebar.subheader("Bugs")
        bug_severities = st.sidebar.multiselect(
            "Severity",
            options=['Blocker', 'Critical', 'Major', 'Minor'],
            key='bug_severity'
        )
        bug_statuses = st.sidebar.multiselect(
            "Status",
            options=['Open', 'Confirmed', 'Reopened'],
            key='bug_status'
        )
    
    # Vulnerabilities filters
    if 'Vulnerabilities' in selected_metrics:
        st.sidebar.subheader("Vulnerabilities")
        vuln_severities = st.sidebar.multiselect(
            "Severity",
            options=['Critical', 'High', 'Medium', 'Low'],
            key='vuln_severity'
        )
        vuln_statuses = st.sidebar.multiselect(
            "Status",
            options=['Open', 'Confirmed', 'Reopened'],
            key='vuln_status'
        )
    
    # Code Smells filters
    if 'Code Smells' in selected_metrics:
        st.sidebar.subheader("Code Smells")
        smell_severities = st.sidebar.multiselect(
            "Severity",
            options=['Blocker', 'Critical', 'Major', 'Minor'],
            key='smell_severity'
        )
    
    # Security Hotspots filters
    if 'Security Hotspots' in selected_metrics:
        st.sidebar.subheader("Security Hotspots")
        hotspot_severities = st.sidebar.multiselect(
            "Severity",
            options=['High', 'Medium', 'Low'],
            key='hotspot_severity'
        )
        hotspot_statuses = st.sidebar.multiselect(
            "Status",
            options=['To Review', 'Reviewed', 'Acknowledged', 'Fixed'],
            key='hotspot_status'
        )
    
    # Fetch data
    if selected_project_ids:
        df = fetch_metrics_data(selected_project_ids, str(start_date), str(end_date))
        
        if not df.empty:
            # Ensure metric_date is datetime type
            df['metric_date'] = pd.to_datetime(df['metric_date'])
            
            # Calculate comparison date for trends
            date_diff = (end_date - start_date).days
            comparison_date = pd.Timestamp(start_date - timedelta(days=date_diff))
            
            # Section 1: KPI Cards
            if code_scope == "Comparison":
                st.header("📊 Overall vs New Code Comparison")
                
                # For comparison view, show metrics in pairs
                for metric in selected_metrics:
                    col1, col2, col3 = st.columns([2, 2, 1])
                    
                    # Overall Code
                    with col1:
                        st.subheader(f"Overall {metric}")
                        current_df = df[df['metric_date'] == df['metric_date'].max()]
                        current_value = get_metric_value(current_df, metric, code_scope="Overall Code")
                        previous_df = df[df['metric_date'] <= comparison_date]
                        if not previous_df.empty:
                            previous_value = get_metric_value(previous_df, metric, code_scope="Overall Code")
                        else:
                            previous_value = current_value
                        trend_indicator, trend_direction = calculate_trend(current_value, previous_value)
                        is_percentage = metric in ['Coverage', 'Duplications']
                        render_kpi_card(f"Overall {metric}", current_value, trend_indicator, trend_direction, is_percentage)
                    
                    # New Code
                    with col2:
                        st.subheader(f"New Code {metric}")
                        current_value_new = get_metric_value(current_df, metric, code_scope="New Code")
                        if not previous_df.empty:
                            previous_value_new = get_metric_value(previous_df, metric, code_scope="New Code")
                        else:
                            previous_value_new = current_value_new
                        trend_indicator_new, trend_direction_new = calculate_trend(current_value_new, previous_value_new)
                        render_kpi_card(f"New {metric}", current_value_new, trend_indicator_new, trend_direction_new, is_percentage)
                    
                    # Comparison Info
                    with col3:
                        st.subheader("New Lines")
                        new_lines = get_metric_value(current_df, "New Lines", code_scope="New Code")
                        st.metric("Lines Added", f"{int(new_lines):,}")
                        
                        if current_value > 0:
                            new_percentage = (current_value_new / current_value) * 100
                            st.metric("New/Overall %", f"{new_percentage:.1f}%")
            else:
                st.header(f"📊 {code_scope} Key Performance Indicators")
                
                # Create columns for KPI cards
                cols = st.columns(len(selected_metrics))
                
                for idx, metric in enumerate(selected_metrics):
                    with cols[idx]:
                        # Get current and previous values
                        current_df = df[df['metric_date'] == df['metric_date'].max()]
                        
                        if len(selected_project_ids) > 1:
                            # Aggregate across projects
                            current_value = current_df.apply(lambda row: get_metric_value(pd.DataFrame([row]), metric, code_scope=code_scope), axis=1).sum()
                        else:
                            current_value = get_metric_value(current_df, metric, code_scope=code_scope)
                        
                        # Get previous value for trend
                        previous_df = df[df['metric_date'] <= comparison_date]
                        if not previous_df.empty:
                            if len(selected_project_ids) > 1:
                                previous_value = previous_df.iloc[0:len(selected_project_ids)].apply(
                                    lambda row: get_metric_value(pd.DataFrame([row]), metric, code_scope=code_scope), axis=1
                                ).sum()
                            else:
                                previous_value = get_metric_value(previous_df, metric, code_scope=code_scope)
                        else:
                            previous_value = current_value
                        
                        # Calculate trend
                        trend_indicator, trend_direction = calculate_trend(current_value, previous_value)
                        
                        # Render KPI card
                        is_percentage = metric in ['Coverage', 'Duplications']
                        render_kpi_card(metric, current_value, trend_indicator, trend_direction, is_percentage)
            
            # Section 2: Trends Over Time
            st.header("📈 Trends Over Time")
            
            # Create tabs for each metric
            metric_tabs = st.tabs(selected_metrics)
            
            for idx, (metric, tab) in enumerate(zip(selected_metrics, metric_tabs)):
                with tab:
                    # Prepare data for trend chart
                    trend_data = []
                    
                    for _, project_group in df.groupby('sonarqube_project_key'):
                        project_name = project_group.iloc[0]['project_name']
                        dates = pd.to_datetime(project_group['metric_date']).dt.date
                        values = project_group.apply(lambda row: get_metric_value(pd.DataFrame([row]), metric, code_scope=code_scope), axis=1)
                        
                        for date, value in zip(dates, values):
                            trend_data.append({
                                'Date': date,
                                'Value': value,
                                'Project': project_name
                            })
                    
                    trend_df = pd.DataFrame(trend_data)
                    
                    if not trend_df.empty:
                        fig = px.line(
                            trend_df,
                            x='Date',
                            y='Value',
                            color='Project',
                            title=f'{metric} Trend Over Time',
                            markers=True
                        )
                        
                        fig.update_layout(
                            xaxis_title="Date",
                            yaxis_title="Value" if metric not in ['Coverage', 'Duplications'] else "Percentage",
                            hovermode='x unified'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            
            # Section 3: Project Comparison
            st.header("🏆 Project Comparison")
            
            comparison_tabs = st.tabs(selected_metrics)
            
            for idx, (metric, tab) in enumerate(zip(selected_metrics, comparison_tabs)):
                with tab:
                    # Get latest values for each project
                    latest_date = df['metric_date'].max()
                    latest_df = df[df['metric_date'] == latest_date]
                    
                    comparison_data = []
                    for _, row in latest_df.iterrows():
                        value = get_metric_value(pd.DataFrame([row]), metric)
                        comparison_data.append({
                            'Project': row['project_name'],
                            'Value': value
                        })
                    
                    comparison_df = pd.DataFrame(comparison_data)
                    
                    if not comparison_df.empty:
                        # Sort by value for better visualization
                        comparison_df = comparison_df.sort_values('Value', ascending=True)
                        
                        fig = px.bar(
                            comparison_df,
                            x='Value',
                            y='Project',
                            orientation='h',
                            title=f'{metric} by Project',
                            color='Value',
                            color_continuous_scale='Reds' if metric != 'Coverage' else 'Greens'
                        )
                        
                        fig.update_layout(
                            xaxis_title="Value" if metric not in ['Coverage', 'Duplications'] else "Percentage",
                            yaxis_title="Project",
                            showlegend=False
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            
            # Section 4: Data Table
            st.header("📋 Raw Data Table")
            
            # Prepare display columns
            display_columns = ['project_name', 'metric_date']
            
            # Add metric columns based on selection and filters
            for metric in selected_metrics:
                if metric == 'Bugs':
                    display_columns.append('bugs_total')
                    if 'bug_severities' in locals() and bug_severities:
                        for sev in bug_severities:
                            display_columns.append(f'bugs_{sev.lower()}')
                    if 'bug_statuses' in locals() and bug_statuses:
                        for status in bug_statuses:
                            display_columns.append(f'bugs_{status.lower()}')
                            
                elif metric == 'Vulnerabilities':
                    display_columns.append('vulnerabilities_total')
                    if 'vuln_severities' in locals() and vuln_severities:
                        for sev in vuln_severities:
                            display_columns.append(f'vulnerabilities_{sev.lower()}')
                    if 'vuln_statuses' in locals() and vuln_statuses:
                        for status in vuln_statuses:
                            display_columns.append(f'vulnerabilities_{status.lower()}')
                            
                elif metric == 'Code Smells':
                    display_columns.append('code_smells_total')
                    if 'smell_severities' in locals() and smell_severities:
                        for sev in smell_severities:
                            display_columns.append(f'code_smells_{sev.lower()}')
                            
                elif metric == 'Security Hotspots':
                    display_columns.append('security_hotspots_total')
                    if 'hotspot_severities' in locals() and hotspot_severities:
                        for sev in hotspot_severities:
                            display_columns.append(f'security_hotspots_{sev.lower()}')
                    if 'hotspot_statuses' in locals() and hotspot_statuses:
                        for status in hotspot_statuses:
                            status_key = status.lower().replace(' ', '_')
                            display_columns.append(f'security_hotspots_{status_key}')
                            
                elif metric == 'Coverage':
                    display_columns.append('coverage_percentage')
                    
                elif metric == 'Duplications':
                    display_columns.append('duplicated_lines_density')
            
            # Add metadata columns
            display_columns.extend(['is_carried_forward', 'data_source_timestamp'])
            
            # Filter columns that exist in the dataframe
            available_columns = [col for col in display_columns if col in df.columns]
            
            # Display the filtered dataframe
            display_df = df[available_columns].sort_values(['metric_date', 'project_name'], ascending=[False, True])
            
            # Rename columns for better display
            column_rename = {
                'project_name': 'Project',
                'metric_date': 'Date',
                'bugs_total': 'Total Bugs',
                'vulnerabilities_total': 'Total Vulnerabilities',
                'code_smells_total': 'Total Code Smells',
                'security_hotspots_total': 'Total Security Hotspots',
                'coverage_percentage': 'Coverage %',
                'duplicated_lines_density': 'Duplication %',
                'is_carried_forward': 'Carried Forward',
                'data_source_timestamp': 'Last Updated'
            }
            
            display_df = display_df.rename(columns=column_rename)
            
            # Show the dataframe
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # CSV download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"sonarqube_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
        else:
            st.warning("No data available for the selected projects and date range.")
    else:
        st.info("Please select at least one project from the sidebar.")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
        SonarQube Metrics Dashboard | Data refreshed nightly via Apache Airflow
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()