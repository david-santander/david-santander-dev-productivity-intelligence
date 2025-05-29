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
import json

# Page configuration
st.set_page_config(
    page_title="SonarQube Metrics Dashboard v2",
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

# Enhanced metric categories
METRIC_CATEGORIES = {
    'Security': ['Vulnerabilities', 'Security Hotspots', 'Security Review'],
    'Reliability': ['Bugs'],
    'Maintainability': ['Code Smells', 'Technical Debt', 'Duplications'],
    'Coverage': ['Coverage'],
    'Quality Gate': ['Quality Gate Status']
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

# Rating color map
RATING_COLORS = {
    'A': '#00aa00',
    'B': '#80cc00',
    'C': '#ffee00',
    'D': '#ff8800',
    'E': '#ff0000'
}

# Quality gate status colors
QG_STATUS_COLORS = {
    'OK': '#00aa00',
    'WARN': '#ff8800',
    'ERROR': '#ff0000'
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

@st.cache_data(ttl=300)
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

def render_quality_gate_badge(status: str):
    """Render quality gate status badge"""
    if pd.isna(status):
        status = 'N/A'
        color = '#808080'
    else:
        color = QG_STATUS_COLORS.get(status, '#808080')
    
    st.markdown(f"""
        <div style='display: inline-block; padding: 5px 10px; border-radius: 5px; 
                    background-color: {color}; color: white; font-weight: bold;'>
            Quality Gate: {status}
        </div>
    """, unsafe_allow_html=True)

def render_rating_badge(rating: str, label: str):
    """Render rating badge"""
    if pd.isna(rating):
        rating = '?'
        color = '#808080'
    else:
        color = RATING_COLORS.get(rating, '#808080')
    
    st.markdown(f"""
        <div style='display: inline-block; padding: 5px 10px; border-radius: 5px; 
                    background-color: {color}; color: white; font-weight: bold;
                    margin-right: 10px;'>
            {label}: {rating}
        </div>
    """, unsafe_allow_html=True)

def render_security_review_metrics(df: pd.DataFrame, code_scope: str = "Overall Code"):
    """Render security review specific metrics"""
    if df.empty:
        return
    
    latest = df.iloc[0]
    prefix = "new_code_" if code_scope == "New Code" else ""
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        reviewed_pct = latest.get(f'{prefix}security_hotspots_reviewed', 0)
        st.metric(
            "Security Hotspots Reviewed",
            f"{reviewed_pct:.1f}%",
            delta=None
        )
    
    with col2:
        rating = latest.get(f'{prefix}security_review_rating', 'N/A')
        render_rating_badge(rating, "Security Review")
    
    with col3:
        to_review = latest.get(f'{prefix}security_hotspots_to_review', 0)
        st.metric(
            "Hotspots To Review",
            int(to_review),
            delta=None
        )

def render_technical_debt_metrics(df: pd.DataFrame, code_scope: str = "Overall Code"):
    """Render technical debt specific metrics"""
    if df.empty:
        return
    
    latest = df.iloc[0]
    prefix = "new_code_" if code_scope == "New Code" else ""
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Convert minutes to days
        debt_minutes = latest.get(f'{prefix}technical_debt', 0)
        debt_days = debt_minutes / (8 * 60)  # 8 hours per day
        st.metric(
            "Technical Debt",
            f"{debt_days:.1f} days",
            help="Time required to fix all maintainability issues"
        )
    
    with col2:
        debt_ratio = latest.get(f'{prefix}sqale_debt_ratio', 0)
        st.metric(
            "Debt Ratio",
            f"{debt_ratio:.1f}%",
            help="Ratio of technical debt to development time"
        )
    
    with col3:
        rating = latest.get('sqale_rating', 'N/A')
        render_rating_badge(rating, "Maintainability")

def main():
    st.title("🎯 SonarQube Metrics & Trends Dashboard v2")
    st.markdown("Enhanced with Security Review Metrics and Quality Gate Status")
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔧 Filters")
        
        # Project selection
        projects = fetch_projects()
        project_options = {p['project_name']: p['project_id'] for p in projects}
        
        selected_project_names = st.multiselect(
            "Select Projects",
            options=list(project_options.keys()),
            default=list(project_options.keys())[:3] if len(project_options) >= 3 else list(project_options.keys())
        )
        
        selected_project_ids = [project_options[name] for name in selected_project_names]
        
        # Date range selection
        st.subheader("📅 Date Range")
        date_range_preset = st.selectbox("Quick Select", list(DATE_RANGES.keys()))
        
        if isinstance(DATE_RANGES[date_range_preset], int):
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=DATE_RANGES[date_range_preset])
        else:
            # Handle 'week', 'month', 'quarter', 'year'
            end_date = datetime.now().date()
            if DATE_RANGES[date_range_preset] == 'week':
                start_date = end_date - timedelta(days=end_date.weekday())
            elif DATE_RANGES[date_range_preset] == 'month':
                start_date = end_date.replace(day=1)
            elif DATE_RANGES[date_range_preset] == 'quarter':
                quarter_month = ((end_date.month - 1) // 3) * 3 + 1
                start_date = end_date.replace(month=quarter_month, day=1)
            else:  # year
                start_date = end_date.replace(month=1, day=1)
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=start_date)
        with col2:
            end_date = st.date_input("End Date", value=end_date)
        
        # Code scope selection
        st.subheader("🎯 Code Scope")
        code_scope = st.radio(
            "View metrics for:",
            ["Overall Code", "New Code", "Both"],
            index=0
        )
        
        # Metric categories selection
        st.subheader("📊 Metric Categories")
        selected_categories = st.multiselect(
            "Select Categories",
            options=list(METRIC_CATEGORIES.keys()),
            default=list(METRIC_CATEGORIES.keys())
        )
        
        # Get all metrics from selected categories
        selected_metrics = []
        for category in selected_categories:
            selected_metrics.extend(METRIC_CATEGORIES[category])
    
    # Main content area
    if selected_project_ids:
        # Fetch data
        df = fetch_metrics_data(selected_project_ids, str(start_date), str(end_date))
        
        if not df.empty:
            # Get latest data for all sections
            latest_date = df['metric_date'].max()
            latest_df = df[df['metric_date'] == latest_date]
            
            # Section 1: Quality Gate Status
            if 'Quality Gate' in selected_categories:
                st.header("🚦 Quality Gate Status")
                
                cols = st.columns(len(selected_project_names))
                for idx, (project_name, col) in enumerate(zip(selected_project_names, cols)):
                    with col:
                        project_df = latest_df[latest_df['project_name'] == project_name]
                        if not project_df.empty:
                            st.subheader(project_name)
                            status = project_df.iloc[0].get('alert_status', 'N/A')
                            render_quality_gate_badge(status)
                            
                            # Show quality gate details if available
                            details = project_df.iloc[0].get('quality_gate_details')
                            if details and not pd.isna(details):
                                try:
                                    details_json = json.loads(details)
                                    with st.expander("View Details"):
                                        st.json(details_json)
                                except:
                                    pass
            
            # Section 2: Security Review Metrics
            if 'Security Review' in selected_metrics:
                st.header("🔒 Security Review Metrics")
                
                if code_scope in ["Overall Code", "Both"]:
                    st.subheader("Overall Code")
                    render_security_review_metrics(latest_df, "Overall Code")
                
                if code_scope in ["New Code", "Both"]:
                    st.subheader("New Code")
                    render_security_review_metrics(latest_df, "New Code")
            
            # Section 3: Technical Debt Analysis
            if 'Technical Debt' in selected_metrics:
                st.header("💰 Technical Debt Analysis")
                
                if code_scope in ["Overall Code", "Both"]:
                    st.subheader("Overall Code")
                    render_technical_debt_metrics(latest_df, "Overall Code")
                
                if code_scope in ["New Code", "Both"]:
                    st.subheader("New Code")
                    render_technical_debt_metrics(latest_df, "New Code")
            
            # Section 4: Ratings Overview
            st.header("⭐ Quality Ratings Overview")
            
            latest_date = df['metric_date'].max()
            latest_df = df[df['metric_date'] == latest_date]
            
            for project_name in selected_project_names:
                project_df = latest_df[latest_df['project_name'] == project_name]
                if not project_df.empty:
                    st.subheader(project_name)
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        rating = project_df.iloc[0].get('reliability_rating', 'N/A')
                        render_rating_badge(rating, "Reliability")
                    
                    with col2:
                        rating = project_df.iloc[0].get('security_rating', 'N/A')
                        render_rating_badge(rating, "Security")
                    
                    with col3:
                        rating = project_df.iloc[0].get('sqale_rating', 'N/A')
                        render_rating_badge(rating, "Maintainability")
                    
                    with col4:
                        rating = project_df.iloc[0].get('security_review_rating', 'N/A')
                        render_rating_badge(rating, "Security Review")
            
            # Section 5: Issue Trends
            st.header("📈 Issue Trends Over Time")
            
            issue_types = ['bugs_total', 'vulnerabilities_total', 'code_smells_total', 'security_hotspots_total']
            issue_labels = ['Bugs', 'Vulnerabilities', 'Code Smells', 'Security Hotspots']
            
            tabs = st.tabs(issue_labels)
            
            for issue_type, issue_label, tab in zip(issue_types, issue_labels, tabs):
                with tab:
                    trend_data = []
                    
                    for _, project_group in df.groupby('sonarqube_project_key'):
                        project_name = project_group.iloc[0]['project_name']
                        dates = pd.to_datetime(project_group['metric_date']).dt.date
                        values = project_group[issue_type]
                        
                        for date, value in zip(dates, values):
                            trend_data.append({
                                'Date': date,
                                'Count': value,
                                'Project': project_name
                            })
                    
                    trend_df = pd.DataFrame(trend_data)
                    
                    if not trend_df.empty:
                        fig = px.line(
                            trend_df,
                            x='Date',
                            y='Count',
                            color='Project',
                            title=f'{issue_label} Trend',
                            markers=True
                        )
                        
                        fig.update_layout(
                            xaxis_title="Date",
                            yaxis_title="Count",
                            hovermode='x unified'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            
            # Section 6: Data Table with Enhanced Metrics
            st.header("📋 Enhanced Data Table")
            
            # Prepare display columns including new metrics
            display_columns = ['project_name', 'metric_date', 'alert_status']
            
            # Add core metrics
            display_columns.extend([
                'bugs_total', 'vulnerabilities_total', 'code_smells_total', 
                'security_hotspots_total', 'security_hotspots_reviewed',
                'coverage_percentage', 'duplicated_lines_density',
                'technical_debt', 'sqale_debt_ratio',
                'reliability_rating', 'security_rating', 'sqale_rating', 'security_review_rating',
                'violations', 'open_issues', 'accepted_issues',
                'is_carried_forward', 'data_source_timestamp'
            ])
            
            # Filter columns that exist in the dataframe
            available_columns = [col for col in display_columns if col in df.columns]
            
            # Display the filtered dataframe
            display_df = df[available_columns].sort_values(['metric_date', 'project_name'], ascending=[False, True])
            
            # Rename columns for better display
            column_rename = {
                'project_name': 'Project',
                'metric_date': 'Date',
                'alert_status': 'Quality Gate',
                'bugs_total': 'Bugs',
                'vulnerabilities_total': 'Vulnerabilities',
                'code_smells_total': 'Code Smells',
                'security_hotspots_total': 'Security Hotspots',
                'security_hotspots_reviewed': 'Hotspots Reviewed %',
                'coverage_percentage': 'Coverage %',
                'duplicated_lines_density': 'Duplication %',
                'technical_debt': 'Tech Debt (min)',
                'sqale_debt_ratio': 'Debt Ratio %',
                'reliability_rating': 'Reliability',
                'security_rating': 'Security',
                'sqale_rating': 'Maintainability',
                'security_review_rating': 'Security Review',
                'violations': 'Total Issues',
                'open_issues': 'Open Issues',
                'accepted_issues': 'Accepted Issues',
                'is_carried_forward': 'Carried Forward',
                'data_source_timestamp': 'Last Updated'
            }
            
            display_df = display_df.rename(columns=column_rename)
            
            # Format technical debt as days
            if 'Tech Debt (min)' in display_df.columns:
                display_df['Tech Debt (days)'] = display_df['Tech Debt (min)'] / (8 * 60)
                display_df['Tech Debt (days)'] = display_df['Tech Debt (days)'].round(1)
                display_df = display_df.drop(columns=['Tech Debt (min)'])
            
            # Show the dataframe
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # CSV download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Enhanced CSV",
                data=csv,
                file_name=f"sonarqube_metrics_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
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
        SonarQube Metrics Dashboard v2 | Enhanced with Security Review & Quality Gate Metrics
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()