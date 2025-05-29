#!/usr/bin/env python3
"""
Collect SonarQube Metrics v4 - Simple ETL Script
===============================================

This script collects metrics from our scanned projects and stores them in the v4 database.
It's a simplified version of the full ETL pipeline for immediate data collection.
"""

import requests
import psycopg2
from datetime import datetime
import json

# Configuration
SONARQUBE_URL = "http://localhost:9000"
SONARQUBE_TOKEN = "squ_5d92333096d8a5ce94e16599471d13e241df9bee"
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'sonarqube_metrics',
    'user': 'postgres',
    'password': 'postgres'
}

# Authentication
AUTH = ('admin', 'admin')

def get_sonarqube_projects():
    """Fetch all projects from SonarQube."""
    url = f"{SONARQUBE_URL}/api/projects/search"
    response = requests.get(url, auth=AUTH)
    response.raise_for_status()
    return response.json()['components']

def get_project_metrics(project_key):
    """Fetch metrics for a specific project."""
    metrics = [
        'lines', 'bugs', 'vulnerabilities', 'code_smells', 'coverage',
        'duplicated_lines_density', 'complexity', 'sqale_index',
        'reliability_rating', 'security_rating', 'sqale_rating',
        'new_bugs', 'new_vulnerabilities', 'new_code_smells'
    ]
    
    url = f"{SONARQUBE_URL}/api/measures/component"
    params = {
        'component': project_key,
        'metricKeys': ','.join(metrics)
    }
    
    response = requests.get(url, auth=AUTH, params=params)
    response.raise_for_status()
    
    measures = response.json().get('component', {}).get('measures', [])
    return {measure['metric']: measure.get('value', 0) for measure in measures}

def store_metrics(project_key, project_name, metrics):
    """Store metrics in the database."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # First, ensure project exists in sq_projects
    cursor.execute("""
        INSERT INTO sonarqube_metrics.sq_projects (sonarqube_project_key, project_name, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sonarqube_project_key) DO UPDATE SET
            project_name = EXCLUDED.project_name,
            updated_at = EXCLUDED.updated_at
        RETURNING project_id
    """, (project_key, project_name, datetime.now(), datetime.now()))
    
    project_id = cursor.fetchone()[0]
    
    # Insert metrics into daily_project_metrics
    insert_query = """
    INSERT INTO sonarqube_metrics.daily_project_metrics (
        project_id, metric_date, lines, bugs_total, vulnerabilities_total,
        code_smells_total, coverage_percentage, duplicated_lines_density, complexity,
        technical_debt, reliability_rating, security_rating,
        sqale_rating, new_code_bugs_total, new_code_vulnerabilities_total, new_code_code_smells_total,
        data_source_timestamp
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (project_id, metric_date) 
    DO UPDATE SET
        lines = EXCLUDED.lines,
        bugs_total = EXCLUDED.bugs_total,
        vulnerabilities_total = EXCLUDED.vulnerabilities_total,
        code_smells_total = EXCLUDED.code_smells_total,
        coverage_percentage = EXCLUDED.coverage_percentage,
        duplicated_lines_density = EXCLUDED.duplicated_lines_density,
        complexity = EXCLUDED.complexity,
        technical_debt = EXCLUDED.technical_debt,
        reliability_rating = EXCLUDED.reliability_rating,
        security_rating = EXCLUDED.security_rating,
        sqale_rating = EXCLUDED.sqale_rating,
        new_code_bugs_total = EXCLUDED.new_code_bugs_total,
        new_code_vulnerabilities_total = EXCLUDED.new_code_vulnerabilities_total,
        new_code_code_smells_total = EXCLUDED.new_code_code_smells_total,
        data_source_timestamp = EXCLUDED.data_source_timestamp
    """
    
    now = datetime.now()
    values = (
        project_id,
        now.date(),
        int(float(metrics.get('lines', 0))),
        int(float(metrics.get('bugs', 0))),
        int(float(metrics.get('vulnerabilities', 0))),
        int(float(metrics.get('code_smells', 0))),
        float(metrics.get('coverage', 0)) if metrics.get('coverage') else None,
        float(metrics.get('duplicated_lines_density', 0)) if metrics.get('duplicated_lines_density') else None,
        int(float(metrics.get('complexity', 0))),
        int(float(metrics.get('sqale_index', 0))) if metrics.get('sqale_index') else 0,  # Technical debt in minutes
        str(int(float(metrics.get('reliability_rating', 1)))),
        str(int(float(metrics.get('security_rating', 1)))),
        str(int(float(metrics.get('sqale_rating', 1)))),
        int(float(metrics.get('new_bugs', 0))),
        int(float(metrics.get('new_vulnerabilities', 0))),
        int(float(metrics.get('new_code_smells', 0))),
        now
    )
    
    cursor.execute(insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

def main():
    """Main ETL process."""
    print("🚀 SonarQube Metrics Collection v4")
    print("=" * 40)
    
    try:
        # Get projects
        print("📋 Fetching projects from SonarQube...")
        projects = get_sonarqube_projects()
        print(f"✅ Found {len(projects)} projects")
        
        # Process each project
        for project in projects:
            project_key = project['key']
            project_name = project['name']
            
            print(f"\n📊 Processing: {project_name}")
            
            try:
                # Get metrics
                metrics = get_project_metrics(project_key)
                print(f"   📈 Collected {len(metrics)} metrics")
                
                # Store in database
                store_metrics(project_key, project_name, metrics)
                print(f"   ✅ Stored metrics successfully")
                
                # Print key metrics
                print(f"   📋 Key Metrics:")
                print(f"      - Lines: {metrics.get('lines', 0)}")
                print(f"      - Bugs: {metrics.get('bugs', 0)}")
                print(f"      - Vulnerabilities: {metrics.get('vulnerabilities', 0)}")
                print(f"      - Code Smells: {metrics.get('code_smells', 0)}")
                print(f"      - Coverage: {metrics.get('coverage', 0)}%")
                
            except Exception as e:
                print(f"   ❌ Error processing {project_name}: {e}")
        
        print(f"\n🎉 ETL Process Complete!")
        print(f"📊 Processed {len(projects)} projects")
        print(f"🌐 Dashboard available at: http://localhost:8501")
        
    except Exception as e:
        print(f"❌ ETL failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())