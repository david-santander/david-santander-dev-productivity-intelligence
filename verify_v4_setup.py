#!/usr/bin/env python3
"""
Verification script for SonarQube DevSecOps Platform v4
This script checks if all the v4 components are properly configured.
"""

import os
import sys
from pathlib import Path

def check_file_exists(file_path: str, description: str) -> bool:
    """Check if a file exists and print status."""
    if os.path.exists(file_path):
        print(f"✅ {description}: {file_path}")
        return True
    else:
        print(f"❌ {description}: {file_path} - NOT FOUND")
        return False

def verify_v4_setup():
    """Verify that all v4 components are properly set up."""
    print("🔍 Verifying SonarQube DevSecOps Platform v4 Setup")
    print("=" * 60)
    
    base_dir = "/mnt/c/Users/david/OneDrive/Documentos/Jobs/Citibanamex/DevSecOps/dev-productivity-intelligence"
    
    # Check core files
    core_files = [
        (f"{base_dir}/docker-compose.yml", "Docker Compose Configuration"),
        (f"{base_dir}/start_v4_apps.sh", "V4 Startup Script"),
    ]
    
    # Check SonarQube Client v4
    client_files = [
        (f"{base_dir}/sonarqube-etl/airflow/dags/sonarqube_client_v4.py", "SonarQube Client v4"),
    ]
    
    # Check ETL DAG v4
    etl_files = [
        (f"{base_dir}/sonarqube-etl/airflow/dags/sonarqube_etl_dag_v4.py", "ETL DAG v4"),
        (f"{base_dir}/sonarqube-etl/requirements.txt", "ETL Requirements"),
        (f"{base_dir}/sonarqube-etl/Dockerfile", "ETL Dockerfile"),
    ]
    
    # Check Streamlit App v4
    dashboard_files = [
        (f"{base_dir}/sonarqube-dashboard/streamlit/app_v4.py", "Streamlit Dashboard v4"),
        (f"{base_dir}/sonarqube-dashboard/streamlit/requirements.txt", "Dashboard Requirements"),
        (f"{base_dir}/sonarqube-dashboard/Dockerfile", "Dashboard Dockerfile"),
    ]
    
    # Check database scripts
    db_files = [
        (f"{base_dir}/sonarqube-database/postgres/init-scripts/01_create_schema_v3.sql", "Database Schema"),
        (f"{base_dir}/sonarqube-database/postgres/init-scripts/07_update_for_v4.sql", "V4 Database Updates"),
    ]
    
    all_passed = True
    
    print("\n📦 Core Components:")
    for file_path, description in core_files:
        if not check_file_exists(file_path, description):
            all_passed = False
    
    print("\n🔌 SonarQube Client v4:")
    for file_path, description in client_files:
        if not check_file_exists(file_path, description):
            all_passed = False
    
    print("\n🌪️ ETL Pipeline v4:")
    for file_path, description in etl_files:
        if not check_file_exists(file_path, description):
            all_passed = False
    
    print("\n📊 Dashboard v4:")
    for file_path, description in dashboard_files:
        if not check_file_exists(file_path, description):
            all_passed = False
    
    print("\n🗄️ Database Components:")
    for file_path, description in db_files:
        if not check_file_exists(file_path, description):
            all_passed = False
    
    print("\n" + "=" * 60)
    
    if all_passed:
        print("🎉 All v4 components are properly configured!")
        print("\n🚀 Ready to start the platform with:")
        print("   ./start_v4_apps.sh")
        print("\n📋 Or build and run manually with:")
        print("   docker-compose down -v")
        print("   docker-compose build --no-cache")
        print("   docker-compose up -d")
        return True
    else:
        print("❌ Some components are missing. Please check the setup.")
        return False

def check_requirements():
    """Check if required Python packages are available in requirements files."""
    print("\n🐍 Checking Python Requirements:")
    
    # Check ETL requirements
    etl_req_path = "/mnt/c/Users/david/OneDrive/Documentos/Jobs/Citibanamex/DevSecOps/dev-productivity-intelligence/sonarqube-etl/requirements.txt"
    if os.path.exists(etl_req_path):
        with open(etl_req_path, 'r') as f:
            etl_requirements = f.read()
            required_packages = ['requests', 'psycopg2-binary', 'python-dateutil', 'urllib3', 'aiohttp']
            for package in required_packages:
                if package in etl_requirements:
                    print(f"✅ ETL: {package}")
                else:
                    print(f"❌ ETL: {package} - MISSING")
    
    # Check Dashboard requirements
    dash_req_path = "/mnt/c/Users/david/OneDrive/Documentos/Jobs/Citibanamex/DevSecOps/dev-productivity-intelligence/sonarqube-dashboard/streamlit/requirements.txt"
    if os.path.exists(dash_req_path):
        with open(dash_req_path, 'r') as f:
            dash_requirements = f.read()
            required_packages = ['streamlit', 'pandas', 'plotly', 'psycopg2-binary', 'openpyxl']
            for package in required_packages:
                if package in dash_requirements:
                    print(f"✅ Dashboard: {package}")
                else:
                    print(f"❌ Dashboard: {package} - MISSING")

def show_features():
    """Show the new features in v4."""
    print("\n🆕 New Features in v4:")
    print("=" * 40)
    
    print("\n🔌 SonarQube Client v4:")
    print("   • Modular architecture with specialized fetchers")
    print("   • Async/await support for better performance")
    print("   • Type safety with dataclasses and enums")
    print("   • Built-in caching and connection pooling")
    print("   • Comprehensive error handling")
    
    print("\n🌪️ ETL Pipeline v4:")
    print("   • Enterprise-grade monitoring and alerting")
    print("   • Task groups for better organization")
    print("   • Batch processing with parallelization")
    print("   • Data validation framework")
    print("   • Performance metrics collection")
    print("   • Configurable via Airflow Variables")
    
    print("\n📊 Dashboard v4:")
    print("   • Professional UI with responsive design")
    print("   • Advanced visualizations (heatmaps, comparisons)")
    print("   • Database connection pooling")
    print("   • Export to Excel and CSV")
    print("   • Real-time auto-refresh option")
    print("   • Modular component architecture")

if __name__ == "__main__":
    print("🎯 SonarQube DevSecOps Platform v4 - Setup Verification")
    print("=" * 60)
    
    # Verify setup
    setup_ok = verify_v4_setup()
    
    # Check requirements
    check_requirements()
    
    # Show features
    show_features()
    
    print("\n" + "=" * 60)
    if setup_ok:
        print("🎉 Setup verification completed successfully!")
        print("🚀 You can now start the v4 platform!")
    else:
        print("❌ Setup verification failed. Please fix the issues above.")
        sys.exit(1)