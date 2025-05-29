#!/usr/bin/env python3
"""
Status checker for SonarQube DevSecOps Platform v4
This script checks the status of all running services and provides access information.
"""

import subprocess
import time
import sys
import requests
from datetime import datetime

def run_command(command):
    """Run a shell command and return the output."""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return False, "", str(e)

def check_service_health(url, service_name, timeout=5):
    """Check if a service is responding."""
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code < 400:
            return True, f"✅ {service_name}: Healthy"
        else:
            return False, f"⚠️ {service_name}: Responding with status {response.status_code}"
    except requests.exceptions.ConnectionError:
        return False, f"❌ {service_name}: Connection refused"
    except requests.exceptions.Timeout:
        return False, f"⏳ {service_name}: Timeout"
    except Exception as e:
        return False, f"❌ {service_name}: {str(e)}"

def check_platform_status():
    """Check the status of the entire platform."""
    print("🎯 SonarQube DevSecOps Platform v4 - Status Check")
    print("=" * 60)
    print(f"📅 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check Docker containers
    print("🐳 Docker Container Status:")
    success, output, error = run_command("docker-compose ps --format table")
    if success:
        print(output)
    else:
        print(f"❌ Failed to check Docker status: {error}")
    
    print("\n" + "=" * 60)
    
    # Define services to check
    services = [
        ("http://localhost:9000/api/system/status", "SonarQube"),
        ("http://localhost:8501/", "Streamlit Dashboard v4"),
        ("http://localhost:8080/", "Jenkins"),
        ("http://localhost:3000/", "Gitea"),
        ("http://localhost:5050/", "PgAdmin"),
    ]
    
    print("🔍 Service Health Check:")
    all_healthy = True
    
    for url, service_name in services:
        healthy, message = check_service_health(url, service_name)
        print(f"   {message}")
        if not healthy:
            all_healthy = False
    
    # Check database connectivity
    print("\n🗄️ Database Connectivity:")
    
    # Check metrics database
    success, output, error = run_command(
        "docker-compose exec -T postgres pg_isready -U postgres -d sonarqube_metrics"
    )
    if success:
        print("   ✅ Metrics Database: Ready")
    else:
        print("   ❌ Metrics Database: Not ready")
        all_healthy = False
    
    # Check Airflow database
    success, output, error = run_command(
        "docker-compose exec -T postgres-airflow pg_isready -U airflow -d airflow"
    )
    if success:
        print("   ✅ Airflow Database: Ready")
    else:
        print("   ❌ Airflow Database: Not ready")
        all_healthy = False
    
    print("\n" + "=" * 60)
    
    # Service URLs
    print("🌐 Service URLs:")
    print("   📊 SonarQube:         http://localhost:9000")
    print("   📈 Dashboard v4:      http://localhost:8501")
    print("   🔧 Jenkins:          http://localhost:8080")
    print("   📚 Gitea:            http://localhost:3000")
    print("   🗄️ PgAdmin:          http://localhost:5050 (admin@example.com/admin)")
    
    print("\n📊 Database Connections:")
    print("   📈 Metrics DB:       localhost:5432/sonarqube_metrics (postgres/postgres)")
    print("   🌪️ Airflow DB:       localhost:5433/airflow (airflow/airflow)")
    
    print("\n🆕 v4 Features Available:")
    print("   📊 Enhanced Dashboard with professional UI")
    print("   🗺️ Metrics heatmap visualization")
    print("   📤 Excel and CSV export capabilities")
    print("   🔄 Auto-refresh functionality")
    print("   📈 Advanced trend analysis")
    print("   🎨 Responsive design for all screen sizes")
    
    print("\n" + "=" * 60)
    
    if all_healthy:
        print("🎉 Platform Status: All services are healthy!")
        print("\n📋 Next Steps:")
        print("1. 🔑 Set up SonarQube admin password at: http://localhost:9000")
        print("2. 📋 Create sample projects in SonarQube")
        print("3. 📊 View v4 dashboard at: http://localhost:8501")
        print("4. 🌪️ Build and run Airflow ETL DAG v4 when needed")
        return True
    else:
        print("⚠️ Platform Status: Some services need attention")
        print("\n🔧 Troubleshooting:")
        print("   - Check Docker logs: docker-compose logs [service-name]")
        print("   - Restart service: docker-compose restart [service-name]")
        print("   - Full restart: docker-compose down && docker-compose up -d")
        return False

def show_quick_commands():
    """Show quick commands for managing the platform."""
    print("\n⚡ Quick Commands:")
    print("=" * 30)
    print("📋 View all logs:         docker-compose logs")
    print("📋 View service logs:     docker-compose logs [service]")
    print("🔄 Restart service:       docker-compose restart [service]")
    print("🛑 Stop all services:     docker-compose down")
    print("🚀 Start all services:    docker-compose up -d")
    print("📊 Check status:          python3 status_v4.py")
    print("🧹 Clean restart:         docker-compose down -v && docker-compose up -d")

def check_specific_service():
    """Check a specific service if provided as argument."""
    if len(sys.argv) > 1:
        service_name = sys.argv[1].lower()
        
        service_map = {
            'sonarqube': ("http://localhost:9000/api/system/status", "SonarQube"),
            'dashboard': ("http://localhost:8501/", "Streamlit Dashboard v4"),
            'jenkins': ("http://localhost:8080/", "Jenkins"),
            'gitea': ("http://localhost:3000/", "Gitea"),
            'pgadmin': ("http://localhost:5050/", "PgAdmin"),
        }
        
        if service_name in service_map:
            url, name = service_map[service_name]
            print(f"🔍 Checking {name}...")
            healthy, message = check_service_health(url, name)
            print(message)
            
            if healthy:
                print(f"✅ {name} is healthy and accessible at {url.replace('/api/system/status', '')}")
            else:
                print(f"❌ {name} has issues. Check logs with: docker-compose logs {service_name}")
            
            return healthy
        else:
            print(f"❌ Unknown service: {service_name}")
            print("Available services: sonarqube, dashboard, jenkins, gitea, pgadmin")
            return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Check specific service
        success = check_specific_service()
        sys.exit(0 if success else 1)
    else:
        # Check entire platform
        success = check_platform_status()
        show_quick_commands()
        
        print("\n✨ SonarQube DevSecOps Platform v4 status check complete!")
        sys.exit(0 if success else 1)