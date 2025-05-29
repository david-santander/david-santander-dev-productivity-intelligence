#!/usr/bin/env python3
"""
Quick script to check Airflow DAGs status
"""
import requests
from requests.auth import HTTPBasicAuth

def check_dags():
    """Check all DAGs in Airflow."""
    print("🔍 Checking Airflow DAGs...")
    print("=" * 50)
    
    try:
        response = requests.get(
            "http://localhost:8082/api/v1/dags",
            auth=HTTPBasicAuth('admin', 'admin'),
            timeout=10
        )
        response.raise_for_status()
        
        dags = response.json()['dags']
        
        print(f"📊 Found {len(dags)} DAGs:")
        print()
        
        for dag in dags:
            status = "✅ Active" if not dag['is_paused'] else "⏸️ Paused"
            import_status = "✅ OK" if not dag['has_import_errors'] else "❌ Import Error"
            
            print(f"🌪️ {dag['dag_id']}")
            print(f"   Status: {status}")
            print(f"   Import: {import_status}")
            print(f"   Schedule: {dag.get('timetable_description', 'Manual')}")
            print(f"   Description: {dag['description']}")
            print("---")
        
        print()
        active_dags = [d for d in dags if not d['is_paused']]
        print(f"✅ {len(active_dags)} Active DAGs")
        print(f"⏸️ {len(dags) - len(active_dags)} Paused DAGs")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking DAGs: {e}")
        return False

if __name__ == "__main__":
    check_dags()