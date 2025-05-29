#!/usr/bin/env python3
"""Test script to debug new code smell metrics."""

import os
import sys
sys.path.append('/mnt/c/Users/david/OneDrive/Documentos/Jobs/Citibanamex/DevSecOps/dev-productivity-intelligence/sonarqube-etl/airflow/dags')

from sonarqube_client_v2 import SonarQubeClient
from datetime import datetime

# Initialize client
client = SonarQubeClient(
    base_url=os.getenv('SONARQUBE_URL', 'http://sonarqube:9000'),
    token=os.getenv('SONARQUBE_TOKEN', 'squ_abe074f4e658c5f8f8b82c91ead19b616ad36bc9')
)

# Fetch metrics for django-sample-app
project_key = 'dev.productivity:django-sample-app'
metric_date = datetime.now().strftime('%Y-%m-%d')

print(f"Fetching metrics for {project_key} on {metric_date}")
result = client.fetch_metrics_smart(project_key, metric_date)

print("\n=== METRICS ===")
print(f"new_code_smells: {result['metrics'].get('new_code_smells', 'N/A')}")

print("\n=== NEW CODE ISSUES BREAKDOWN ===")
new_code_issues = result.get('new_code_issues_breakdown', {})
if new_code_issues:
    print(f"Total keys: {len(new_code_issues)}")
    print("\nAll keys:")
    for key in sorted(new_code_issues.keys()):
        print(f"  {key}: {new_code_issues[key]}")
    
    print("\nCode smell related keys:")
    for key in sorted(new_code_issues.keys()):
        if 'smell' in key:
            print(f"  {key}: {new_code_issues[key]}")
else:
    print("No new code issues breakdown found")

print("\n=== TRANSFORM TEST ===")
# Test the transform function
from sonarqube_etl_dag_v2 import transform_metric_data
transformed = transform_metric_data(result)

print("Transformed new code smell values:")
print(f"  new_code_code_smells_total: {transformed.get('new_code_code_smells_total', 'N/A')}")
print(f"  new_code_code_smells_blocker: {transformed.get('new_code_code_smells_blocker', 'N/A')}")
print(f"  new_code_code_smells_critical: {transformed.get('new_code_code_smells_critical', 'N/A')}")
print(f"  new_code_code_smells_major: {transformed.get('new_code_code_smells_major', 'N/A')}")
print(f"  new_code_code_smells_minor: {transformed.get('new_code_code_smells_minor', 'N/A')}")
print(f"  new_code_code_smells_info: {transformed.get('new_code_code_smells_info', 'N/A')}")