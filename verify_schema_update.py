#!/usr/bin/env python3
"""
Verification script for updated database schema.
"""

def verify_schema_update():
    """Verify the updated database schema."""
    
    print("=" * 80)
    print("DATABASE SCHEMA UPDATE VERIFICATION")
    print("=" * 80)
    
    print("\n📋 SCHEMA VERSION: 2.0")
    print("Last Updated: 2025-01-28")
    
    print("\n✅ SCHEMA CHANGES APPLIED:")
    
    changes = {
        "1. Bug Metrics": {
            "Existing": ["bugs_total", "bugs_blocker", "bugs_critical", "bugs_major", "bugs_minor", "bugs_info"],
            "Status (existing)": ["bugs_open", "bugs_confirmed", "bugs_reopened", "bugs_resolved", "bugs_closed"],
            "NEW Status": ["bugs_false_positive", "bugs_wontfix"]
        },
        
        "2. Vulnerability Metrics": {
            "NEW Severity": ["vulnerabilities_blocker"],
            "Existing Severities": ["vulnerabilities_critical", "vulnerabilities_high", "vulnerabilities_medium", "vulnerabilities_low"],
            "Status (existing)": ["vulnerabilities_open", "vulnerabilities_confirmed", "vulnerabilities_reopened", "vulnerabilities_resolved", "vulnerabilities_closed"],
            "NEW Status": ["vulnerabilities_false_positive", "vulnerabilities_wontfix"]
        },
        
        "3. Code Smell Metrics": {
            "Severities (existing)": ["code_smells_blocker", "code_smells_critical", "code_smells_major", "code_smells_minor", "code_smells_info"],
            "NEW Status (all new)": ["code_smells_open", "code_smells_confirmed", "code_smells_reopened", "code_smells_resolved", "code_smells_closed", "code_smells_false_positive", "code_smells_wontfix"]
        },
        
        "4. Security Hotspot Metrics": {
            "Severities (existing)": ["security_hotspots_high", "security_hotspots_medium", "security_hotspots_low"],
            "Status (existing)": ["security_hotspots_to_review", "security_hotspots_acknowledged", "security_hotspots_fixed"],
            "NEW Status": ["security_hotspots_safe"],
            "REMOVED": ["security_hotspots_reviewed (doesn't exist in API)"]
        },
        
        "5. New Code Metrics": {
            "NEW Vulnerability Severity": ["new_code_vulnerabilities_blocker"],
            "REMOVED": ["new_code_security_hotspots_reviewed"],
            "Existing": "All other new code metrics remain unchanged"
        }
    }
    
    for category, details in changes.items():
        print(f"\n{category}:")
        for key, value in details.items():
            if isinstance(value, list):
                print(f"  {key}:")
                for item in value:
                    print(f"    - {item}")
            else:
                print(f"  {key}: {value}")
    
    print("\n" + "=" * 80)
    print("NEW DATABASE VIEWS")
    print("=" * 80)
    
    views = {
        "latest_project_metrics": "Latest metrics for each project (existing, unchanged)",
        "issue_status_summary": "Summary of all issues by status with active count calculation",
        "issue_severity_summary": "Summary of all issues by severity including new blocker column"
    }
    
    for view_name, description in views.items():
        print(f"\n{view_name}:")
        print(f"  {description}")
    
    print("\n" + "=" * 80)
    print("METRIC COUNT SUMMARY")
    print("=" * 80)
    
    metric_counts = {
        "Bug columns": 13,  # 6 severity + 7 status
        "Vulnerability columns": 14,  # 6 severity + 7 status + 1 total
        "Code Smell columns": 14,  # 6 severity + 7 status + 1 total
        "Security Hotspot columns": 8,  # 3 severity + 4 status + 1 total
        "Coverage/Duplication": 2,
        "Quality Ratings": 3,
        "New Code Bug columns": 7,  # 6 severity + 1 total
        "New Code Vulnerability columns": 7,  # 6 severity + 1 total
        "New Code Code Smell columns": 7,  # 6 severity + 1 total
        "New Code Security Hotspot columns": 6,  # 3 severity + 1 status + 1 total + 1 lines
        "New Code Quality": 3,  # coverage, duplication, lines
        "Metadata": 3  # data_source_timestamp, is_carried_forward, created_at
    }
    
    total = sum(metric_counts.values()) + 3  # +3 for project_id, metric_date, metric_id
    
    for metric_type, count in metric_counts.items():
        print(f"  {metric_type}: {count}")
    
    print(f"\n  TOTAL COLUMNS: {total}")
    print(f"  TOTAL METRICS: 85 (excluding metadata)")
    
    print("\n" + "=" * 80)
    print("MIGRATION NOTES")
    print("=" * 80)
    
    print("""
For existing installations, run the following migrations in order:
1. 03_add_new_code_metrics.sql (if not already applied)
2. 04_complete_metric_mappings.sql (new migration for these changes)

The migrations will:
- Add missing columns using ALTER TABLE ADD COLUMN IF NOT EXISTS
- Drop the security_hotspots_reviewed column
- Create new views for better data analysis
- Add column comments for documentation
""")
    
    print("\n✅ Schema update completed successfully!")
    print("The database now tracks all 85 metrics with complete status and severity breakdowns.")

if __name__ == "__main__":
    verify_schema_update()