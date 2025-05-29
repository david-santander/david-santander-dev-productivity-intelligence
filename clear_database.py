#!/usr/bin/env python3
"""
Script to manually clear the SonarQube metrics database for testing purposes.

Usage:
    python clear_database.py                    # Clear only metrics data
    python clear_database.py --clear-projects  # Clear metrics and projects
    python clear_database.py --dry-run         # Show what would be deleted
"""

import psycopg2
import argparse
import sys
import os
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'sonarqube_metrics'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

def get_connection():
    """Create database connection."""
    try:
        return psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def clear_metrics(conn, dry_run=False):
    """Clear daily metrics data."""
    cursor = conn.cursor()
    
    try:
        # Count records before deletion
        cursor.execute("SELECT COUNT(*) FROM sonarqube_metrics.daily_project_metrics")
        count = cursor.fetchone()[0]
        
        print(f"Found {count} metric records")
        
        if count > 0 and not dry_run:
            cursor.execute("DELETE FROM sonarqube_metrics.daily_project_metrics")
            conn.commit()
            print(f"✓ Deleted {count} metric records")
        elif dry_run:
            print(f"[DRY RUN] Would delete {count} metric records")
        else:
            print("No records to delete")
            
    except Exception as e:
        conn.rollback()
        print(f"Error clearing metrics: {e}")
        raise
    finally:
        cursor.close()

def clear_projects(conn, dry_run=False):
    """Clear projects data."""
    cursor = conn.cursor()
    
    try:
        # Count projects before deletion
        cursor.execute("SELECT COUNT(*) FROM sonarqube_metrics.sq_projects")
        count = cursor.fetchone()[0]
        
        print(f"Found {count} projects")
        
        if count > 0 and not dry_run:
            cursor.execute("DELETE FROM sonarqube_metrics.sq_projects")
            conn.commit()
            print(f"✓ Deleted {count} projects")
        elif dry_run:
            print(f"[DRY RUN] Would delete {count} projects")
        else:
            print("No projects to delete")
            
    except Exception as e:
        conn.rollback()
        print(f"Error clearing projects: {e}")
        raise
    finally:
        cursor.close()

def main():
    parser = argparse.ArgumentParser(description='Clear SonarQube metrics database')
    parser.add_argument('--clear-projects', action='store_true', 
                        help='Also clear projects table (default: metrics only)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be deleted without actually deleting')
    
    args = parser.parse_args()
    
    print(f"=== SonarQube Database Cleaner ===")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}")
    print()
    
    if not args.dry_run:
        print("⚠️  WARNING: This will DELETE data from the database!")
        print("Press Enter to continue or Ctrl+C to cancel...")
        try:
            input()
        except KeyboardInterrupt:
            print("\nCancelled")
            sys.exit(0)
    
    conn = get_connection()
    
    try:
        # Clear metrics first (due to foreign key constraint)
        clear_metrics(conn, args.dry_run)
        
        # Clear projects if requested
        if args.clear_projects:
            clear_projects(conn, args.dry_run)
            
        print("\n✓ Database clearing complete")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()