-- Add missing columns to daily_project_metrics table
-- This script adds columns that are referenced in the ETL DAG but missing from the schema

\c sonarqube_metrics;
SET search_path TO sonarqube_metrics;

-- Add bug-related columns
ALTER TABLE daily_project_metrics 
ADD COLUMN IF NOT EXISTS bugs_false_positive INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS bugs_wontfix INTEGER DEFAULT 0;

-- Add vulnerability-related columns
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS vulnerabilities_blocker INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS vulnerabilities_false_positive INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS vulnerabilities_wontfix INTEGER DEFAULT 0;

-- Add code smell status columns
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS code_smells_open INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_confirmed INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_reopened INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_resolved INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_closed INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_false_positive INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_wontfix INTEGER DEFAULT 0;

-- Add security hotspot columns
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS security_hotspots_safe INTEGER DEFAULT 0;

-- Add new code columns
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS new_code_vulnerabilities_blocker INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_acknowledged INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_fixed INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_safe INTEGER DEFAULT 0;

-- Verify the columns were added
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'sonarqube_metrics' 
AND table_name = 'daily_project_metrics'
AND column_name IN (
    'bugs_false_positive',
    'bugs_wontfix',
    'vulnerabilities_blocker',
    'vulnerabilities_false_positive',
    'vulnerabilities_wontfix',
    'code_smells_open',
    'code_smells_confirmed',
    'code_smells_reopened',
    'code_smells_resolved',
    'code_smells_closed',
    'code_smells_false_positive',
    'code_smells_wontfix',
    'security_hotspots_safe',
    'new_code_vulnerabilities_blocker',
    'new_code_security_hotspots_acknowledged',
    'new_code_security_hotspots_fixed',
    'new_code_security_hotspots_safe'
)
ORDER BY column_name;