-- Complete metric mappings to ensure full consistency with SonarQube API
-- This migration adds missing columns identified during mapping verification

-- 1. Add BLOCKER severity for vulnerabilities (both overall and new code)
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS vulnerabilities_blocker INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_vulnerabilities_blocker INTEGER DEFAULT 0;

-- 2. Add FALSE-POSITIVE and WONTFIX status columns for bugs
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS bugs_false_positive INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS bugs_wontfix INTEGER DEFAULT 0;

-- 3. Add FALSE-POSITIVE and WONTFIX status columns for vulnerabilities
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS vulnerabilities_false_positive INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS vulnerabilities_wontfix INTEGER DEFAULT 0;

-- 4. Add FALSE-POSITIVE and WONTFIX status columns for code smells
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS code_smells_open INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_confirmed INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_reopened INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_resolved INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_closed INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_false_positive INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS code_smells_wontfix INTEGER DEFAULT 0;

-- 5. Drop the misleading security_hotspots_reviewed column and recreate properly
-- Note: In production, you might want to preserve data, but for this case we'll drop and recreate
ALTER TABLE sonarqube_metrics.daily_project_metrics
DROP COLUMN IF EXISTS security_hotspots_reviewed,
DROP COLUMN IF EXISTS new_code_security_hotspots_reviewed;

-- 6. Update view to include new columns
DROP VIEW IF EXISTS sonarqube_metrics.latest_project_metrics;
CREATE OR REPLACE VIEW sonarqube_metrics.latest_project_metrics AS
SELECT 
    p.project_id,
    p.sonarqube_project_key,
    p.project_name,
    m.*
FROM sonarqube_metrics.sq_projects p
LEFT JOIN LATERAL (
    SELECT *
    FROM sonarqube_metrics.daily_project_metrics dpm
    WHERE dpm.project_id = p.project_id
    ORDER BY dpm.metric_date DESC
    LIMIT 1
) m ON true;

-- 7. Create a comprehensive issue status view
CREATE OR REPLACE VIEW sonarqube_metrics.issue_status_summary AS
SELECT 
    p.project_name,
    m.metric_date,
    -- Bugs by status
    m.bugs_open,
    m.bugs_confirmed,
    m.bugs_reopened,
    m.bugs_resolved,
    m.bugs_closed,
    m.bugs_false_positive,
    m.bugs_wontfix,
    (m.bugs_open + m.bugs_confirmed + m.bugs_reopened) as bugs_active,
    -- Vulnerabilities by status
    m.vulnerabilities_open,
    m.vulnerabilities_confirmed,
    m.vulnerabilities_reopened,
    m.vulnerabilities_resolved,
    m.vulnerabilities_closed,
    m.vulnerabilities_false_positive,
    m.vulnerabilities_wontfix,
    (m.vulnerabilities_open + m.vulnerabilities_confirmed + m.vulnerabilities_reopened) as vulnerabilities_active,
    -- Code Smells by status
    m.code_smells_open,
    m.code_smells_confirmed,
    m.code_smells_reopened,
    m.code_smells_resolved,
    m.code_smells_closed,
    m.code_smells_false_positive,
    m.code_smells_wontfix,
    (m.code_smells_open + m.code_smells_confirmed + m.code_smells_reopened) as code_smells_active,
    -- Security Hotspots by status
    m.security_hotspots_to_review,
    m.security_hotspots_acknowledged,
    m.security_hotspots_fixed,
    m.security_hotspots_safe
FROM sonarqube_metrics.daily_project_metrics m
JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id;

-- 8. Create a severity breakdown view including all severities
CREATE OR REPLACE VIEW sonarqube_metrics.issue_severity_summary AS
SELECT 
    p.project_name,
    m.metric_date,
    -- Bugs by severity
    m.bugs_blocker,
    m.bugs_critical,
    m.bugs_major,
    m.bugs_minor,
    m.bugs_info,
    -- Vulnerabilities by severity (including new blocker column)
    m.vulnerabilities_blocker,
    m.vulnerabilities_critical,
    m.vulnerabilities_high,
    m.vulnerabilities_medium,
    m.vulnerabilities_low,
    -- Code Smells by severity
    m.code_smells_blocker,
    m.code_smells_critical,
    m.code_smells_major,
    m.code_smells_minor,
    m.code_smells_info,
    -- Security Hotspots by severity
    m.security_hotspots_high,
    m.security_hotspots_medium,
    m.security_hotspots_low
FROM sonarqube_metrics.daily_project_metrics m
JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id;

-- Add comments for new columns
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.vulnerabilities_blocker IS 'Number of blocker severity vulnerabilities';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_vulnerabilities_blocker IS 'Number of blocker severity vulnerabilities in new code';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.bugs_false_positive IS 'Number of bugs marked as false positive';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.bugs_wontfix IS 'Number of bugs marked as wont fix';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.vulnerabilities_false_positive IS 'Number of vulnerabilities marked as false positive';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.vulnerabilities_wontfix IS 'Number of vulnerabilities marked as wont fix';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.code_smells_false_positive IS 'Number of code smells marked as false positive';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.code_smells_wontfix IS 'Number of code smells marked as wont fix';