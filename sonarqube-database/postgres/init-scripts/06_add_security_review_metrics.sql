-- Add security review metrics and quality gate status to align with SonarQube API
-- Version: 1.0 - Security Review and Quality Gate Metrics
-- Last Updated: 2025-05-28
--
-- This migration adds:
-- 1. Security review metrics (security_hotspots_reviewed, security_review_rating)
-- 2. Quality gate status (alert_status, quality_gate_details)
-- 3. Additional issue metrics (violations, open_issues, accepted_issues)
-- 4. Corrections to existing metric mappings

SET search_path TO sonarqube_metrics;

-- Add security review metrics
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS security_hotspots_reviewed DECIMAL(5,2) DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_reviewed DECIMAL(5,2) DEFAULT 0,
ADD COLUMN IF NOT EXISTS security_review_rating CHAR(1),
ADD COLUMN IF NOT EXISTS new_code_security_review_rating CHAR(1);

-- Add quality gate metrics
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS alert_status VARCHAR(10), -- OK, WARN, ERROR
ADD COLUMN IF NOT EXISTS quality_gate_details TEXT; -- JSON details of conditions

-- Add violations metrics (total issues)
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS violations INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_violations INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS open_issues INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS accepted_issues INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_accepted_issues INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS false_positive_issues INTEGER DEFAULT 0;

-- Add effort to reach maintainability rating A
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS effort_to_reach_maintainability_rating_a INTEGER DEFAULT 0;

-- Add confirmed issues metrics (if not already present)
ALTER TABLE daily_project_metrics
ADD COLUMN IF NOT EXISTS confirmed_issues INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_confirmed_issues INTEGER DEFAULT 0;

-- Update the view to include new columns
DROP VIEW IF EXISTS latest_project_metrics CASCADE;
CREATE OR REPLACE VIEW latest_project_metrics AS
SELECT 
    p.project_id,
    p.sonarqube_project_key,
    p.project_name,
    m.metric_id,
    m.metric_date,
    m.lines,
    m.ncloc,
    m.classes,
    m.functions,
    m.statements,
    m.files,
    m.directories,
    m.bugs_total,
    m.vulnerabilities_total,
    m.code_smells_total,
    m.security_hotspots_total,
    m.security_hotspots_reviewed,
    m.security_review_rating,
    m.reliability_rating,
    m.security_rating,
    m.sqale_rating,
    m.coverage_percentage,
    m.duplicated_lines_density,
    m.technical_debt,
    m.sqale_debt_ratio,
    m.alert_status,
    m.quality_gate_details,
    m.violations,
    m.open_issues,
    m.accepted_issues,
    m.false_positive_issues,
    m.new_code_security_hotspots_reviewed,
    m.new_code_security_review_rating,
    m.data_source_timestamp,
    m.is_carried_forward,
    m.created_at
FROM sq_projects p
LEFT JOIN LATERAL (
    SELECT *
    FROM daily_project_metrics dpm
    WHERE dpm.project_id = p.project_id
    ORDER BY dpm.metric_date DESC
    LIMIT 1
) m ON true;

-- Create a view for security review metrics
CREATE OR REPLACE VIEW security_review_summary AS
SELECT 
    p.project_name,
    m.metric_date,
    m.security_hotspots_total,
    m.security_hotspots_reviewed,
    m.security_review_rating,
    m.security_hotspots_to_review,
    m.security_hotspots_acknowledged,
    m.security_hotspots_fixed,
    m.security_hotspots_safe,
    -- Calculate reviewed percentage
    CASE 
        WHEN m.security_hotspots_total > 0 
        THEN ROUND((m.security_hotspots_acknowledged + m.security_hotspots_fixed + m.security_hotspots_safe)::DECIMAL / m.security_hotspots_total * 100, 2)
        ELSE 100
    END as calculated_reviewed_percentage,
    -- New code metrics
    m.new_code_security_hotspots_total,
    m.new_code_security_hotspots_reviewed,
    m.new_code_security_review_rating,
    m.new_code_security_hotspots_to_review,
    m.new_code_security_hotspots_acknowledged,
    m.new_code_security_hotspots_fixed,
    m.new_code_security_hotspots_safe
FROM daily_project_metrics m
JOIN sq_projects p ON m.project_id = p.project_id
ORDER BY m.metric_date DESC, p.project_name;

-- Create a view for quality gate status
CREATE OR REPLACE VIEW quality_gate_status AS
SELECT 
    p.project_name,
    m.metric_date,
    m.alert_status,
    m.quality_gate_details,
    m.bugs_total,
    m.vulnerabilities_total,
    m.code_smells_total,
    m.security_hotspots_total,
    m.coverage_percentage,
    m.duplicated_lines_density,
    m.reliability_rating,
    m.security_rating,
    m.sqale_rating,
    m.security_review_rating
FROM daily_project_metrics m
JOIN sq_projects p ON m.project_id = p.project_id
WHERE m.alert_status IS NOT NULL
ORDER BY m.metric_date DESC, p.project_name;

-- Add comments for new columns
COMMENT ON COLUMN daily_project_metrics.security_hotspots_reviewed IS 'Percentage of security hotspots reviewed';
COMMENT ON COLUMN daily_project_metrics.new_code_security_hotspots_reviewed IS 'Percentage of new code security hotspots reviewed';
COMMENT ON COLUMN daily_project_metrics.security_review_rating IS 'Security review rating (A-E) based on hotspot review percentage';
COMMENT ON COLUMN daily_project_metrics.new_code_security_review_rating IS 'New code security review rating (A-E)';
COMMENT ON COLUMN daily_project_metrics.alert_status IS 'Quality gate status: OK, WARN, or ERROR';
COMMENT ON COLUMN daily_project_metrics.quality_gate_details IS 'JSON details of quality gate conditions and their status';
COMMENT ON COLUMN daily_project_metrics.violations IS 'Total number of all issues (bugs + vulnerabilities + code smells)';
COMMENT ON COLUMN daily_project_metrics.new_violations IS 'Total number of new issues in new code period';
COMMENT ON COLUMN daily_project_metrics.open_issues IS 'Total number of open issues';
COMMENT ON COLUMN daily_project_metrics.accepted_issues IS 'Total number of accepted issues';
COMMENT ON COLUMN daily_project_metrics.false_positive_issues IS 'Total number of false positive issues';
COMMENT ON COLUMN daily_project_metrics.effort_to_reach_maintainability_rating_a IS 'Effort in minutes to reach maintainability rating A';

-- Grant permissions on new views
GRANT ALL PRIVILEGES ON security_review_summary TO PUBLIC;
GRANT ALL PRIVILEGES ON quality_gate_status TO PUBLIC;