-- Add new code metrics columns to the daily_project_metrics table
-- New code metrics track issues in code changed during the new code period (typically last 30 days)

-- Add new code metric columns
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS new_code_bugs_total INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_vulnerabilities_total INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_code_smells_total INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_total INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_coverage_percentage DECIMAL(5,2) DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_duplicated_lines_density DECIMAL(5,2) DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_lines INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_period_date DATE;

-- Add severity breakdown for new code vulnerabilities
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS new_code_vulnerabilities_critical INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_vulnerabilities_high INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_vulnerabilities_medium INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_vulnerabilities_low INTEGER DEFAULT 0;

-- Add severity breakdown for new code bugs
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS new_code_bugs_blocker INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_bugs_critical INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_bugs_major INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_bugs_minor INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_bugs_info INTEGER DEFAULT 0;

-- Add severity breakdown for new code smells
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS new_code_code_smells_blocker INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_code_smells_critical INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_code_smells_major INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_code_smells_minor INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_code_smells_info INTEGER DEFAULT 0;

-- Add security hotspots breakdown for new code
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_high INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_medium INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_low INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_to_review INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS new_code_security_hotspots_reviewed INTEGER DEFAULT 0;

-- Add missing security hotspot "safe" status column for overall code
ALTER TABLE sonarqube_metrics.daily_project_metrics
ADD COLUMN IF NOT EXISTS security_hotspots_safe INTEGER DEFAULT 0;

-- Create index for new code period queries
CREATE INDEX IF NOT EXISTS idx_new_code_period_date 
ON sonarqube_metrics.daily_project_metrics(project_id, new_code_period_date);

-- Create a view for comparing overall vs new code metrics
CREATE OR REPLACE VIEW sonarqube_metrics.code_quality_comparison AS
SELECT 
    p.project_name,
    m.metric_date,
    -- Overall code metrics
    m.bugs_total as overall_bugs,
    m.vulnerabilities_total as overall_vulnerabilities,
    m.code_smells_total as overall_code_smells,
    m.security_hotspots_total as overall_security_hotspots,
    m.coverage_percentage as overall_coverage,
    m.duplicated_lines_density as overall_duplication,
    -- New code metrics
    m.new_code_bugs_total as new_code_bugs,
    m.new_code_vulnerabilities_total as new_code_vulnerabilities,
    m.new_code_code_smells_total as new_code_code_smells,
    m.new_code_security_hotspots_total as new_code_security_hotspots,
    m.new_code_coverage_percentage as new_code_coverage,
    m.new_code_duplicated_lines_density as new_code_duplication,
    m.new_code_lines as new_lines_of_code,
    m.new_code_period_date,
    -- Calculated fields
    CASE 
        WHEN m.bugs_total > 0 
        THEN ROUND((m.new_code_bugs_total::DECIMAL / m.bugs_total) * 100, 2)
        ELSE 0 
    END as new_code_bugs_percentage,
    CASE 
        WHEN m.vulnerabilities_total > 0 
        THEN ROUND((m.new_code_vulnerabilities_total::DECIMAL / m.vulnerabilities_total) * 100, 2)
        ELSE 0 
    END as new_code_vulnerabilities_percentage,
    m.is_carried_forward,
    m.data_source_timestamp
FROM sonarqube_metrics.daily_project_metrics m
JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id;

-- Create a view for new code trend analysis
CREATE OR REPLACE VIEW sonarqube_metrics.new_code_trends AS
SELECT 
    p.project_name,
    DATE_TRUNC('week', m.metric_date) as week,
    AVG(m.new_code_bugs_total) as avg_new_bugs,
    AVG(m.new_code_vulnerabilities_total) as avg_new_vulnerabilities,
    AVG(m.new_code_code_smells_total) as avg_new_code_smells,
    AVG(m.new_code_coverage_percentage) as avg_new_code_coverage,
    SUM(m.new_code_lines) as total_new_lines,
    COUNT(DISTINCT m.metric_date) as days_with_data
FROM sonarqube_metrics.daily_project_metrics m
JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
WHERE m.new_code_lines > 0
GROUP BY p.project_name, DATE_TRUNC('week', m.metric_date);

COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_bugs_total IS 'Total bugs in new code during the new code period';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_vulnerabilities_total IS 'Total vulnerabilities in new code during the new code period';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_code_smells_total IS 'Total code smells in new code during the new code period';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_security_hotspots_total IS 'Total security hotspots in new code during the new code period';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_coverage_percentage IS 'Test coverage percentage for new code';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_duplicated_lines_density IS 'Percentage of duplicated lines in new code';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_lines IS 'Number of new lines of code added during the new code period';
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.new_code_period_date IS 'Start date of the new code period for this analysis';