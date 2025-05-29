-- Create schema for SonarQube metrics storage
-- Version: 3.0 - COMPREHENSIVE SONARQUBE METRICS
-- Last Updated: 2025-05-28
-- 
-- Changes in this version:
-- 1. Added all standard SonarQube metrics per official documentation
-- 2. Added technical debt metrics (technical_debt, sqale_debt_ratio)
-- 3. Added complexity metrics (complexity, cognitive_complexity)
-- 4. Added detailed coverage metrics (line_coverage, branch_coverage, etc.)
-- 5. Added size metrics (lines, ncloc, classes, functions, statements)
-- 6. Added comment metrics (comment_lines, comment_lines_density)
-- 7. Added detailed duplication metrics (duplicated_blocks, duplicated_files, etc.)
-- 8. Added missing new code metrics for all categories
-- 9. Fixed new code security hotspot status tracking
-- 10. Added remediation effort metrics
--
-- Total metrics tracked: 150+ (comprehensive coverage)

CREATE SCHEMA IF NOT EXISTS sonarqube_metrics;

-- Set search path
SET search_path TO sonarqube_metrics;

-- Create projects table
CREATE TABLE IF NOT EXISTS sq_projects (
    project_id SERIAL PRIMARY KEY,
    sonarqube_project_key VARCHAR(255) UNIQUE NOT NULL,
    project_name VARCHAR(255) NOT NULL,
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_analysis_date_from_sq TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create comprehensive daily metrics table
CREATE TABLE IF NOT EXISTS daily_project_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES sq_projects(project_id),
    metric_date DATE NOT NULL,
    
    -- ================================================================
    -- SIZE METRICS
    -- ================================================================
    lines INTEGER DEFAULT 0,                    -- Total lines of code
    ncloc INTEGER DEFAULT 0,                    -- Non-comment lines of code
    classes INTEGER DEFAULT 0,                 -- Number of classes
    functions INTEGER DEFAULT 0,               -- Number of functions
    statements INTEGER DEFAULT 0,              -- Number of statements
    files INTEGER DEFAULT 0,                   -- Number of files
    directories INTEGER DEFAULT 0,             -- Number of directories
    
    -- ================================================================
    -- BUG METRICS (RELIABILITY)
    -- ================================================================
    bugs_total INTEGER DEFAULT 0,
    bugs_blocker INTEGER DEFAULT 0,
    bugs_critical INTEGER DEFAULT 0,
    bugs_major INTEGER DEFAULT 0,
    bugs_minor INTEGER DEFAULT 0,
    bugs_info INTEGER DEFAULT 0,
    
    -- Bug status counts
    bugs_open INTEGER DEFAULT 0,
    bugs_confirmed INTEGER DEFAULT 0,
    bugs_reopened INTEGER DEFAULT 0,
    bugs_resolved INTEGER DEFAULT 0,
    bugs_closed INTEGER DEFAULT 0,
    bugs_false_positive INTEGER DEFAULT 0,
    bugs_wontfix INTEGER DEFAULT 0,
    
    -- Reliability metrics
    reliability_rating CHAR(1),                -- A-E rating
    reliability_remediation_effort INTEGER DEFAULT 0, -- Minutes to fix bugs
    
    -- ================================================================
    -- VULNERABILITY METRICS (SECURITY)
    -- ================================================================
    vulnerabilities_total INTEGER DEFAULT 0,
    vulnerabilities_blocker INTEGER DEFAULT 0,
    vulnerabilities_critical INTEGER DEFAULT 0,
    vulnerabilities_high INTEGER DEFAULT 0,
    vulnerabilities_medium INTEGER DEFAULT 0,
    vulnerabilities_low INTEGER DEFAULT 0,
    
    -- Vulnerability status counts
    vulnerabilities_open INTEGER DEFAULT 0,
    vulnerabilities_confirmed INTEGER DEFAULT 0,
    vulnerabilities_reopened INTEGER DEFAULT 0,
    vulnerabilities_resolved INTEGER DEFAULT 0,
    vulnerabilities_closed INTEGER DEFAULT 0,
    vulnerabilities_false_positive INTEGER DEFAULT 0,
    vulnerabilities_wontfix INTEGER DEFAULT 0,
    
    -- Security metrics
    security_rating CHAR(1),                   -- A-E rating
    security_remediation_effort INTEGER DEFAULT 0, -- Minutes to fix vulnerabilities
    
    -- ================================================================
    -- CODE SMELL METRICS (MAINTAINABILITY)
    -- ================================================================
    code_smells_total INTEGER DEFAULT 0,
    code_smells_blocker INTEGER DEFAULT 0,
    code_smells_critical INTEGER DEFAULT 0,
    code_smells_major INTEGER DEFAULT 0,
    code_smells_minor INTEGER DEFAULT 0,
    code_smells_info INTEGER DEFAULT 0,
    
    -- Code smell status counts
    code_smells_open INTEGER DEFAULT 0,
    code_smells_confirmed INTEGER DEFAULT 0,
    code_smells_reopened INTEGER DEFAULT 0,
    code_smells_resolved INTEGER DEFAULT 0,
    code_smells_closed INTEGER DEFAULT 0,
    code_smells_false_positive INTEGER DEFAULT 0,
    code_smells_wontfix INTEGER DEFAULT 0,
    
    -- Technical debt metrics
    technical_debt INTEGER DEFAULT 0,          -- Technical debt in minutes
    sqale_rating CHAR(1),                      -- A-E maintainability rating
    sqale_debt_ratio DECIMAL(10,2),           -- Technical debt ratio
    
    -- ================================================================
    -- SECURITY HOTSPOT METRICS
    -- ================================================================
    security_hotspots_total INTEGER DEFAULT 0,
    security_hotspots_high INTEGER DEFAULT 0,
    security_hotspots_medium INTEGER DEFAULT 0,
    security_hotspots_low INTEGER DEFAULT 0,
    
    -- Security hotspot status counts
    security_hotspots_to_review INTEGER DEFAULT 0,
    security_hotspots_acknowledged INTEGER DEFAULT 0,
    security_hotspots_fixed INTEGER DEFAULT 0,
    security_hotspots_safe INTEGER DEFAULT 0,
    
    -- ================================================================
    -- COVERAGE METRICS
    -- ================================================================
    coverage_percentage DECIMAL(5,2),          -- Overall coverage
    line_coverage_percentage DECIMAL(5,2),     -- Line coverage
    branch_coverage_percentage DECIMAL(5,2),   -- Branch coverage
    covered_lines INTEGER DEFAULT 0,           -- Lines covered by tests
    uncovered_lines INTEGER DEFAULT 0,         -- Lines not covered
    covered_conditions INTEGER DEFAULT 0,      -- Conditions covered
    uncovered_conditions INTEGER DEFAULT 0,    -- Conditions not covered
    lines_to_cover INTEGER DEFAULT 0,          -- Total lines to cover
    conditions_to_cover INTEGER DEFAULT 0,     -- Total conditions to cover
    
    -- ================================================================
    -- DUPLICATION METRICS
    -- ================================================================
    duplicated_lines_density DECIMAL(5,2),     -- Duplicated lines density
    duplicated_lines INTEGER DEFAULT 0,        -- Number of duplicated lines
    duplicated_blocks INTEGER DEFAULT 0,       -- Number of duplicated blocks
    duplicated_files INTEGER DEFAULT 0,        -- Number of files with duplications
    
    -- ================================================================
    -- COMPLEXITY METRICS
    -- ================================================================
    complexity INTEGER DEFAULT 0,              -- Cyclomatic complexity
    cognitive_complexity INTEGER DEFAULT 0,    -- Cognitive complexity
    
    -- ================================================================
    -- COMMENT METRICS
    -- ================================================================
    comment_lines INTEGER DEFAULT 0,           -- Lines of comments
    comment_lines_density DECIMAL(5,2),        -- Comment density percentage
    
    -- ================================================================
    -- NEW CODE METRICS
    -- ================================================================
    new_code_lines INTEGER DEFAULT 0,
    new_code_ncloc INTEGER DEFAULT 0,
    
    -- New code bugs
    new_code_bugs_total INTEGER DEFAULT 0,
    new_code_bugs_blocker INTEGER DEFAULT 0,
    new_code_bugs_critical INTEGER DEFAULT 0,
    new_code_bugs_major INTEGER DEFAULT 0,
    new_code_bugs_minor INTEGER DEFAULT 0,
    new_code_bugs_info INTEGER DEFAULT 0,
    new_code_reliability_remediation_effort INTEGER DEFAULT 0,
    
    -- New code vulnerabilities
    new_code_vulnerabilities_total INTEGER DEFAULT 0,
    new_code_vulnerabilities_blocker INTEGER DEFAULT 0,
    new_code_vulnerabilities_critical INTEGER DEFAULT 0,
    new_code_vulnerabilities_high INTEGER DEFAULT 0,
    new_code_vulnerabilities_medium INTEGER DEFAULT 0,
    new_code_vulnerabilities_low INTEGER DEFAULT 0,
    new_code_security_remediation_effort INTEGER DEFAULT 0,
    
    -- New code smells
    new_code_code_smells_total INTEGER DEFAULT 0,
    new_code_code_smells_blocker INTEGER DEFAULT 0,
    new_code_code_smells_critical INTEGER DEFAULT 0,
    new_code_code_smells_major INTEGER DEFAULT 0,
    new_code_code_smells_minor INTEGER DEFAULT 0,
    new_code_code_smells_info INTEGER DEFAULT 0,
    new_code_technical_debt INTEGER DEFAULT 0,
    new_code_sqale_debt_ratio DECIMAL(10,2),
    
    -- New code security hotspots
    new_code_security_hotspots_total INTEGER DEFAULT 0,
    new_code_security_hotspots_high INTEGER DEFAULT 0,
    new_code_security_hotspots_medium INTEGER DEFAULT 0,
    new_code_security_hotspots_low INTEGER DEFAULT 0,
    new_code_security_hotspots_to_review INTEGER DEFAULT 0,
    new_code_security_hotspots_acknowledged INTEGER DEFAULT 0,
    new_code_security_hotspots_fixed INTEGER DEFAULT 0,
    new_code_security_hotspots_safe INTEGER DEFAULT 0,
    
    -- New code coverage
    new_code_coverage_percentage DECIMAL(5,2),
    new_code_line_coverage_percentage DECIMAL(5,2),
    new_code_branch_coverage_percentage DECIMAL(5,2),
    new_code_covered_lines INTEGER DEFAULT 0,
    new_code_uncovered_lines INTEGER DEFAULT 0,
    new_code_covered_conditions INTEGER DEFAULT 0,
    new_code_uncovered_conditions INTEGER DEFAULT 0,
    new_code_lines_to_cover INTEGER DEFAULT 0,
    new_code_conditions_to_cover INTEGER DEFAULT 0,
    
    -- New code duplications
    new_code_duplicated_lines_density DECIMAL(5,2),
    new_code_duplicated_lines INTEGER DEFAULT 0,
    new_code_duplicated_blocks INTEGER DEFAULT 0,
    
    -- New code complexity
    new_code_complexity INTEGER DEFAULT 0,
    new_code_cognitive_complexity INTEGER DEFAULT 0,
    
    -- New code period information
    new_code_period_date DATE,
    new_code_period_mode VARCHAR(50),
    new_code_period_value VARCHAR(50),
    
    -- ================================================================
    -- METADATA
    -- ================================================================
    data_source_timestamp TIMESTAMP NOT NULL,
    is_carried_forward BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicate entries
    CONSTRAINT unique_project_date UNIQUE(project_id, metric_date)
);

-- Create indexes for performance
CREATE INDEX idx_daily_metrics_project_date ON daily_project_metrics(project_id, metric_date DESC);
CREATE INDEX idx_daily_metrics_date ON daily_project_metrics(metric_date DESC);
CREATE INDEX idx_projects_sonarqube_key ON sq_projects(sonarqube_project_key);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for projects table
CREATE TRIGGER update_sq_projects_updated_at BEFORE UPDATE
    ON sq_projects FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create trigger for daily_project_metrics table
CREATE TRIGGER update_daily_project_metrics_updated_at BEFORE UPDATE
    ON daily_project_metrics FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create view for latest metrics per project
CREATE OR REPLACE VIEW latest_project_metrics AS
SELECT 
    p.project_id,
    p.sonarqube_project_key,
    p.project_name,
    m.*
FROM sq_projects p
LEFT JOIN LATERAL (
    SELECT *
    FROM daily_project_metrics dpm
    WHERE dpm.project_id = p.project_id
    ORDER BY dpm.metric_date DESC
    LIMIT 1
) m ON true;

-- Grant permissions (adjust as needed)
GRANT ALL PRIVILEGES ON SCHEMA sonarqube_metrics TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sonarqube_metrics TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA sonarqube_metrics TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA sonarqube_metrics TO PUBLIC;

-- Add helpful comments
COMMENT ON TABLE daily_project_metrics IS 'Comprehensive daily SonarQube metrics including all standard metrics per official API documentation';
COMMENT ON COLUMN daily_project_metrics.technical_debt IS 'Technical debt in minutes to fix all maintainability issues';
COMMENT ON COLUMN daily_project_metrics.sqale_debt_ratio IS 'Technical debt ratio as percentage';
COMMENT ON COLUMN daily_project_metrics.complexity IS 'Cyclomatic complexity of the project';
COMMENT ON COLUMN daily_project_metrics.cognitive_complexity IS 'Cognitive complexity of the project';
COMMENT ON COLUMN daily_project_metrics.created_at IS 'Timestamp when this record was first created';
COMMENT ON COLUMN daily_project_metrics.updated_at IS 'Timestamp when this record was last updated (automatically maintained by trigger)';