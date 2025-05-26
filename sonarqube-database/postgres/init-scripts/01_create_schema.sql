-- Create schema for SonarQube metrics storage
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

-- Create daily metrics table
CREATE TABLE IF NOT EXISTS daily_project_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES sq_projects(project_id),
    metric_date DATE NOT NULL,
    
    -- Bug metrics
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
    
    -- Vulnerability metrics
    vulnerabilities_total INTEGER DEFAULT 0,
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
    
    -- Code smell metrics
    code_smells_total INTEGER DEFAULT 0,
    code_smells_blocker INTEGER DEFAULT 0,
    code_smells_critical INTEGER DEFAULT 0,
    code_smells_major INTEGER DEFAULT 0,
    code_smells_minor INTEGER DEFAULT 0,
    code_smells_info INTEGER DEFAULT 0,
    
    -- Security hotspot metrics
    security_hotspots_total INTEGER DEFAULT 0,
    security_hotspots_high INTEGER DEFAULT 0,
    security_hotspots_medium INTEGER DEFAULT 0,
    security_hotspots_low INTEGER DEFAULT 0,
    
    -- Security hotspot status counts
    security_hotspots_to_review INTEGER DEFAULT 0,
    security_hotspots_reviewed INTEGER DEFAULT 0,
    security_hotspots_acknowledged INTEGER DEFAULT 0,
    security_hotspots_fixed INTEGER DEFAULT 0,
    
    -- Coverage and duplication metrics
    coverage_percentage DECIMAL(5,2),
    duplicated_lines_density DECIMAL(5,2),
    
    -- Quality ratings
    reliability_rating CHAR(1),
    security_rating CHAR(1),
    sqale_rating CHAR(1),
    
    -- New code metrics
    new_code_bugs_total INTEGER DEFAULT 0,
    new_code_bugs_blocker INTEGER DEFAULT 0,
    new_code_bugs_critical INTEGER DEFAULT 0,
    new_code_bugs_major INTEGER DEFAULT 0,
    new_code_bugs_minor INTEGER DEFAULT 0,
    new_code_bugs_info INTEGER DEFAULT 0,
    
    new_code_vulnerabilities_total INTEGER DEFAULT 0,
    new_code_vulnerabilities_critical INTEGER DEFAULT 0,
    new_code_vulnerabilities_high INTEGER DEFAULT 0,
    new_code_vulnerabilities_medium INTEGER DEFAULT 0,
    new_code_vulnerabilities_low INTEGER DEFAULT 0,
    
    new_code_code_smells_total INTEGER DEFAULT 0,
    new_code_code_smells_blocker INTEGER DEFAULT 0,
    new_code_code_smells_critical INTEGER DEFAULT 0,
    new_code_code_smells_major INTEGER DEFAULT 0,
    new_code_code_smells_minor INTEGER DEFAULT 0,
    new_code_code_smells_info INTEGER DEFAULT 0,
    
    new_code_security_hotspots_total INTEGER DEFAULT 0,
    new_code_security_hotspots_high INTEGER DEFAULT 0,
    new_code_security_hotspots_medium INTEGER DEFAULT 0,
    new_code_security_hotspots_low INTEGER DEFAULT 0,
    new_code_security_hotspots_to_review INTEGER DEFAULT 0,
    new_code_security_hotspots_reviewed INTEGER DEFAULT 0,
    
    new_code_coverage_percentage DECIMAL(5,2),
    new_code_duplicated_lines_density DECIMAL(5,2),
    new_code_lines INTEGER DEFAULT 0,
    new_code_period_date DATE,
    
    -- Metadata
    data_source_timestamp TIMESTAMP NOT NULL,
    is_carried_forward BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
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