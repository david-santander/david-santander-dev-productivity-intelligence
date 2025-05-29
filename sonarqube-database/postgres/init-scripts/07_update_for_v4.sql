-- Schema updates for v4 ETL Pipeline
-- This script adds new tables and columns needed for the enhanced v4 functionality

-- Create ETL task metrics table for performance monitoring
CREATE TABLE IF NOT EXISTS sonarqube_metrics.etl_task_metrics (
    task_metric_id SERIAL PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    duration_seconds NUMERIC(10,3) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('success', 'failed', 'running')),
    error_message TEXT,
    dag_run_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_name, execution_date)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_etl_task_metrics_execution_date ON sonarqube_metrics.etl_task_metrics(execution_date);
CREATE INDEX IF NOT EXISTS idx_etl_task_metrics_status ON sonarqube_metrics.etl_task_metrics(status);
CREATE INDEX IF NOT EXISTS idx_etl_task_metrics_task_name ON sonarqube_metrics.etl_task_metrics(task_name);

-- Add is_active column to projects table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'sonarqube_metrics' 
        AND table_name = 'sq_projects' 
        AND column_name = 'is_active'
    ) THEN
        ALTER TABLE sonarqube_metrics.sq_projects 
        ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
    END IF;
END $$;

-- Create function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION sonarqube_metrics.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add update triggers if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'update_etl_task_metrics_updated_at'
    ) THEN
        CREATE TRIGGER update_etl_task_metrics_updated_at 
            BEFORE UPDATE ON sonarqube_metrics.etl_task_metrics 
            FOR EACH ROW EXECUTE FUNCTION sonarqube_metrics.update_updated_at_column();
    END IF;
END $$;

-- Add comments for documentation
COMMENT ON TABLE sonarqube_metrics.etl_task_metrics IS 'Stores performance metrics for ETL pipeline tasks';
COMMENT ON COLUMN sonarqube_metrics.etl_task_metrics.task_name IS 'Name of the Airflow task';
COMMENT ON COLUMN sonarqube_metrics.etl_task_metrics.execution_date IS 'Execution date of the task';
COMMENT ON COLUMN sonarqube_metrics.etl_task_metrics.duration_seconds IS 'Task execution duration in seconds';
COMMENT ON COLUMN sonarqube_metrics.etl_task_metrics.status IS 'Task execution status';
COMMENT ON COLUMN sonarqube_metrics.etl_task_metrics.error_message IS 'Error message if task failed';

-- Update existing projects to be active
UPDATE sonarqube_metrics.sq_projects SET is_active = TRUE WHERE is_active IS NULL;

-- Create a view for monitoring dashboard
CREATE OR REPLACE VIEW sonarqube_metrics.etl_performance_summary AS
SELECT 
    task_name,
    COUNT(*) as total_executions,
    COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_executions,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_executions,
    ROUND(AVG(duration_seconds), 2) as avg_duration_seconds,
    ROUND(MIN(duration_seconds), 2) as min_duration_seconds,
    ROUND(MAX(duration_seconds), 2) as max_duration_seconds,
    MAX(execution_date) as last_execution,
    ROUND(
        (COUNT(CASE WHEN status = 'success' THEN 1 END)::NUMERIC / COUNT(*)) * 100, 
        2
    ) as success_rate_percent
FROM sonarqube_metrics.etl_task_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY task_name
ORDER BY last_execution DESC;

COMMENT ON VIEW sonarqube_metrics.etl_performance_summary IS 'Summary of ETL task performance over the last 30 days';

-- Grant permissions to airflow user
GRANT ALL PRIVILEGES ON TABLE sonarqube_metrics.etl_task_metrics TO airflow;
GRANT ALL PRIVILEGES ON SEQUENCE sonarqube_metrics.etl_task_metrics_task_metric_id_seq TO airflow;
GRANT SELECT ON sonarqube_metrics.etl_performance_summary TO airflow;

-- Also grant to postgres user for admin access
GRANT ALL PRIVILEGES ON TABLE sonarqube_metrics.etl_task_metrics TO postgres;
GRANT SELECT ON sonarqube_metrics.etl_performance_summary TO postgres;