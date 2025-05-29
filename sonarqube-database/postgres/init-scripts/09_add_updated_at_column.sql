-- Add updated_at column to daily_project_metrics table
-- This helps track when metrics were last modified, useful for auditing and debugging

-- Add the updated_at column
ALTER TABLE sonarqube_metrics.daily_project_metrics 
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Create or replace the update trigger function
CREATE OR REPLACE FUNCTION sonarqube_metrics.update_daily_metrics_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- Drop the trigger if it exists and create it
DROP TRIGGER IF EXISTS update_daily_project_metrics_updated_at ON sonarqube_metrics.daily_project_metrics;

CREATE TRIGGER update_daily_project_metrics_updated_at 
BEFORE UPDATE ON sonarqube_metrics.daily_project_metrics
FOR EACH ROW EXECUTE FUNCTION sonarqube_metrics.update_daily_metrics_updated_at();

-- Add comment explaining the column
COMMENT ON COLUMN sonarqube_metrics.daily_project_metrics.updated_at IS 'Timestamp of the last update to this record, automatically maintained by trigger';

-- For existing records, set updated_at to created_at if they're different
UPDATE sonarqube_metrics.daily_project_metrics 
SET updated_at = created_at 
WHERE updated_at > created_at;