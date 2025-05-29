-- Script to empty SonarQube metrics data for testing
-- This will remove all metrics data but preserve the schema and projects table structure

-- Connect to the database
\c sonarqube_metrics;

-- Set search path
SET search_path TO sonarqube_metrics;

-- Delete all daily metrics data
TRUNCATE TABLE daily_project_metrics CASCADE;

-- Delete all projects data
TRUNCATE TABLE sq_projects CASCADE;

-- Reset sequences
ALTER SEQUENCE sq_projects_project_id_seq RESTART WITH 1;
ALTER SEQUENCE daily_project_metrics_metric_id_seq RESTART WITH 1;

-- Verify cleanup
SELECT 'Projects count: ' || COUNT(*) FROM sq_projects;
SELECT 'Metrics count: ' || COUNT(*) FROM daily_project_metrics;