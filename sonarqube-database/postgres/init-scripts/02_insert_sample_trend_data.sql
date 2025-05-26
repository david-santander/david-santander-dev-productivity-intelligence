-- This script inserts sample trend data to demonstrate how the dashboard would look with actual metric changes
-- This is for demonstration purposes only

SET search_path TO sonarqube_metrics;

-- Update some historical data to show trends for Django app
-- Simulate gradual improvement in code quality over time

-- February: Started with 5 code smells
UPDATE daily_project_metrics 
SET code_smells_total = 5
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:django-sample-app')
AND metric_date BETWEEN '2025-02-24' AND '2025-02-28';

-- March: Increased to 8 code smells
UPDATE daily_project_metrics 
SET code_smells_total = 8
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:django-sample-app')
AND metric_date BETWEEN '2025-03-01' AND '2025-03-15';

-- Mid-March: Further increased to 10
UPDATE daily_project_metrics 
SET code_smells_total = 10
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:django-sample-app')
AND metric_date BETWEEN '2025-03-16' AND '2025-03-31';

-- April: Peaked at 15
UPDATE daily_project_metrics 
SET code_smells_total = 15
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:django-sample-app')
AND metric_date BETWEEN '2025-04-01' AND '2025-04-15';

-- Mid-April: Started improving, reduced to 12
UPDATE daily_project_metrics 
SET code_smells_total = 12
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:django-sample-app')
AND metric_date BETWEEN '2025-04-16' AND '2025-04-30';

-- May: Continued improvement to 11 (current state)
-- Already at 11, no update needed

-- Also update security hotspots to show variation
UPDATE daily_project_metrics 
SET security_hotspots_total = 2
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:django-sample-app')
AND metric_date BETWEEN '2025-02-24' AND '2025-03-15';

UPDATE daily_project_metrics 
SET security_hotspots_total = 4
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:django-sample-app')
AND metric_date BETWEEN '2025-03-16' AND '2025-04-30';

-- Current state is 6 security hotspots (from May onwards)

-- For FastAPI app, show different trend pattern
-- Started with 2 code smells, gradually increased
UPDATE daily_project_metrics 
SET code_smells_total = 2
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:fastapi-sample-app')
AND metric_date BETWEEN '2025-02-24' AND '2025-03-15';

UPDATE daily_project_metrics 
SET code_smells_total = 4
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:fastapi-sample-app')
AND metric_date BETWEEN '2025-03-16' AND '2025-04-15';

UPDATE daily_project_metrics 
SET code_smells_total = 6
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:fastapi-sample-app')
AND metric_date BETWEEN '2025-04-16' AND '2025-05-15';

-- Current state is 7 code smells

-- Add bug trend for FastAPI
UPDATE daily_project_metrics 
SET bugs_total = 0
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:fastapi-sample-app')
AND metric_date < '2025-05-20';

-- Bug was introduced on May 20th
UPDATE daily_project_metrics 
SET bugs_total = 1
WHERE project_id = (SELECT project_id FROM sq_projects WHERE project_name = 'dev.productivity:fastapi-sample-app')
AND metric_date >= '2025-05-20';