-- Fix ETL task metrics table schema
-- This script updates the etl_task_metrics table to match the expected column names

-- First, check if the table exists and has the wrong column name
DO $$
BEGIN
    -- Check if execution_time column exists (old name)
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'sonarqube_metrics' 
        AND table_name = 'etl_task_metrics' 
        AND column_name = 'execution_time'
    ) THEN
        -- Rename the column to match what the code expects
        ALTER TABLE sonarqube_metrics.etl_task_metrics 
        RENAME COLUMN execution_time TO execution_time_seconds;
        
        RAISE NOTICE 'Renamed column execution_time to execution_time_seconds';
    END IF;
    
    -- Check if the table exists but is missing the column entirely
    IF EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'sonarqube_metrics' 
        AND table_name = 'etl_task_metrics'
    ) AND NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'sonarqube_metrics' 
        AND table_name = 'etl_task_metrics' 
        AND column_name = 'execution_time_seconds'
    ) THEN
        -- Add the missing column
        ALTER TABLE sonarqube_metrics.etl_task_metrics 
        ADD COLUMN execution_time_seconds NUMERIC(10, 2);
        
        RAISE NOTICE 'Added missing column execution_time_seconds';
    END IF;
END $$;

-- Ensure all required columns exist with correct types
ALTER TABLE sonarqube_metrics.etl_task_metrics 
    ALTER COLUMN execution_time_seconds TYPE NUMERIC(10, 2);

-- Add comment for documentation
COMMENT ON COLUMN sonarqube_metrics.etl_task_metrics.execution_time_seconds 
    IS 'Task execution time in seconds with 2 decimal precision';