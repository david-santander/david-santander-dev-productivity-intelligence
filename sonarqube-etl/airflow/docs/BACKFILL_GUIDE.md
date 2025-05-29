# SonarQube ETL Backfill Guide

## Overview

The SonarQube ETL system uses a dedicated backfill DAG (`sonarqube_etl_backfill`) for populating historical data. This separation follows Airflow best practices by keeping daily operations and backfill logic independent.

## Why Separate DAGs?

1. **Clear Separation of Concerns**: Daily ETL focuses on incremental updates, backfill handles historical data
2. **Different Scheduling Needs**: Daily runs on schedule, backfill is manual/on-demand
3. **Resource Management**: Backfill can be resource-intensive and should not interfere with daily operations
4. **Configuration Simplicity**: Each DAG has its own focused configuration options
5. **Monitoring & Debugging**: Easier to track and troubleshoot issues when logic is separated

## How It Works

1. **Historical Data Source**: Uses SonarQube's `/api/measures/search_history` endpoint to fetch historical metric values
2. **Date Range Support**: Configurable date ranges for targeted backfilling
3. **Automatic Carry-Forward**: If data is missing for specific dates, the DAG will carry forward the last known values

## Using the Backfill DAG

### DAG Name: `sonarqube_etl_backfill`

This dedicated DAG handles all historical data backfilling with optimized batch processing and validation.

### Backfill Options

#### 1. Default Backfill (Year-to-Date)

Backfills from January 1, 2025 to the current date:

```bash
# Using Airflow CLI
airflow dags trigger sonarqube_etl_backfill

# Using Airflow Web UI
# Navigate to DAGs > sonarqube_etl_backfill > Trigger DAG
# Configuration JSON: {} (empty config uses defaults)
```

#### 2. Custom Date Range

Specify exact start and end dates:

```bash
# Backfill Q1 2025
airflow dags trigger sonarqube_etl_backfill --conf '{
    "start_date": "2025-01-01",
    "end_date": "2025-03-31"
}'

# Backfill specific month
airflow dags trigger sonarqube_etl_backfill --conf '{
    "start_date": "2025-02-01",
    "end_date": "2025-02-28"
}'
```

#### 3. Last N Days

Backfill a specific number of days from today:

```bash
# Backfill last 30 days
airflow dags trigger sonarqube_etl_backfill --conf '{"backfill_days": 30}'

# Backfill last week
airflow dags trigger sonarqube_etl_backfill --conf '{"backfill_days": 7}'
```

#### 4. Batch Processing

For large date ranges, specify batch size to process data in chunks:

```bash
# Backfill with weekly batches (7 days per batch)
airflow dags trigger sonarqube_etl_backfill --conf '{
    "start_date": "2025-01-01",
    "end_date": "2025-05-31",
    "batch_size": 7
}'

# Backfill with monthly batches (30 days per batch)
airflow dags trigger sonarqube_etl_backfill --conf '{
    "backfill_days": 365,
    "batch_size": 30
}'
```

## Important Considerations

### Performance

- **API Rate Limits**: SonarQube may have rate limits. The DAG includes retry logic with exponential backoff
- **Processing Time**: Backfilling large date ranges may take considerable time
- **Database Load**: Large backfills will insert many records - ensure your database can handle the load

### Data Availability

- **Historical Retention**: SonarQube retains historical data based on your instance configuration
- **Missing Data**: If SonarQube doesn't have data for a specific date, the DAG will carry forward the last known values
- **Analysis Frequency**: Historical data depends on how often your projects were analyzed
- **Data Validation**: After backfill completes, the DAG runs a validation task that generates a comprehensive report including:
  - Days with data per project
  - Actual vs carried-forward data ratio
  - Average metrics per project
  - Detection of any data gaps

### Best Practices

1. **Start Small**: Test with a short date range first

   ```bash
   airflow dags trigger sonarqube_etl_backfill --conf '{"backfill_days": 7}'
   ```

2. **Monitor Progress**: Check Airflow logs for progress updates

   ```bash
   airflow tasks logs sonarqube_etl_backfill extract_historical_metrics <execution_date>
   ```

3. **Verify Data**: After backfill, verify data in the dashboard or database

   ```sql
   SELECT 
       p.project_name,
       COUNT(DISTINCT m.metric_date) as days_with_data,
       MIN(m.metric_date) as earliest_date,
       MAX(m.metric_date) as latest_date
   FROM sonarqube_metrics.daily_project_metrics m
   JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
   GROUP BY p.project_name;
   ```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Ensure SONARQUBE_TOKEN environment variable is set
   - Verify token has necessary permissions

2. **No Historical Data**
   - Check if your SonarQube instance has historical data retention enabled
   - Verify projects have been analyzed during the backfill period

3. **Slow Performance**
   - Consider breaking large backfills into smaller chunks
   - Run during off-peak hours

### Checking Logs

```bash
# View DAG run logs
airflow dags show-logs sonarqube_etl_backfill

# Check specific task logs
airflow tasks logs sonarqube_etl_backfill extract_historical_metrics <execution_date>
airflow tasks logs sonarqube_etl_backfill load_historical_metrics <execution_date>
airflow tasks logs sonarqube_etl_backfill validate_backfill <execution_date>
```

## Example: January 2025 Backfill

To backfill all data from January 2025 to today:

```bash
# Option 1: Use default (recommended)
airflow dags trigger sonarqube_etl_backfill

# Option 2: Explicit date range
airflow dags trigger sonarqube_etl_backfill --conf '{
    "start_date": "2025-01-01",
    "end_date": "2025-05-25"
}'
```

This will:

1. Fetch all projects from SonarQube
2. For each project, retrieve historical metrics for each day in the range
3. Transform and load the data into PostgreSQL
4. Mark any missing data as "carried forward" from the last known values
5. Run a comprehensive validation report showing data coverage and quality
