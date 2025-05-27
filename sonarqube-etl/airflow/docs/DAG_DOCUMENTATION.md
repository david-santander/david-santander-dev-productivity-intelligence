# SonarQube ETL DAG Documentation

## Overview

This project implements two complementary Airflow DAGs for extracting SonarQube metrics:

1. **sonarqube_etl**: Daily incremental ETL for ongoing metrics collection
2. **sonarqube_etl_backfill**: On-demand historical data backfill

Both DAGs follow Airflow and Python best practices with comprehensive documentation, type hints, and error handling.

## DAG Architecture

### Separation of Concerns

The architecture now includes three main components:

1. **sonarqube_client.py**: Handles all SonarQube API interactions
2. **sonarqube_etl_dag.py**: Daily incremental ETL logic
3. **sonarqube_etl_backfill_dag.py**: Historical data backfill logic

This separation provides:

- **Clear Responsibility**: API client vs. ETL orchestration vs. backfill logic
- **Resource Isolation**: Backfill won't impact daily ETL performance
- **Configuration Flexibility**: Different parameters for each use case
- **Monitoring Clarity**: Easy to track different operation types
- **Code Reusability**: Shared client prevents code duplication

### Component Overview

#### SonarQubeClient (`sonarqube_client.py`)
A unified API client that provides:
- `fetch_all_projects()`: Get all projects with pagination
- `fetch_current_metrics()`: Get latest metrics
- `fetch_historical_metrics()`: Get metrics for specific dates
- `fetch_metrics_smart()`: Automatically choose the right API
- `fetch_issue_breakdown()`: Get detailed issue counts

#### Shared Components
Both DAGs share these components:
- `SonarQubeClient`: For all API interactions
- `get_sonarqube_config()`: Configuration management
- `transform_metric_data()`: Data transformation
- `insert_metric()`: Database operations
- `insert_carried_forward_metric()`: Carry-forward logic

## Daily ETL DAG (`sonarqube_etl`)

### Purpose

Runs automatically every day at 2 AM to collect yesterday's metrics from all SonarQube projects.

### Schedule

```python
schedule_interval='0 2 * * *'  # Daily at 2 AM
```

### Task Flow

```
fetch_projects → extract_metrics → transform_and_load
```

### Key Features

- **Automatic Execution**: No manual intervention needed
- **Carry-Forward Logic**: Handles missing data gracefully
- **Idempotent**: Safe to re-run without duplicating data
- **Transaction Safety**: All-or-nothing database commits

### Configuration

```bash
# Required environment variables
export AIRFLOW_VAR_SONARQUBE_TOKEN="your-token-here"
export AIRFLOW_VAR_SONARQUBE_BASE_URL="http://sonarqube:9000"  # Optional
```

### Manual Trigger

```bash
airflow dags trigger sonarqube_etl
```

## Backfill DAG (`sonarqube_etl_backfill`)

### Purpose

On-demand DAG for populating historical data or filling gaps in metrics.

### Schedule

```python
schedule_interval=None  # Manual trigger only
```

### Task Flow

```
fetch_projects → extract_historical_metrics → load_historical_metrics → validate_backfill
```

### Key Features

- **Batch Processing**: Handles large date ranges efficiently
- **Progress Tracking**: Detailed logging of processing status
- **Validation Report**: Comprehensive data quality analysis
- **Flexible Configuration**: Multiple ways to specify date ranges

### Configuration Options

#### 1. Default (Year-to-Date)

```bash
airflow dags trigger sonarqube_etl_backfill
```

#### 2. Specific Date Range

```bash
airflow dags trigger sonarqube_etl_backfill --conf '{
    "start_date": "2025-01-01",
    "end_date": "2025-03-31"
}'
```

#### 3. Last N Days

```bash
airflow dags trigger sonarqube_etl_backfill --conf '{
    "backfill_days": 90
}'
```

#### 4. With Batch Size

```bash
airflow dags trigger sonarqube_etl_backfill --conf '{
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "batch_size": 30
}'
```

### Validation Report

The backfill DAG generates a comprehensive validation report including:

- Date range coverage
- Per-project statistics
- Data quality metrics
- Gap detection

## Data Model

### Metrics Collected

#### Overall Code Metrics

- **Bugs**: Total count and breakdown by severity (blocker, critical, major, minor, info)
- **Vulnerabilities**: Total count and severity breakdown
- **Code Smells**: Total count and severity breakdown
- **Security Hotspots**: Total count and status breakdown
- **Coverage**: Code coverage percentage
- **Duplication**: Duplicated lines density
- **Quality Ratings**: Reliability, security, and maintainability ratings

#### New Code Metrics

All the above metrics specifically for code added in the new code period, plus:

- Lines of new code
- New code period start date

### Database Schema

The PostgreSQL schema includes:

- `sq_projects`: Project registry
- `daily_project_metrics`: Time-series metrics data
- Support for carry-forward tracking
- Comprehensive indexes for performance

## Best Practices Implementation

### 1. Documentation

- **Module-level docstrings**: Comprehensive overview with examples
- **Function docstrings**: Google-style with full parameter descriptions
- **Type hints**: Complete type annotations for all functions
- **Inline comments**: Where logic requires clarification

### 2. Error Handling

- **Graceful degradation**: Continues processing on individual failures
- **Transaction safety**: Rollback on database errors
- **Comprehensive logging**: Detailed error messages with context
- **Retry logic**: Exponential backoff for transient failures

### 3. Performance

- **Batch processing**: Efficient handling of large datasets
- **Pagination**: API calls handle large project lists
- **Database optimization**: Bulk inserts and efficient queries
- **Resource management**: Proper connection handling

### 4. Maintainability

- **DRY principle**: Shared client eliminates code duplication
- **Single source of truth**: All API logic in SonarQubeClient
- **Configuration externalization**: Environment-based settings
- **Clear separation**: API logic vs. ETL orchestration vs. database operations
- **Testability**: Client methods are easy to mock and unit test
- **Extensibility**: Easy to add new API methods to the client

## Monitoring and Debugging

### Airflow UI

1. Navigate to <http://localhost:8080>
2. Monitor DAG runs and task status
3. Access detailed logs for each task

### CLI Commands

```bash
# Check DAG status
airflow dags state sonarqube_etl <execution_date>

# View task logs
airflow tasks logs sonarqube_etl extract_metrics <execution_date>

# Test specific task
airflow tasks test sonarqube_etl fetch_projects <execution_date>
```

### Database Queries

```sql
-- Check latest metrics
SELECT * FROM sonarqube_metrics.latest_project_metrics;

-- Analyze carry-forward frequency
SELECT 
    project_name,
    COUNT(*) as total_days,
    SUM(CASE WHEN is_carried_forward THEN 1 ELSE 0 END) as carried_days
FROM sonarqube_metrics.daily_project_metrics m
JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY project_name;
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify SONARQUBE_TOKEN is set correctly
   - Check token permissions in SonarQube

2. **Missing Data**
   - Confirm SonarQube analyses are running
   - Check project visibility settings
   - Review carry-forward patterns

3. **Performance Issues**
   - Adjust batch_size for backfill operations
   - Monitor API response times
   - Check database query performance

4. **Data Quality**
   - Run validation after backfill
   - Monitor carry-forward ratios
   - Verify SonarQube retention policies

## Integration Points

### With CI/CD Pipeline

- Schedule daily ETL after nightly builds
- Trigger backfill after major SonarQube upgrades
- Monitor metrics trends in dashboards

### With SonarQube

- Requires API token with project browse permissions
- Respects SonarQube's data retention policies
- Handles API rate limits gracefully

### With PostgreSQL

- Uses connection pooling
- Implements proper transaction management
- Maintains referential integrity

## Architecture Diagram

```
┌─────────────────────┐     ┌─────────────────────┐
│  sonarqube_etl_dag  │     │ sonarqube_etl_      │
│  (Daily ETL)        │     │ backfill_dag        │
│                     │     │ (Historical)        │
└──────────┬──────────┘     └──────────┬──────────┘
           │                           │
           │  Uses                     │  Uses
           │                           │
           ▼                           ▼
    ┌─────────────────────────────────────────┐
    │          SonarQubeClient                 │
    │                                          │
    │  - fetch_all_projects()                 │
    │  - fetch_current_metrics()              │
    │  - fetch_historical_metrics()           │
    │  - fetch_metrics_smart()                │
    │  - fetch_issue_breakdown()              │
    └──────────────────┬───────────────────────┘
                       │
                       │  API Calls
                       ▼
              ┌────────────────┐
              │   SonarQube    │
              │   Server       │
              └────────────────┘
```

## Future Enhancements

Potential improvements identified:

1. **Caching layer** in SonarQubeClient for frequently accessed data
2. **Async support** for parallel API calls
3. **Parallel project processing** for faster execution
4. **Incremental backfill** to resume interrupted runs
5. **Data archival** for old metrics
6. **Alert mechanisms** for data quality issues
7. **GraphQL API** adoption when available
8. **Rate limiting** handling in the client

## References

- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [SonarQube Web API](https://docs.sonarqube.org/latest/extend/web-api/)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
