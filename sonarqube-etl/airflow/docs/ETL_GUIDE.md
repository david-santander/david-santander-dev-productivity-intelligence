# SonarQube ETL Daily Operations Guide

## Overview

The SonarQube ETL system uses a primary DAG (`sonarqube_etl`) for daily incremental data extraction. This DAG runs automatically every day to capture the latest code quality metrics from SonarQube and store them in PostgreSQL for dashboard visualization.

## Architecture

### Data Flow

1. **SonarQube API** → Extracts metrics and issue details
2. **Airflow Processing** → Transforms and validates data
3. **PostgreSQL Database** → Stores historical metrics
4. **Streamlit Dashboard** → Visualizes trends and insights

### Key Components

- **DAG Name**: `sonarqube_etl`
- **Schedule**: Daily at 2 AM (0 2 * * *)
- **Catchup**: Disabled (use backfill DAG for historical data)
- **Max Active Runs**: 1 (prevents concurrent executions)
- **API Client**: `SonarQubeClient` (handles all SonarQube API interactions)

## How It Works

### Daily Process

The ETL runs through three main tasks:

1. **fetch_projects**: Retrieves all projects from SonarQube
2. **extract_metrics**: Fetches metrics for yesterday's date
3. **transform_and_load**: Processes and stores data in PostgreSQL

### Data Collection

The DAG collects two types of metrics:

#### Overall Code Metrics
- Bugs (total and by severity/status)
- Vulnerabilities (total and by severity/status)
- Code smells (total and by severity/status)
- Security hotspots (total and by severity/status)
- Code coverage percentage
- Duplicated lines density
- Quality ratings (reliability, security, maintainability)

#### New Code Metrics
- Issues introduced in the new code period
- Coverage of new code
- Duplicated lines in new code
- Lines of new code

### Carry-Forward Logic

If SonarQube data is unavailable for a specific day (e.g., no analysis was run), the ETL automatically carries forward the last known values to maintain data continuity.

## Configuration

### Environment Variables

```bash
# Required
export AIRFLOW_VAR_SONARQUBE_TOKEN="your-sonarqube-token"

# Optional (defaults to http://sonarqube:9000)
export AIRFLOW_VAR_SONARQUBE_BASE_URL="http://your-sonarqube-url:9000"
```

### Database Connection

The DAG uses the Airflow connection ID `sonarqube_metrics_db` to connect to PostgreSQL. This is configured automatically by the Docker setup.

## Running the ETL

### Automatic Execution

The DAG runs automatically every day at 2 AM. No manual intervention is required for regular operations.

### Manual Trigger

To run the ETL manually for today's data:

```bash
# Using Airflow CLI
airflow dags trigger sonarqube_etl

# Using Airflow Web UI
# Navigate to DAGs > sonarqube_etl > Trigger DAG
```

### Monitoring Execution

#### Web UI
1. Navigate to http://localhost:8080
2. Find `sonarqube_etl` in the DAGs list
3. Click on the DAG to see execution history
4. Click on a specific run to see task details

#### CLI Commands
```bash
# Check DAG status
airflow dags state sonarqube_etl <execution_date>

# View task logs
airflow tasks logs sonarqube_etl fetch_projects <execution_date>
airflow tasks logs sonarqube_etl extract_metrics <execution_date>
airflow tasks logs sonarqube_etl transform_and_load <execution_date>

# List recent DAG runs
airflow dags list-runs -d sonarqube_etl
```

## Data Validation

### Checking Data Quality

After the ETL runs, validate the data:

```sql
-- Check today's data
SELECT 
    p.project_name,
    m.metric_date,
    m.bugs_total,
    m.vulnerabilities_total,
    m.code_smells_total,
    m.is_carried_forward
FROM sonarqube_metrics.daily_project_metrics m
JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
WHERE m.metric_date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY p.project_name;

-- Check for carried forward data
SELECT 
    p.project_name,
    COUNT(*) as total_days,
    SUM(CASE WHEN m.is_carried_forward THEN 1 ELSE 0 END) as carried_forward_days
FROM sonarqube_metrics.daily_project_metrics m
JOIN sonarqube_metrics.sq_projects p ON m.project_id = p.project_id
WHERE m.metric_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.project_name;
```

### Common Validation Checks

1. **Data Freshness**: Ensure yesterday's data is present
2. **Completeness**: All projects should have metrics
3. **Carry-Forward Ratio**: Monitor how often data is carried forward
4. **Metric Ranges**: Validate that metrics are within expected ranges

## Troubleshooting

### Common Issues

#### 1. Authentication Failures
**Symptom**: 401 or 403 errors in logs

**Solution**:
```bash
# Verify token is set
echo $AIRFLOW_VAR_SONARQUBE_TOKEN

# Test token directly
curl -u your-token: http://localhost:9000/api/projects/search
```

#### 2. Missing Yesterday's Data
**Symptom**: No data for yesterday, unexpected carry-forward

**Possible Causes**:
- No SonarQube analysis was run yesterday
- Project was deleted or renamed
- Analysis failed

**Solution**:
- Check SonarQube project analysis history
- Verify project exists and has recent analyses
- Run manual analysis if needed

#### 3. Database Connection Issues
**Symptom**: Connection refused or timeout errors

**Solution**:
```bash
# Check database is running
docker-compose ps sonarqube-postgres

# Test connection
docker exec -it sonarqube-etl-airflow-webserver-1 airflow connections test sonarqube_metrics_db
```

#### 4. Task Failures
**Symptom**: Red task boxes in Airflow UI

**Debugging Steps**:
1. Click on the failed task in the UI
2. View logs for error details
3. Check for:
   - API rate limits
   - Network connectivity
   - Data format changes
   - Schema mismatches

### Performance Optimization

For environments with many projects:

1. **Monitor Execution Time**:
   ```bash
   airflow tasks list-run -d sonarqube_etl -t extract_metrics
   ```

2. **Check API Response Times**:
   - Review logs for slow API calls
   - Consider implementing pagination limits

3. **Database Optimization**:
   - Ensure indexes are properly maintained
   - Monitor query performance

## Best Practices

### 1. Regular Monitoring
- Check DAG execution daily
- Monitor for increasing carry-forward rates
- Review error logs weekly

### 2. Token Management
- Rotate SonarQube tokens periodically
- Use tokens with minimal required permissions
- Never commit tokens to version control

### 3. Data Retention
- Define retention policies for old metrics
- Archive data older than needed for dashboards
- Monitor database growth

### 4. Coordination with SonarQube
- Schedule ETL after typical analysis windows
- Coordinate with CI/CD pipeline schedules
- Ensure SonarQube analyses run before ETL

## Technical Implementation Details

### SonarQube Client

The ETL uses a shared `SonarQubeClient` class that provides:

- **Smart API Selection**: Automatically chooses between current and historical APIs
- **Error Handling**: Graceful handling of API failures
- **Type Safety**: Full type hints for all methods
- **Logging**: Detailed logging of all API interactions

```python
# Example of how the ETL uses the client
from sonarqube_client import SonarQubeClient

config = get_sonarqube_config()
client = SonarQubeClient(config)

# The client automatically uses the right API
metrics = client.fetch_metrics_smart(project_key, metric_date)
```

## Integration with Backfill

For historical data or gap filling, use the dedicated backfill DAG:

```bash
# Backfill last 7 days
airflow dags trigger sonarqube_etl_backfill --conf '{"backfill_days": 7}'

# Backfill specific month
airflow dags trigger sonarqube_etl_backfill --conf '{
    "start_date": "2025-01-01",
    "end_date": "2025-01-31"
}'
```

See [BACKFILL_GUIDE.md](./BACKFILL_GUIDE.md) for detailed backfill instructions.

## Metrics Reference

### Issue Severities
- **Blocker**: Must be fixed immediately
- **Critical**: Must be fixed quickly
- **Major**: Should be fixed
- **Minor**: Could be fixed
- **Info**: Informational only

### Issue Statuses
- **Open**: New issue
- **Confirmed**: Validated issue
- **Reopened**: Previously closed, now open
- **Resolved**: Fixed but not verified
- **Closed**: Fixed and verified

### Quality Ratings
- **A**: Excellent (0 issues)
- **B**: Good (at least 1 minor issue)
- **C**: Fair (at least 1 major issue)
- **D**: Poor (at least 1 critical issue)
- **E**: Bad (at least 1 blocker issue)

## Support and Maintenance

### Log Locations
- Airflow logs: `/opt/airflow/logs/`
- Task logs: Available in Airflow UI
- Database logs: PostgreSQL container logs

### Health Checks
```bash
# Check all services
docker-compose ps

# Check Airflow scheduler
docker exec -it sonarqube-etl-airflow-scheduler-1 airflow jobs check

# Check database connectivity
docker exec -it sonarqube-etl-airflow-webserver-1 airflow db check

# Test SonarQube client import
docker exec -it sonarqube-etl-airflow-webserver-1 python -c "from sonarqube_client import SonarQubeClient; print('Client OK')"
```

### Getting Help
1. Check Airflow UI for error messages
2. Review task logs for detailed errors
3. Check SonarQubeClient logs for API issues
4. Consult SonarQube API documentation
5. Check database query logs