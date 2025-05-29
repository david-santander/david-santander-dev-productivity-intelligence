# SonarQube Metrics & Trends Dashboard

This document provides instructions for accessing and using the SonarQube Metrics & Trends Dashboard.

## Overview

The SonarQube Metrics & Trends Dashboard is a Streamlit-based web application that visualizes code quality metrics extracted from SonarQube. Data is collected nightly via Apache Airflow ETL processes and stored in PostgreSQL for historical analysis.

## Architecture Components

1. **Streamlit Dashboard** - Web interface for metric visualization (Port 8501)
2. **Apache Airflow** - ETL orchestration for data extraction (Port 8082)
3. **PostgreSQL** - Database for metrics storage
4. **Redis** - Message broker for Airflow

## Getting Started

### Prerequisites

1. Ensure the main CI/CD stack (Gitea, Jenkins, SonarQube) is running
2. Obtain a SonarQube API token from your SonarQube instance:
   - Login to SonarQube at `http://localhost:9000`
   - Go to My Account → Security → Generate Token
   - Copy the generated token

### Initial Setup

1. **Configure Environment Variables**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and update the `SONARQUBE_TOKEN` with your generated token.

2. **Start the Dashboard Stack**
   ```bash
   docker-compose up -d
   ```

3. **Initialize Airflow Database**
   The `airflow-init` container will automatically initialize the database on first run.

4. **Access the Services**
   - **Streamlit Dashboard**: http://localhost:8501
   - **Airflow UI**: http://localhost:8082 (login: admin/admin)
   - **SonarQube**: http://localhost:9000

## Using the Dashboard

### Dashboard Sections

1. **Key Performance Indicators (KPIs)**
   - Displays current metric values with trend indicators
   - Trends show direction (↑↓→) based on comparison with previous period
   - Metrics include: Bugs, Vulnerabilities, Code Smells, Security Hotspots, Coverage, Duplications

2. **Trends Over Time**
   - Line charts showing historical metric values
   - Default view: Last week with daily data points
   - Interactive charts with hover details

3. **Project Comparison**
   - Horizontal bar charts comparing metrics across projects
   - Color-coded based on metric severity

4. **Data Table**
   - Raw metric data with filtering capabilities
   - CSV export functionality
   - Shows data source timestamp and carry-forward status

### Filters

#### Date Range Filters
- Predefined: Last 7 days, Last 30 days, This Week, This Month, This Quarter, This Year
- Custom date range picker for specific periods

#### Category Filters
- **Security**: Vulnerabilities, Security Hotspots
- **Reliability**: Bugs
- **Maintainability**: Code Smells, Duplications
- **Coverage**: Test coverage percentage

#### Metric-Specific Filters
- **Bugs**: Severity (Blocker, Critical, Major, Minor), Status (Open, Confirmed, Reopened)
- **Vulnerabilities**: Severity (Critical, High, Medium, Low), Status (Open, Confirmed, Reopened)
- **Code Smells**: Severity (Blocker, Critical, Major, Minor)
- **Security Hotspots**: Severity (High, Medium, Low), Status (To Review, Reviewed, Acknowledged, Fixed)

## Airflow ETL Management

### Accessing Airflow

1. Navigate to http://localhost:8082
2. Login with username: `admin`, password: `admin`

### Managing the ETL DAG

1. **Enable the DAG**
   - Find `sonarqube_etl` in the DAGs list
   - Toggle the switch to enable it

2. **Manual Trigger**
   - Click on the DAG name
   - Click "Trigger DAG" button
   - For initial historical load, add config: `{"backfill": true}`

3. **Monitor Execution**
   - View task status in the Graph or Tree view
   - Check logs for any errors

### ETL Schedule

- **Default Schedule**: Runs nightly at 2:00 AM
- **Initial Run**: Loads last 3 months of historical data
- **Subsequent Runs**: Loads previous day's data only

## Database Schema

### Tables

1. **sq_projects**
   - Stores SonarQube project information
   - Fields: project_id, sonarqube_project_key, project_name, timestamps

2. **daily_project_metrics**
   - Stores daily metric snapshots
   - Includes all metric values, severities, and statuses
   - Tracks whether data was carried forward

### Data Retention

- Metrics are retained for 2 years
- Older data may be archived or purged based on storage requirements

## Troubleshooting

### Common Issues

1. **Dashboard shows "No data available"**
   - Ensure Airflow DAG has run successfully
   - Check if SonarQube has analysis data
   - Verify database connectivity

2. **Airflow DAG failures**
   - Check Airflow logs for specific errors
   - Verify SonarQube token is valid
   - Ensure SonarQube API is accessible

3. **Slow dashboard performance**
   - Consider reducing date range
   - Check PostgreSQL query performance
   - Monitor container resource usage

### Container Logs

View logs for troubleshooting:
```bash
# Streamlit logs
docker-compose logs streamlit-dashboard

# Airflow logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver

# Database logs
docker-compose logs postgres
```

## Performance Considerations

- Dashboard caches data for 5 minutes to improve performance
- Large date ranges or many projects may impact load times
- Streamlit uses session state to minimize redundant queries

## Security Notes

- Dashboard runs on localhost only by default
- No authentication implemented in this version
- Ensure proper network isolation in production environments
- Store sensitive tokens in environment variables, not in code

## Future Enhancements

Planned improvements include:
- User authentication for dashboard access
- Email/Slack alerts for metric threshold breaches
- Predictive analytics and forecasting
- Custom metric definitions
- Quality gate status tracking