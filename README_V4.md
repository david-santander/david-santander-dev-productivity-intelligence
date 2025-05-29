# SonarQube DevSecOps Platform v4

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://www.docker.com/)
[![SonarQube](https://img.shields.io/badge/SonarQube-Compatible-green.svg)](https://www.sonarqube.org/)

A comprehensive DevSecOps intelligence platform for code quality analytics and CI/CD pipeline monitoring.

## 🌟 What's New in v4

### 🚀 Major Improvements
- **Enterprise-grade Architecture**: Modular, scalable, and maintainable codebase
- **Professional Dashboard**: Responsive UI with advanced visualizations
- **Type Safety**: Comprehensive use of Python type hints and dataclasses
- **Performance Optimizations**: Connection pooling, caching, and async operations
- **Export Capabilities**: Excel and CSV export functionality
- **Real-time Updates**: Auto-refresh dashboard with live data

### 📊 Enhanced Analytics
- **Advanced Visualizations**: Heatmaps, trend analysis, comparison charts
- **Metrics Correlation**: Cross-project and temporal analysis
- **Quality Ratings**: Comprehensive quality gate tracking
- **Security Insights**: Enhanced security hotspot analysis

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SonarQube     │    │   Airflow ETL   │    │   Dashboard     │
│   (Code Scans)  │───▶│   (Data Proc.)  │───▶│   (Analytics)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         └─────────────▶│   PostgreSQL    │◀─────────────┘
                        │   (Metrics DB)  │
                        └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- 8GB RAM minimum
- 10GB free disk space

### 1. Launch Platform
```bash
# Clone repository
git clone <repository-url>
cd dev-productivity-intelligence

# Switch to v4 branch
git checkout v4-platform

# Start all services
docker-compose up -d
```

### 2. Access Services
- **Dashboard**: http://localhost:8501
- **SonarQube**: http://localhost:9000
- **Airflow**: http://localhost:8082 (admin/admin)
- **Database**: localhost:5432 (postgres/postgres)

### 3. Check Status
```bash
python3 platform_status.py
```

## 📊 Features

### 🎯 Dashboard v4
- **Professional UI**: Modern, responsive design
- **Interactive Charts**: Plotly-powered visualizations
- **Export Options**: Excel, CSV downloads
- **Real-time Data**: Auto-refresh functionality
- **Advanced Filtering**: Multi-dimensional data analysis
- **Heatmaps**: Visual correlation analysis

### 🔌 SonarQube Client v4
- **Modular Architecture**: Specialized metric fetchers
- **Async Operations**: High-performance data extraction
- **Type Safety**: Dataclasses and enums throughout
- **Error Handling**: Comprehensive exception management
- **Caching**: Intelligent API response caching
- **Connection Pooling**: Efficient HTTP management

### 🌪️ ETL Pipeline v4
- **Enterprise Features**: Monitoring, alerting, validation
- **Task Groups**: Organized Airflow workflows
- **Batch Processing**: Configurable parallelism
- **Data Quality**: Built-in validation framework
- **Performance Metrics**: Task execution monitoring
- **Configuration Management**: Airflow Variables integration

## 🛠️ Configuration

### Environment Variables
```env
# Database
POSTGRES_HOST=postgres-metrics
POSTGRES_DB=sonarqube_metrics
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# SonarQube
SONARQUBE_BASE_URL=http://sonarqube:9000
SONARQUBE_TOKEN=your_token_here
```

### Airflow Variables
```json
{
  "sonarqube_etl_config": {
    "environment": "production",
    "batch_size": 10,
    "parallel_workers": 4,
    "enable_notifications": true
  }
}
```

## 📈 Metrics Supported

### Code Quality
- **Reliability**: Bugs, remediation effort
- **Security**: Vulnerabilities, hotspots, review ratings
- **Maintainability**: Code smells, technical debt
- **Coverage**: Line, branch, condition coverage
- **Duplication**: Duplicated lines, blocks, files
- **Complexity**: Cyclomatic, cognitive complexity

### Project Metrics
- **Size**: Lines of code, files, classes, functions
- **Issues**: Open, confirmed, resolved by severity
- **Quality Gates**: Status tracking and history
- **New Code**: All metrics for recent changes

## 🔧 Management

### Platform Status
```bash
# Check all services
python3 platform_status.py

# Check specific service
python3 platform_status.py dashboard
python3 platform_status.py sonarqube
```

### Service Management
```bash
# View logs
docker-compose logs [service-name]

# Restart service
docker-compose restart [service-name]

# Scale services
docker-compose up -d --scale airflow-worker=3
```

### Data Management
```bash
# Backup platform
./backup_platform.sh

# Export metrics
curl -s "http://localhost:8501/api/export/csv"
```

## 🧪 Development

### Local Development
```bash
# Run dashboard locally
cd sonarqube-dashboard/streamlit
streamlit run app.py

# Test ETL components
cd sonarqube-etl
python -m pytest tests/
```

### Adding New Metrics
1. Update `MetricDefinitions` in `sonarqube_client.py`
2. Add database columns in schema migrations
3. Update dashboard visualizations
4. Test with sample data

## 📚 Documentation

- [Platform Status](./platform_status.py) - Health monitoring
- [Backup Strategy](./backup_platform.sh) - Data protection
- [Database Schema](./sonarqube-database/postgres/init-scripts/) - Data structure
- [ETL Documentation](./sonarqube-etl/airflow/docs/) - Pipeline details

## 🤝 Contributing

1. Create feature branch from `v4-platform`
2. Implement changes with tests
3. Update documentation
4. Submit pull request

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 🆘 Support

For issues and questions:
1. Check platform status: `python3 platform_status.py`
2. View service logs: `docker-compose logs [service]`
3. Consult documentation in `/docs`

---

**v4 Platform**: Enterprise-ready DevSecOps intelligence with advanced analytics and professional UI.