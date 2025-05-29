#!/bin/bash

echo "======================================================"
echo "🚀 Starting SonarQube DevSecOps Platform v4"
echo "======================================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose > /dev/null 2>&1; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "🧹 Cleaning up any existing containers..."
docker-compose down -v --remove-orphans

echo "🔨 Building Docker images..."
docker-compose build --no-cache

echo "🗄️ Starting database services first..."
docker-compose up -d postgres postgres-airflow redis

echo "⏳ Waiting for databases to be ready..."
sleep 30

# Check if databases are ready
echo "🔍 Checking database connectivity..."
docker-compose exec -T postgres pg_isready -U postgres -d sonarqube_metrics
if [ $? -ne 0 ]; then
    echo "❌ Metrics database is not ready. Waiting longer..."
    sleep 30
fi

docker-compose exec -T postgres-airflow pg_isready -U airflow -d airflow
if [ $? -ne 0 ]; then
    echo "❌ Airflow database is not ready. Waiting longer..."
    sleep 30
fi

echo "🚀 Starting SonarQube..."
docker-compose up -d sonarqube

echo "⏳ Waiting for SonarQube to start..."
sleep 60

# Wait for SonarQube to be ready
echo "🔍 Checking SonarQube connectivity..."
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
    if curl -s http://localhost:9000/api/system/status | grep -q "UP"; then
        echo "✅ SonarQube is ready!"
        break
    fi
    echo "⏳ Attempt $attempt/$max_attempts: Waiting for SonarQube..."
    sleep 10
    ((attempt++))
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ SonarQube failed to start within expected time."
    echo "📋 Checking SonarQube logs..."
    docker-compose logs sonarqube
    exit 1
fi

echo "🔧 Initializing Airflow..."
docker-compose up -d airflow-init

echo "⏳ Waiting for Airflow initialization..."
sleep 30

echo "🌪️ Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler

echo "📊 Starting Streamlit dashboard..."
docker-compose up -d streamlit-dashboard

echo "🔍 Starting other services..."
docker-compose up -d gitea jenkins pgadmin

echo "⏳ Waiting for all services to be ready..."
sleep 30

echo ""
echo "======================================================"
echo "🎉 SonarQube DevSecOps Platform v4 is starting up!"
echo "======================================================"
echo ""
echo "📋 Service URLs:"
echo "   🌐 SonarQube:         http://localhost:9000"
echo "   🌪️ Airflow:          http://localhost:8082 (admin/admin)"
echo "   📊 Dashboard:         http://localhost:8501"
echo "   🔧 Jenkins:          http://localhost:8080"
echo "   📚 Gitea:            http://localhost:3000"
echo "   🗄️ PgAdmin:          http://localhost:5050 (admin@example.com/admin)"
echo ""
echo "📊 Database Connections:"
echo "   📈 Metrics DB:       localhost:5432/sonarqube_metrics (postgres/postgres)"
echo "   🌪️ Airflow DB:       localhost:5433/airflow (airflow/airflow)"
echo ""
echo "⚡ Quick Actions:"
echo "   📋 View logs:         docker-compose logs -f [service]"
echo "   🔄 Restart service:   docker-compose restart [service]"
echo "   🛑 Stop all:          docker-compose down"
echo "   📊 View status:       docker-compose ps"
echo ""

# Check service health
echo "🔍 Checking service health..."
echo ""

services=("postgres" "postgres-airflow" "redis" "sonarqube" "airflow-webserver" "airflow-scheduler" "streamlit-dashboard")

for service in "${services[@]}"; do
    if docker-compose ps $service | grep -q "Up"; then
        echo "✅ $service: Running"
    else
        echo "❌ $service: Not running"
    fi
done

echo ""
echo "🔧 Setting up Airflow connections and variables..."

# Wait a bit more for Airflow to be fully ready
sleep 20

# Set up SonarQube connection in Airflow
docker-compose exec -T airflow-webserver airflow connections add 'sonarqube_api' \
    --conn-type 'HTTP' \
    --conn-host 'sonarqube' \
    --conn-port '9000' \
    --conn-password 'squ_34a1a84bd11058208c852fc67f06416919f24d74' \
    --conn-extra '{"timeout": 30, "max_retries": 3}' 2>/dev/null || true

# Set up ETL configuration
docker-compose exec -T airflow-webserver airflow variables set 'sonarqube_etl_config' \
    '{"environment": "development", "batch_size": 5, "parallel_workers": 2, "enable_notifications": false}' 2>/dev/null || true

# Set up alert email list
docker-compose exec -T airflow-webserver airflow variables set 'alert_email_list' \
    '["admin@example.com"]' 2>/dev/null || true

echo ""
echo "🎯 Next Steps:"
echo "1. 🔑 Set up SonarQube admin password at: http://localhost:9000"
echo "2. 📋 Create some sample projects in SonarQube"
echo "3. 🌪️ Enable the ETL DAG in Airflow: http://localhost:8082"
echo "4. 📊 View metrics in the dashboard: http://localhost:8501"
echo ""
echo "📚 Documentation:"
echo "   - SonarQube: https://docs.sonarqube.org/"
echo "   - Airflow: https://airflow.apache.org/docs/"
echo "   - Streamlit: https://docs.streamlit.io/"
echo ""
echo "✨ Enjoy your enhanced DevSecOps platform!"