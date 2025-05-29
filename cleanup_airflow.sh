#!/bin/bash

echo "🧹 Starting Airflow cleanup and rebuild..."

# Stop all containers
echo "📦 Stopping all containers..."
docker-compose down

# Remove Airflow containers and images
echo "🗑️ Removing Airflow containers and images..."
docker rm -f airflow-webserver airflow-scheduler airflow-init 2>/dev/null
docker rmi -f dev-productivity-intelligence-airflow-webserver dev-productivity-intelligence-airflow-scheduler dev-productivity-intelligence-airflow-init 2>/dev/null

# Clean up Airflow volumes and logs
echo "📁 Cleaning up Airflow logs and temporary files..."
rm -rf ./sonarqube-etl/airflow/logs/*
rm -rf ./sonarqube-etl/airflow/logs/.* 2>/dev/null
rm -rf ./sonarqube-etl/airflow/plugins/__pycache__
rm -rf ./sonarqube-etl/airflow/dags/__pycache__
rm -rf ./sonarqube-etl/airflow/dags/.airflow

# Remove Airflow database to start fresh
echo "🗄️ Removing Airflow database..."
docker volume rm dev-productivity-intelligence_airflow_db 2>/dev/null
docker rm -f postgres-airflow 2>/dev/null

# Clean Docker build cache
echo "🧹 Cleaning Docker build cache..."
docker builder prune -f

echo "✅ Cleanup complete!"
echo ""
echo "🔨 Rebuilding Airflow..."

# Rebuild and start services
docker-compose build --no-cache airflow-webserver airflow-scheduler
docker-compose up -d

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service status
echo ""
echo "📊 Service status:"
docker-compose ps | grep airflow

echo ""
echo "✅ Airflow rebuild complete!"
echo ""
echo "📌 Access Airflow at: http://localhost:8082"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "💡 Note: It may take a few minutes for all services to be fully ready."