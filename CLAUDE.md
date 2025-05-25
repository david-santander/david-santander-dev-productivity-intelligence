# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DevSecOps CI/CD demo project that integrates:

- **Gitea** (Git repository hosting, port 3000)
- **Jenkins** (CI/CD automation, port 8080)
- **SonarQube** (Code quality analysis, port 9000)

The project demonstrates automated code quality scanning for Python applications using a containerized pipeline. It includes two sample applications:

- Django 1.11 sample application
- FastAPI sample application with REST API endpoints

## Common Commands

### Docker Operations

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs for specific service
docker-compose logs jenkins
docker-compose logs sonarqube
docker-compose logs gitea

# Restart a specific service
docker-compose restart jenkins
```

### Git Operations for Sample Apps

```bash
# Initialize and push Django app to Gitea
cd django-sample-app
git init
git checkout -b main
git add .
git commit -m "commit message"
git remote add origin http://localhost:3000/admin/django-sample-app.git
git push -u origin main

# Initialize and push FastAPI app to Gitea
cd fastapi-sample-app
git init
git checkout -b main
git add .
git commit -m "commit message"
git remote add origin http://localhost:3000/admin/fastapi-sample-app.git
git push -u origin main
```

### FastAPI Development

```bash
cd fastapi-sample-app

# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Run tests with coverage
pytest tests/ --cov=app --cov-report=xml --cov-report=html
```

## Architecture

### Service Integration Flow

1. **Gitea** hosts the sample application repositories
2. **Webhook** triggers Jenkins on push to main branch (token: `mySuperSecretGiteaHookToken123!`)
3. **Jenkins** pulls code and runs SonarQube Scanner
4. **SonarQube** analyzes code quality and reports results

### Key Configuration Files

- `docker-compose.yml`: Orchestrates all three services with shared network
- `*/Jenkinsfile`: Declarative pipelines for each application
- `*/sonar-project.properties`: SonarQube project configurations
- `gitea_config/app.ini`: Gitea server configuration

### Jenkins Pipeline Structure

#### Django Pipeline

The Jenkinsfile defines two main stages:

1. **Checkout**: Pulls code from Gitea repository
2. **SonarQube Analysis**: Runs scanner with project properties

#### FastAPI Pipeline

The Jenkinsfile includes additional stages:

1. **Checkout**: Pulls code from Gitea repository
2. **Setup Python Environment**: Creates virtual environment and installs dependencies
3. **Run Tests**: Executes pytest with coverage reporting
4. **SonarQube Analysis**: Runs scanner with coverage data

### Authentication Flow

- Gitea Personal Access Token → Jenkins credential `gitea-token`
- SonarQube Authentication Token → Jenkins credential `sonarqube-token`
- Jenkins configured with SonarQube server `SonarQubeServer` at `http://sonarqube:9000`

## Sample Applications

### Django Sample App

The sample application uses Django 1.11 with minimal dependencies. The SonarQube scanner is configured to analyze Python code with the project key `dev.productivity:django-sample-app`.

### FastAPI Sample App

A modern REST API application built with FastAPI that includes:

- User management endpoints (`/api/v1/users`)
- Item management endpoints (`/api/v1/items`)
- Health check endpoints
- Comprehensive test suite with pytest
- SonarQube configuration with coverage reporting (project key: `dev.productivity:fastapi-sample-app`)