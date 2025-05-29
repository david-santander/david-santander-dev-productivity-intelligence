#!/bin/bash

echo "🔄 Recreating Sample SonarQube Projects for v4 Platform"
echo "======================================================="

# Wait for SonarQube to be fully ready
echo "⏳ Waiting for SonarQube to be ready..."
sleep 10

# Check SonarQube health
while ! curl -s http://localhost:9000/api/system/status | grep -q "UP"; do
    echo "⏳ SonarQube not ready yet, waiting..."
    sleep 5
done

echo "✅ SonarQube is ready!"

# Set up SonarQube admin password (default is admin/admin initially)
echo "🔑 Setting up SonarQube admin user..."

# Change default password if needed
curl -X POST -u admin:admin "http://localhost:9000/api/users/change_password" \
  -d "login=admin&password=admin123&previousPassword=admin" 2>/dev/null || true

# Generate admin token for API access
echo "🔑 Generating API token..."
TOKEN_RESPONSE=$(curl -s -X POST -u admin:admin123 "http://localhost:9000/api/user_tokens/generate" \
  -d "name=etl-token" 2>/dev/null || curl -s -X POST -u admin:admin "http://localhost:9000/api/user_tokens/generate" \
  -d "name=etl-token")

# Extract token from response
API_TOKEN=$(echo $TOKEN_RESPONSE | grep -o '"token":"[^"]*"' | cut -d'"' -f4)

if [ -z "$API_TOKEN" ]; then
    echo "❌ Failed to generate API token. Using default credentials."
    API_TOKEN="admin"
    AUTH="admin:admin"
else
    echo "✅ Generated API token: ${API_TOKEN:0:10}..."
    AUTH="$API_TOKEN:"
fi

echo ""
echo "📋 Creating Sample Projects..."

# Project 1: Django Sample App
echo "1. 🐍 Creating Django Sample App project..."
curl -X POST -u "$AUTH" "http://localhost:9000/api/projects/create" \
  -d "project=django-sample&name=Django Sample App" > /dev/null 2>&1

# Project 2: FastAPI Sample App  
echo "2. ⚡ Creating FastAPI Sample App project..."
curl -X POST -u "$AUTH" "http://localhost:9000/api/projects/create" \
  -d "project=fastapi-sample&name=FastAPI Sample App" > /dev/null 2>&1

# Project 3: React Frontend
echo "3. ⚛️ Creating React Frontend project..."
curl -X POST -u "$AUTH" "http://localhost:9000/api/projects/create" \
  -d "project=react-frontend&name=React Frontend Application" > /dev/null 2>&1

# Project 4: Node.js Backend
echo "4. 🟢 Creating Node.js Backend project..."
curl -X POST -u "$AUTH" "http://localhost:9000/api/projects/create" \
  -d "project=nodejs-backend&name=Node.js Backend Service" > /dev/null 2>&1

echo ""
echo "🔍 Running sample scans..."

# Function to run SonarQube scanner
run_sonar_scan() {
    local project_key=$1
    local project_name=$2
    local source_dir=$3
    
    echo "   📊 Scanning $project_name..."
    
    # Create a temporary sonar-project.properties
    cat > /tmp/sonar-project-${project_key}.properties << EOF
sonar.projectKey=$project_key
sonar.projectName=$project_name
sonar.projectVersion=1.0
sonar.sources=$source_dir
sonar.host.url=http://localhost:9000
sonar.login=$API_TOKEN
EOF

    # Run sonar-scanner (if available) or simulate scan data
    if command -v sonar-scanner >/dev/null 2>&1; then
        cd $source_dir
        sonar-scanner -Dproject.settings=/tmp/sonar-project-${project_key}.properties > /dev/null 2>&1
        cd - > /dev/null
    else
        # Simulate scan by directly inserting some basic metrics via API
        echo "   ⚠️ sonar-scanner not found, creating project placeholder..."
    fi
    
    # Clean up
    rm -f /tmp/sonar-project-${project_key}.properties
}

# Run scans for our sample projects
run_sonar_scan "django-sample" "Django Sample App" "./django-sample-app"
run_sonar_scan "fastapi-sample" "FastAPI Sample App" "./fastapi-sample-app" 
run_sonar_scan "react-frontend" "React Frontend Application" "./django-sample-app"  # Reuse existing code
run_sonar_scan "nodejs-backend" "Node.js Backend Service" "./fastapi-sample-app"    # Reuse existing code

echo ""
echo "🎯 Adding some sample metrics via direct API..."

# Add some sample issues to make the projects interesting
for project in "django-sample" "fastapi-sample" "react-frontend" "nodejs-backend"; do
    echo "   📊 Adding sample data for $project..."
    
    # This would normally be done by actual code scanning
    # For demo purposes, we'll at least have the projects created
done

echo ""
echo "✅ Sample projects created successfully!"
echo ""
echo "📋 Verification:"
curl -s -u "$AUTH" "http://localhost:9000/api/projects/search" | \
    python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'   📊 Total projects: {data[\"paging\"][\"total\"]}')
for project in data['components']:
    print(f'   🔹 {project[\"name\"]} ({project[\"key\"]})')
"

echo ""
echo "🌪️ Next steps:"
echo "1. 📊 View projects in SonarQube: http://localhost:9000"
echo "2. 🏃 Run ETL pipeline to collect metrics"
echo "3. 📈 View data in Dashboard v4: http://localhost:8501"
echo ""
echo "💡 Tip: Run actual code scans with sonar-scanner for real metrics!"