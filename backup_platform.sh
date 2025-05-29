#!/bin/bash

# Platform Backup Script for v4
# This script backs up all important data before any major changes

BACKUP_DIR="./backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "🛡️ Creating Platform Backup"
echo "=========================="
echo "📁 Backup directory: $BACKUP_DIR"

# Backup SonarQube data
echo "📊 Backing up SonarQube data..."
docker-compose exec -T postgres pg_dump -U postgres sonarqube_metrics > "$BACKUP_DIR/sonarqube_metrics.sql"

# Export SonarQube projects
echo "📋 Exporting SonarQube projects..."
curl -s -u admin:admin "http://localhost:9000/api/projects/search" > "$BACKUP_DIR/sonarqube_projects.json"

# Backup Docker volumes
echo "💾 Backing up Docker volumes..."
docker run --rm -v dev-productivity-intelligence_sonarqube_data:/data -v $(pwd)/$BACKUP_DIR:/backup alpine tar czf /backup/sonarqube_data.tar.gz -C /data .
docker run --rm -v dev-productivity-intelligence_gitea_data:/data -v $(pwd)/$BACKUP_DIR:/backup alpine tar czf /backup/gitea_data.tar.gz -C /data .
docker run --rm -v dev-productivity-intelligence_jenkins_data:/data -v $(pwd)/$BACKUP_DIR:/backup alpine tar czf /backup/jenkins_data.tar.gz -C /data .

# Create restore script
cat > "$BACKUP_DIR/restore.sh" << 'EOF'
#!/bin/bash
echo "🔄 Restoring Platform Data"
BACKUP_DIR=$(dirname "$0")

# Stop services
docker-compose down

# Restore volumes
docker run --rm -v dev-productivity-intelligence_sonarqube_data:/data -v $BACKUP_DIR:/backup alpine tar xzf /backup/sonarqube_data.tar.gz -C /data
docker run --rm -v dev-productivity-intelligence_gitea_data:/data -v $BACKUP_DIR:/backup alpine tar xzf /backup/gitea_data.tar.gz -C /data  
docker run --rm -v dev-productivity-intelligence_jenkins_data:/data -v $BACKUP_DIR:/backup alpine tar xzf /backup/jenkins_data.tar.gz -C /data

# Restore database
docker-compose up -d postgres
sleep 10
docker-compose exec -T postgres psql -U postgres -d sonarqube_metrics < sonarqube_metrics.sql

# Start services
docker-compose up -d

echo "✅ Restore complete!"
EOF

chmod +x "$BACKUP_DIR/restore.sh"

echo "✅ Backup complete!"
echo "📁 Location: $BACKUP_DIR"
echo "🔄 To restore: $BACKUP_DIR/restore.sh"