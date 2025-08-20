# OpenMetadata Local Setup

This guide will help you set up OpenMetadata locally using Docker and PostgreSQL.

## Prerequisites

### Docker Setup
1. Install [Docker](https://docs.docker.com/get-docker/) (version 20.10.0 or greater)
2. Install [Docker Compose](https://docs.docker.com/compose/install/) (version v2.1.1 or greater)
3. Allocate at least 6GB memory and 4 CPUs to Docker:
   - Open Docker Desktop
   - Go to Settings/Preferences -> Resources -> Advanced
   - Set Memory to 6GB or more
   - Set CPUs to 4 or more

## Installation Steps

1. **Verify Docker and Docker Compose Installation**
   ```bash
   docker --version
   docker compose version
   ```

2. **Start OpenMetadata Services**
   ```bash
   # Using PostgreSQL (recommended)
   docker compose -f docker-compose-postgres.yml up --detach
   ```

3. **Verify Services**
   ```bash
   docker ps
   ```
   You should see containers for:
   - OpenMetadata Server
   - PostgreSQL
   - Elasticsearch
   - Ingestion Service (Airflow)

## Accessing Services

### OpenMetadata UI
- URL: http://localhost:8585
- Default credentials:
  - Username: `admin@open-metadata.org`
  - Password: `admin`

### Airflow UI (Ingestion Service)
- URL: http://localhost:8090 (Note: Changed from default 8080 to avoid conflicts)
- Default credentials:
  - Username: `admin`
  - Password: `admin`

## Managing Services

### Stop Services
```bash
docker compose -f docker-compose.yml stop
```

### Start Services
```bash
docker compose -f docker-compose.yml start
```

### Cleanup
```bash
# Stop and remove containers
docker compose -f docker-compose-postgres.yml down

# To also remove volumes (WARNING: This will delete all data)
docker compose -f docker-compose-postgres.yml down --volumes
```

## Troubleshooting

### Port Conflicts
- The default setup uses the following ports. Make sure they are available:
  - 8585: OpenMetadata UI
  - 8090: Airflow UI (Ingestion Service)
  - 9200: Elasticsearch
  - 5432: PostgreSQL

### Network Issues
If you see network-related errors:
```bash
# Clean up unused networks
docker network prune
```

### Volume Permissions (Windows WSL2)
If you encounter permission errors with volumes on Windows WSL2, add the following to `/etc/wsl.conf`:
```
[automount]
options = "metadata"
```

## Database Backup and Restoration

### Creating a Backup

Before upgrading OpenMetadata or making major changes, it's recommended to backup the PostgreSQL database:

1. **Navigate to the OpenMetadata directory and create backup folder**
   ```bash
   cd openmetadata
   mkdir -p backup_db
   ```

   ```powershell
   # For Windows PowerShell:
   cd openmetadata
   New-Item -ItemType Directory -Force -Path backup_db
   ```

2. **Create a timestamped backup**
   ```bash
   # Generate backup filename with current timestamp
   BACKUP_FILE="backup_db/backup_$(date +%Y%m%d%H%M).sql"

   # For Windows PowerShell:
   # $BACKUP_FILE = "backup_db/backup_$(Get-Date -Format 'yyyyMMddHHmm').sql"

   # Create the backup
   docker exec -e PGPASSWORD=openmetadata_password openmetadata_ingestion pg_dump -U openmetadata_user -h postgresql -d openmetadata_db > $BACKUP_FILE
   ```

3. **Verify backup was created**
   ```bash
   ls -la backup_db/backup_*.sql

   # For Windows PowerShell:
   # dir backup_db/backup_*.sql
   ```

### Restoring from Backup (if needed)

If you need to restore from a backup:

1. **Create a restore database**
   ```bash
   docker exec -e PGPASSWORD=openmetadata_password openmetadata_postgresql psql -U postgres -c "CREATE DATABASE restore;"
   docker exec -e PGPASSWORD=openmetadata_password openmetadata_postgresql psql -U postgres -c "ALTER DATABASE restore OWNER TO openmetadata_user;"
   ```

2. **Restore from backup**
   ```bash
   # Use the specific backup file from backup_db folder
   docker exec -e PGPASSWORD=openmetadata_password -i openmetadata_ingestion psql -U openmetadata_user -h postgresql -d restore < backup_db/backup_YYYYMMDDHHMM.sql
   ```

3. **Update environment and restart**
   ```bash
   export OM_DATABASE=restore
   docker compose down
   docker compose up -d
   ```

### ⚠️ Security Notice
- **Always delete backup files after use** to avoid storing sensitive metadata locally
- **Never commit backup files to version control** - add `backup_db/` to your `.gitignore`
- Backup files contain sensitive database information including connection strings and metadata
- The `backup_db` folder structure makes it easy to exclude all backups with a single gitignore entry

## Next Steps
1. After successful installation, visit http://localhost:8585 to start exploring OpenMetadata
2. Configure metadata ingestion from your data sources
3. Set up teams and users in Settings -> Users

For more detailed information, visit the [OpenMetadata Documentation](https://docs.open-metadata.org/).
