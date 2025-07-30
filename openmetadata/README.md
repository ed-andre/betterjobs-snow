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

## Next Steps
1. After successful installation, visit http://localhost:8585 to start exploring OpenMetadata
2. Configure metadata ingestion from your data sources
3. Set up teams and users in Settings -> Users

For more detailed information, visit the [OpenMetadata Documentation](https://docs.open-metadata.org/).
