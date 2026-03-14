# Local Development Setup

## Prerequisites
- Docker Desktop (docker.com)
- Python 3.11+
- Node.js 20+

## Step 1 - Configure environment
```
cp .env.example .env
```
Open `.env` and fill in API keys. For Phase 1 only FRED requires registration (free forever).

## Step 2 - Start the stack
```
docker compose -f infra/docker/docker-compose.yml up -d
```
Starts: Zookeeper, Kafka, Schema Registry, MinIO, QuestDB, PostgreSQL.
Takes 30-60 seconds for all services to become healthy.

## Step 3 - Verify
```
docker compose -f infra/docker/docker-compose.yml ps
python scripts/verify_stack.py
```

## Service URLs
| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | agora / agora_secret |
| QuestDB Console | http://localhost:9003 | none |
| Schema Registry | http://localhost:8081 | none |

## Kafka UI (optional)
```
docker compose -f infra/docker/docker-compose.yml --profile dev up -d
# Then open http://localhost:8080
```

## Stopping
```
# Stop but keep data
docker compose -f infra/docker/docker-compose.yml down

# Stop and wipe everything
docker compose -f infra/docker/docker-compose.yml down -v
```
