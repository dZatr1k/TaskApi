#!/usr/bin/env bash
# Runs ON THE SERVER (invoked by the GitHub Actions deploy job over SSH) from /opt/taskapi.
# Expects in the current directory: image.tar.gz, efbundle, docker-compose.yml, .env
#   usage: bash deploy.sh <image tag>
set -euo pipefail

TAG="${1:?image tag required}"
cd /opt/taskapi

set -a
# shellcheck disable=SC1091
source .env
set +a

echo "== load image taskapi:$TAG"
docker load -i image.tar.gz
rm -f image.tar.gz

echo "== start infrastructure"
docker compose up -d postgres rabbitmq
for _ in $(seq 1 30); do
  docker compose ps postgres --format '{{.Health}}' | grep -q healthy && break
  sleep 2
done

echo "== apply migrations (separate step, API has MigrateOnStartup=false)"
chmod +x efbundle
./efbundle --connection "Host=127.0.0.1;Port=5432;Database=taskapi;Username=taskapi;Password=${POSTGRES_PASSWORD}"

echo "== roll out api"
sed -i "s/^TASKAPI_TAG=.*/TASKAPI_TAG=${TAG}/" .env
grep -q '^TASKAPI_TAG=' .env || echo "TASKAPI_TAG=${TAG}" >> .env
TASKAPI_TAG="$TAG" docker compose up -d api

echo "== health"
for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8080/health; then echo; break; fi
  sleep 2
done
curl -fsS http://127.0.0.1:8080/health >/dev/null || { echo "API is not healthy"; docker compose logs --tail=50 api; exit 1; }

echo "== cleanup old images"
docker image prune -f >/dev/null
echo "deployed taskapi:$TAG"
