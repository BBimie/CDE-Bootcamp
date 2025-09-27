#!/bin/bash
set -e

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Cleaning up previous docker runs
echo "=> Cleaning up previous runs"
docker stop "$POSTGRES_DB_CONTAINER_NAME" 2>/dev/null || true
docker rm "$POSTGRES_DB_CONTAINER_NAME" 2>/dev/null || true
docker network rm "$NETWORK_NAME" 2>/dev/null || true
echo "=== Cleanup complete."

# CREATE DOCKER NETWORK
echo "=> Creating Docker network '$NETWORK_NAME' "
docker network create "$NETWORK_NAME"

# BUILD POSTGRES DATABASE CONTAINER
echo "=> Starting PostgreSQL container '$POSTGRES_DB_CONTAINER_NAME'"
docker run -d \
    --name "$POSTGRES_DB_CONTAINER_NAME" \
    --network "$NETWORK_NAME" \
    -p 5433:5432 \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -e POSTGRES_DB="$POSTGRES_DB" \
    --restart always \
    postgres:14-alpine

# BUILD THE PYTHON ETL APP IMAGE
echo "=> Building the ETL app image '$ETL_IMAGE_NAME' "
docker build -t "$ETL_IMAGE_NAME" .

# WAIT FOR POSTGRES TO BE READY
echo "=> Waiting for PostgreSQL to accept connections..."
until docker exec "$POSTGRES_DB_CONTAINER_NAME" pg_isready -q -U "$POSTGRES_USER"; do
  echo "   PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "   PostgreSQL is ready!"

docker run --rm \
  --name "$ETL_CONTAINER" \
  --network "$NETWORK_NAME" \
  --env-file .env \
  "$ETL_IMAGE_NAME"


echo "--- ETL Pipeline Finished Successfully ---"

