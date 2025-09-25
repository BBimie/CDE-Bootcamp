#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Starting the Weather ETL Pipeline ---"

echo "=>Docker Build"

# Step 2: Wait for the PostgreSQL database to be ready
echo "=> Waiting for PostgreSQL to be ready..."
# Use docker-compose exec to run pg_isready inside the db container
until docker-compose exec -T db pg_isready -q -U "$POSTGRES_USER"; do
  echo "   PostgreSQL is unavailable - sleeping"
  sleep 1
done
echo "   PostgreSQL is ready!"

# Step 3: Run the database schema setup (if you have one)
# echo "=> Running database migrations/schema setup..."
# docker-compose exec -T app python setup_database.py

# Step 4: Run the full ETL pipeline
echo "=> Running the ETL script..."
docker-compose exec -T app python load.py

echo "--- ETL Pipeline Finished Successfully ---"

# Optional: Stop the containers after the run
# echo "=> Stopping containers..."
# docker-compose down