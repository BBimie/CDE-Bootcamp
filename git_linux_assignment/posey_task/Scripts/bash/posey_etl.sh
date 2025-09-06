#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  source .env
else
  echo ".env file not found!"
  exit 1
fi

DATA_DIR="./data"

# create the database if it doesn't exist
PGPASSWORD="$PG_PASSWORD" psql \
  -h "$PG_HOST" \
  -U "$PG_USER" \
  -tc "SELECT 1 FROM pg_database WHERE datname = '$PG_DB_NAME'" | grep -q 1 || \
  PGPASSWORD="$PG_PASSWORD" psql \
  -h "$PG_HOST" \
  -U "$PG_USER" \
  -c "CREATE DATABASE \"$PG_DB_NAME\";"

echo "=== Database $PG_DB_NAME has been created! ==="

for file in "$DATA_DIR"/*.csv; do
  if [ -f "$file" ]; then
    table_name=$(basename "$file" .csv)
    echo "=== Preparing table $table_name"

    # use csv header to CREATE TABLE
    header=$(head -1 "$file")
    columns=$(echo "$header" | tr ',' '\n' | awk '{print $0 " TEXT"}' | paste -sd, -)
    create_sql="CREATE TABLE IF NOT EXISTS $table_name ($columns);"

    PGPASSWORD="$PG_PASSWORD" psql \
      -h "$PG_HOST" \
      -U "$PG_USER" \
      -d "$PG_DB_NAME" \
      -c "$create_sql"

    echo "=== Loading $file into table $table_name"

    PGPASSWORD="$PG_PASSWORD" psql \
      -h "$PG_HOST" \
      -U "$PG_USER" \
      -d "$PG_DB_NAME" \
      -c "\copy $table_name FROM '$file' WITH (FORMAT csv, HEADER true)"
  fi
done