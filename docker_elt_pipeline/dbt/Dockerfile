# Use a Python base image, as dbt is a Python tool
FROM python:3.11-slim

# Set the working directory for dbt
WORKDIR /dbt

# Install dbt and its PostgreSQL adapter from your requirements file
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire dbt project (dbt.yml, profiles.yml, models/, etc.)
# into the container's working directory.
COPY . .

# This is the command that will run when the container starts
CMD ["dbt", "run"]