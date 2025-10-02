# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /docker_elt_pipeline

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your ETL scripts (extract.py, transform.py, etc.) into the container
COPY . .

# This is the command that will run when the container starts
CMD ["python", "extract_load.py"]