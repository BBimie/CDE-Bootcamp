# OpenweatherAPI ETL Pipeline with Docker and PostgreSQL

### Objective
This project implements a containerized ETL pipeline to collect and store weather data for Nigeria’s 36 state capitals and the FCT


### Components
1. **Data Source**: [OpenWeather API](https://api.openweathermap.org/data/2.5/weather)
2. **ETL**: Extracts weather data, transforms it, and retains clean, important information
3. **Storage**: PostgreSQL
4. **Containerization**: Docker
5. **Orchestration**: Bash scripting

### Architecture Diagram
<img src=docker_etl_pipeline/assets/achitecturaldiagram.png>

#### Steps

1. **Data Ingestion**  
   - **Extract**: Script to get data from the API  
   - **Transform**: Clean, enrich, and prepare data for loading  
   - **Load**: Database schema designed to match data structure, then load data  

   - **ERD**  
   <img src=docker_etl_pipeline/assets/erd.png>

2. **ETL Orchestration with `run_etl.sh`**  
   - Uses DB & network credentials provided in the `.env` file  
   - Creates a new, isolated PostgreSQL database inside a Docker container using the env variables  
   - Runs the ETL project  

### How to Run
1. Clone the repository  
2. Get an API key from OpenWeather  
3. Set up `.env`  
```bash
API_KEY=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
POSTGRES_DB_CONTAINER_NAME=
ETL_IMAGE_NAME=
ETL_CONTAINER=
NETWORK_NAME=
```
4. Make orchestration script executable \
 `chmod +x run_etl.sh`

5. Create virtual environment and install the packages in `requirements.txt`

6. Execute the orchestration script \
 `./run_etl.sh`


After running successfully, you can connect to the DB and run \
`SELECT * FRON cities;` \
AND \
`SELECT * FROM weather_readings;`




