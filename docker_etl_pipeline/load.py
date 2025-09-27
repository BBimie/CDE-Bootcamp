import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from transform import TransformWeatherData
import logging

logging.basicConfig(level=logging.INFO)

load_dotenv()

class LoadWeatherData:
    def __init__(self):
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.host = os.getenv("POSTGRES_DB_CONTAINER_NAME")
        self.port = 5432
        self.dbname = os.getenv("POSTGRES_DB")

        #check db connection cred
        if not all([self.user, self.password, self.host, self.port, self.dbname]):
            raise ValueError("One or more database environment variables are not set.")

        #transformed data
        self.transformed_data = TransformWeatherData().run_transform()

        # # Create a SQLAlchemy engine
        self.engine = create_engine(f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")

    def _setup_schema(self):
        create_cities_table = """
        CREATE TABLE IF NOT EXISTS cities (
            city_id INTEGER PRIMARY KEY,
            city_name VARCHAR(100) NOT NULL,
            country_code VARCHAR(2) NOT NULL
        );
        """
        create_readings_table = """
        CREATE TABLE IF NOT EXISTS weather_readings (
            id SERIAL PRIMARY KEY,
            city_id INTEGER REFERENCES cities(city_id),
            observation_timestamp_utc TIMESTAMP WITH TIME ZONE,
            temperature_celsius NUMERIC(5, 2),
            feels_like_celsius NUMERIC(5, 2),
            min_temperature_celsius NUMERIC(5, 2),
            max_temperature_celsius NUMERIC(5, 2),
            humidity_percent INTEGER,
            pressure_hpa INTEGER,
            visibility_meters INTEGER,
            wind_speed_ms NUMERIC(5, 2),
            wind_direction_deg INTEGER,
            weather_condition VARCHAR(50),
            weather_description VARCHAR(100),
            sunrise_utc TIMESTAMP WITH TIME ZONE,
            sunset_utc TIMESTAMP WITH TIME ZONE,
            daylight_duration_hours NUMERIC(4, 2),
            UNIQUE (city_id, observation_timestamp_utc)
        );
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_cities_table))
                conn.execute(text(create_readings_table))
                conn.commit()
            print("Schema is ready.")
        except Exception as e:
            print(f"An error occurred during schema setup: {e}")
            raise



    def load_data(self,):

        #setup db schema
        self._setup_schema()


        if not self.transformed_data:
            logging.info("No data to load. Aborting.")
            return

        weather_df = pd.DataFrame(self.transformed_data)

        #data for city table
        cities_df = weather_df[['city_id', 'city_name', 'country_code']].drop_duplicates().reset_index(drop=True)

        #data for the weather table
        weather_columns = [
            'city_id', 'temperature_celsius', 'feels_like_celsius', 'min_temperature_celsius',
            'max_temperature_celsius', 'humidity_percent', 'pressure_hpa', 'visibility_meters',
            'wind_speed_ms', 'wind_direction_deg', 'weather_condition', 'weather_description',
            'observation_timestamp_utc', 'sunrise_utc', 'sunset_utc',
        ]
        readings_df = weather_df[weather_columns]

        try:


            with self.engine.connect() as conn:

                #load cities data into cities table
                cities_df.to_sql(
                name='cities',
                con=conn,
                if_exists='append',
                index=False,
                method=self.upsert_cities # Custom method to handle primary key conflicts
            )
                
            # load readings data
            logging.info(f"Loading {len(weather_df)} weather readings into 'weather_readings' table...")
            readings_df.to_sql(
                'weather_readings',
                self.engine,
                if_exists='append',
                index=False
            )
        
        except Exception as e:
            logging.info(f"Could not load data, {e}")
            
    @staticmethod
    def upsert_cities(table, conn, keys, data_iter):
        """
        Custom method for pd.to_sql to handle ON CONFLICT DO NOTHING for the cities table.
        This prevents errors if a city already exists.
        """
        from sqlalchemy.dialects.postgresql import insert

        data = [dict(zip(keys, row)) for row in data_iter]

        stmt = insert(table.table).values(data)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['city_id']
        )
        conn.execute(stmt)

LoadWeatherData().load_data()