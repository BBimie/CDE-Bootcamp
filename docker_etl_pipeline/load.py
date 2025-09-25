import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

class LoadWeatherData:
    def __init__(self):
        # Database connection details from environment variables
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        dbname = os.getenv("DB_NAME")

        if not all([user, password, host, port, dbname]):
            raise ValueError("One or more database environment variables are not set.")

        # Create a SQLAlchemy engine
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")
        print("Successfully connected to the PostgreSQL database.")

    def load_data(self, transformed_data: list):
        """
        Loads the transformed data into the 'cities' and 'weather_readings' tables.

        Args:
            transformed_data (list): A list of dictionaries with cleaned weather data.
        """
        if not transformed_data:
            print("No data to load. Aborting.")
            return

        # 1. Convert the list of dicts to a pandas DataFrame
        weather_df = pd.DataFrame(transformed_data)

        # 2. Prepare the data for the 'cities' table
        # Select unique cities and the necessary columns
        cities_df = weather_df[['city_id', 'city_name', 'country_code']].drop_duplicates().reset_index(drop=True)

        # 3. Prepare the data for the 'weather_readings' table
        # Ensure columns are in the correct order for insertion
        weather_columns = [
            'city_id', 'temperature_celsius', 'feels_like_celsius', 'min_temperature_celsius',
            'max_temperature_celsius', 'humidity_percent', 'pressure_hpa', 'visibility_meters',
            'wind_speed_ms', 'wind_direction_deg', 'weather_condition', 'weather_description',
            'observation_timestamp_utc', 'sunrise_utc', 'sunset_utc', 'daylight_duration_hours'
        ]
        readings_df = weather_df[weather_columns]

        # Use a single connection for all operations
        with self.engine.connect() as conn:
            # 4. UPSERT into the 'cities' table
            print(f"Upserting {len(cities_df)} unique cities into the 'cities' table...")
            for index, row in cities_df.iterrows():
                sql_cities = text("""
                    INSERT INTO cities (city_id, city_name, country_code)
                    VALUES (:city_id, :city_name, :country_code)
                    ON CONFLICT (city_id) DO NOTHING;
                """)
                conn.execute(sql_cities, row.to_dict())

            # 5. UPSERT into the 'weather_readings' table
            print(f"Upserting {len(readings_df)} weather readings into the 'weather_readings' table...")
            for index, row in readings_df.iterrows():
                sql_readings = text("""
                    INSERT INTO weather_readings (
                        city_id, temperature_celsius, feels_like_celsius, min_temperature_celsius,
                        max_temperature_celsius, humidity_percent, pressure_hpa, visibility_meters,
                        wind_speed_ms, wind_direction_deg, weather_condition, weather_description,
                        observation_timestamp_utc, sunrise_utc, sunset_utc
                    ) VALUES (
                        :city_id, :temperature_celsius, :feels_like_celsius, :min_temperature_celsius,
                        :max_temperature_celsius, :humidity_percent, :pressure_hpa, :visibility_meters,
                        :wind_speed_ms, :wind_direction_deg, :weather_condition, :weather_description,
                        :observation_timestamp_utc, :sunrise_utc, :sunset_utc
                    )
                    ON CONFLICT (city_id, observation_timestamp_utc) DO NOTHING;
                """)
                conn.execute(sql_readings, row.to_dict())
            
            # Commit the transaction to make the changes permanent
            conn.commit()
            print("Load process complete. Data has been successfully committed.")