import requests
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import sqlalchemy
import os
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, )

class Extractor:
    
    def __init__(self):
        self.url = os.environ['CSV_URL']
        self.DB_HOST =  os.getenv("DB_HOST", "localhost")
        self.DB_USER = os.environ["DB_USER"]
        self.DB_PASSWORD = os.environ["DB_PASSWORD"]
        self.DB_NAME = os.environ["DB_NAME"]

        self._connect_db()


    def extract(self) -> pd.DataFrame:
        logging.info("Starting data extraction process...")
        try:
            df = pd.read_csv(self.url)

            #get file name from url
            file_name = self.url.split("/")[-1]

            logging.info(f"{file_name} Data extracted successfully with shape: {df.shape}")
            df.to_csv(f"data.csv", index=False)
            return df
        
        except Exception as e:
            logging.error(f"Failed to extract data from {self.url}. Error: {e}")
            raise

    def _connect_db(self):
        logging.info("Establishing database connection...")
        self.engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}/{self.DB_NAME}")
            

    def load(self,):
        df = self.extract()
        
        logging.info("Starting data loading process...")
        try:
            conn = self.engine.connect()

            # Load data into the database
            table_name = 'enterprise_survey_2023'

            df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
            
            conn.close()
            logging.info(f"Data loaded into table '{table_name}' successfully.")

        except Exception as e:
            logging.error(f"Failed to load data into the database. Error: {e}")
            raise

if __name__ == "__main__":
    extractor = Extractor()
    extractor.load()
