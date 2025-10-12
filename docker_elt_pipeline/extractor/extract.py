import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
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
            return df
        
        except Exception as e:
            logging.error(f"Failed to extract data from {self.url}. Error: {e}")
            raise

    def _connect_db(self):
        logging.info("Establishing database connection...")
        self.engine = create_engine(f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}/{self.DB_NAME}")
            

    def load(self,):
        df = self.extract()
    
        logging.info("Starting data loading process...")
        try:
            table_name = 'enterprise_survey_2023'

            with self.engine.begin() as conn:
                # Drop table and dependent views
                logging.info(f"Dropping existing table '{table_name}' with CASCADE...")
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))

                # Load data into the database
                df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

            logging.info(f"Data loaded into table '{table_name}' successfully.")

        except Exception as e:
            logging.error(f"Failed to load data into the database. Error: {e}")
            raise

if __name__ == "__main__":
    extractor = Extractor()
    extractor.load()
