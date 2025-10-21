import psycopg2
import logging

class LoadPageViews:
    def __init__(self, data, db_config=None):
        """
        data: dict of company -> view count
        db_config: dict with keys host, dbname, user, password, port
        """
        self.data = data
        self.db_config = db_config or {
            "host": "localhost",
            "dbname": "wikipedia_db",
            "user": "airflow",
            "password": "airflow",
            "port": 5432
        }

    def load(self):
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Create table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pageviews (
                    id SERIAL PRIMARY KEY,
                    company_name TEXT,
                    view_count INT,
                    log_date TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()

            # Insert data
            for company, count in self.data.items():
                cursor.execute(
                    "INSERT INTO pageviews (company_name, view_count) VALUES (%s, %s);",
                    (company, count)
                )
            conn.commit()

            logging.info("✅ Data successfully loaded into PostgreSQL")
            cursor.close()
            conn.close()

        except Exception as e:
            logging.error(f"❌ Failed to load data into database: {e}")
