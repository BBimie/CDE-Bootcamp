import os
import logging
import requests
import gzip
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)

class DownloadPageViews:
    """
    Handles downloading and extracting Wikipedia pageview files.
    """

    def __init__(self, day, hour):
        """
        Initialize with day and hour provided by the user.
        """
        self.base_url = "https://dumps.wikimedia.org/other/pageviews/2025/2025-10/"
        self.download_dir = Path("/opt/airflow/data/downloads")
        self.extract_dir = Path("/opt/airflow/data/extracted")

        self.day = day
        self.hour = hour

        # Make sure directories exist
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.extract_dir.mkdir(parents=True, exist_ok=True)

        # Construct filename and URL
        self.pageview_filename = self.__construct_pageview_filename()
        self.download_url = self.__construct_download_link()

    
    def get_valid_october_datetime(self):
        today = datetime.now()
        current_day = today.day
        current_hour = today.hour

        # Step 1: Get valid day
        while True:
            try:
                day = int(input("Enter the October date (1 - 311): "))
                if not (1 <= day <= 31):
                    logging.info("Please enter a valid day between 1 and 31.")
                    continue
                if day > current_day:
                    logging.info(f"You can only enter a date up to {current_day} October.")
                    continue
                break
            except ValueError:
                logging.info("Invalid input. Please enter a number.")

        # Step 2: Get valid hour
        while True:
            try:
                hour = int(input("Enter the hour (0 - 23): "))
                if not (0 <= hour <= 23):
                    logging.info("Please enter a valid hour between 0 and 23.")
                    continue
                if day == current_day and hour > current_hour:
                    logging.info(f"You can only enter up to {current_hour}:00 for today.")
                    continue
                break
            except ValueError:
                logging.info("Invalid input. Please enter a number.")

        self.day = day
        self.hour = hour

    def __construct_pageview_filename(self) -> str:
        """
        Build the Wikipedia pageview filename based on day and hour.
        """
        day_str = str(self.day).zfill(2)
        hour_str = str(self.hour).zfill(2)
        return f"pageviews-202510{day_str}-{hour_str}0000"

    def __construct_download_link(self) -> str:
        """
        Build the download link from the base URL and filename.
        """
        return f"{self.base_url}{self.pageview_filename}.gz"

    def download_file(self) -> Path:
        """
        Downloads the pageview .gz file and extracts it.
        Returns the path to the extracted file (no extension).
        """
        download_path = self.download_dir / f"{self.pageview_filename}.gz"
        extract_path = self.extract_dir / self.pageview_filename  # no extension

        logging.info(f"Downloading: {self.download_url}")

        try:
            response = requests.get(self.download_url, stream=True, timeout=30)
            response.raise_for_status()
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logging.error(f"Pageview file not available yet for Oct {self.day}, hour {self.hour}. "
                            "Try again later.")
            else:
                logging.error(f"❌ HTTP error while fetching file: {e}")
            return None  # stop execution gracefully

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error while fetching file: {e}")
            return None

        # Save .gz file
        with open(download_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logging.info(f"✅ Downloaded to: {download_path}")

        # Extract the .gz file (without extension)
        try:
            with gzip.open(download_path, "rb") as gz_file:
                with open(extract_path, "wb") as out_file:
                    out_file.write(gz_file.read())

            logging.info(f"📂 Extracted to: {extract_path}")

            # Optional cleanup (remove .gz)
            download_path.unlink(missing_ok=True)

            return extract_path
        
        except Exception as e:
            logging.error(f"❌ Error extracting .gz file: {e}")
            return None


def download_task_callable(**context):
    """
    This is the function your PythonOperator will run.
    It pulls the 'day' and 'hour' from the DAG's parameters (params).
    """
    
    #get day and hour from dag params
    params = context["params"]
    day = params.get("day")
    hour = params.get("hour")

    if day is None or hour is None:
        logging.error("DAG was triggered without 'day' or 'hour' parameters.")
        raise ValueError("Missing 'day' or 'hour' in DAG run configuration.")

    logging.info(f"Starting download for Oct {day}, hour {hour}")
    
    # Instantiate the class with the parameters
    downloader = DownloadPageViews(day=int(day), hour=int(hour))
    
    # Run the download
    extracted_file_path = downloader.download_file()
    
    # store the extracted file path in xcom
    if extracted_file_path:
        logging.info(f"Task complete. File is at: {extracted_file_path}")
        return extracted_file_path
    else:
        raise Exception("Download failed.")