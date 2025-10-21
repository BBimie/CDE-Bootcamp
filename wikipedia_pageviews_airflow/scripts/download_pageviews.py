import requests
from datetime import datetime
import logging
import shutil
from pathlib import Path
import gzip

logging.basicConfig(level=logging.INFO)

class DownloadPageViews:
    def __init__(self, day=None, hour=None):
        """Handles downloading and extracting Wikipedia pageview data for October 2025."""

        if day is None or hour is None:
            self.get_valid_october_datetime()
        else:
            self.day = day
            self.hour = hour

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
                logging.info("❌ Invalid input. Please enter a number.")

        self.day = day
        self.hour = hour

    def construct_pageview_filename(self):
        day_str = str(self.day).zfill(2)
        hour_str = str(self.hour).zfill(2)
        filename = f"pageviews-202510{day_str}-{hour_str}0000.gz"
        return filename

    def download_file(self):
        pageview_filename = self.construct_pageview_filename()
        url = f"https://dumps.wikimedia.org/other/pageviews/2025/2025-10/{pageview_filename}"

        logging.info(f"Downloading page views for October {self.day}, hour {self.hour}")
        data_dir = Path("./data")
        data_dir.mkdir(parents=True, exist_ok=True)

        download_path = data_dir / pageview_filename
        extracted_path = data_dir / pageview_filename.replace(".gz", "")

        # Step 1: Download
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(download_path, "wb") as f:
                shutil.copyfileobj(response.raw, f)
            logging.info(f"Download complete: {download_path}")
        else:
            logging.error(f"Failed to download file. Status code: {response.status_code}")
            return None

        # Step 2: Unzip
        logging.info("Extracting .gz file...")
        with gzip.open(download_path, "rb") as f_in:
            with open(extracted_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        logging.info(f"Extraction complete: {extracted_path}")

        # Step 3: Remove .gz file
        download_path.unlink(missing_ok=True)
        logging.info(f"Deleted compressed file: {download_path}")

        return extracted_path


if __name__ == "__main__":
    path = DownloadPageViews().download_file()
    logging.info(f"File ready for extraction at: {path}")
