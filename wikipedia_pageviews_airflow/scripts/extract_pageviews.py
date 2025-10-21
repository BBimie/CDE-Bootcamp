import logging
from pathlib import Path
from download_pageviews import DownloadPageViews

logging.basicConfig(level=logging.INFO)

class ExtractPageViews:
    def __init__(self, extracted_path):
        self.extracted_path = Path(extracted_path)
        self.output_path = self.extracted_path

        self.companies = {
            'Amazon': 'en Amazon_(company)',
            'Apple': 'en Apple_Inc.',
            'Facebook': 'en Facebook',
            'Google': 'en Google',
            'Microsoft': 'en Microsoft'
        }

    def extract(self):
        logging.info(f"Extracting company pageviews from: {self.extracted_path}")

        # Reverse the mapping for easier lookup (page title → company name)
        page_titles = {v: k for k, v in self.companies.items()}
        results = {company: 0 for company in self.companies}

        try:
            with open(self.extracted_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(" ")

                    # Skip malformed lines
                    if len(parts) < 3:
                        continue

                    domain_code, page_title, view_count, response_size = parts[0], parts[1], parts[2], parts[3]

                    # Combine first two fields for comparison (e.g., "en Apple_Inc.")
                    full_title = f"{domain_code} {page_title}"

                    if full_title in page_titles:
                        company = page_titles[full_title]
                        try:
                            results[company] = int(view_count)
                        except ValueError:
                            logging.warning(f"⚠️ Could not parse view count for {company}")

            logging.info(f"✅ Extracted pageviews: {results}")
            return results

        except FileNotFoundError:
            logging.error(f"❌ Extracted file not found: {self.extracted_path}")
            return None


if __name__ == "__main__":
    file_name = DownloadPageViews().construct_pageview_filename()
    file_path = f"./data/{file_name}".replace(".gz", "") 
    extractor = ExtractPageViews(file_path).extract()
