import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)

class ExtractPageViews:
    def __init__(self, extracted_path, companies : dict):
        self.extracted_path = Path(extracted_path)
        self.companies = companies

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
                    if len(parts) < 4:
                        continue

                    domain_code, page_title, view_count, response_size = parts[0], parts[1], parts[2], parts[3]

                    # Combine first two fields for comparison (e.g., "en Apple_Inc.")
                    full_title = f"{domain_code} {page_title}"

                    if full_title in page_titles:
                        company = page_titles[full_title]
                        try:
                            results[company] = {'domain_code': domain_code, 
                                                'page_title': page_title, 
                                                'views' : view_count, 
                                                'resp_size': response_size
                                                }
                        except ValueError:
                            logging.warning(f"⚠️ Could not parse view count for {company}")

            logging.info(f"✅ Extracted pageviews")
            return results

        except FileNotFoundError:
            logging.error(f"Extracted file not found: {self.extracted_path}")
            return None


def extract_task_callable(**context):
    """
    Airflow callable function for extracting pageviews.
    Pulls file path from XCom and company list from params.
    """
    logging.info("--- Starting Extract Task ---")

    # Get file path from 'download' task via XCom
    task_instance = context['task_instance']
    input_file_path = task_instance.xcom_pull(task_ids='download')

    if not input_file_path:
        logging.error("❌ No file path received from XCom. Upstream task 'download' may have failed.")
        raise ValueError("Could not get file path from upstream task.")

    logging.info(f"Received file path from XCom: {input_file_path}")

    # Get companies dictionary from DAG params (or use default)
    params = context["params"]
    companies_dict = params.get('companies',)
    logging.info(f"Filtering for companies: {list(companies_dict.keys())}")

    # Instantiate and run the extractor
    try:
        extractor = ExtractPageViews(
            extracted_path=input_file_path, 
            companies=companies_dict
        )
        extraction_results = extractor.extract()

        if extraction_results is None:
            logging.error("❌ Extraction failed, returned None (e.g., file not found).")
            raise FileNotFoundError("Extraction process failed to find file.")
        
        # This pushes the entire dictionary to XCom for the 'load' task
        logging.info(f"✅ Extraction successful. Pushing results to XCom.")
        return extraction_results

    except Exception as e:
        logging.error(f"❌ An error occurred during extraction: {e}")
        raise