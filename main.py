import logging
from etl.extract import extract_from_csv, extract_from_api, extract_from_db
from etl.transform import clean_data, merge_data, aggregate_data
from etl.load import load_to_db
from config import config

logging.basicConfig(filename='logs/etl.log', level=logging.INFO)


def run_etl():
	try:
		# Extract
		csv_data = extract_from_csv(config.csv_path)
		api_data = extract_from_api(config.base_url, {"Authorization": f"Bearer {config.api_key}"})
		db_data = extract_from_db("SELECT * FROM sales", config.data_base)

		# Transform
		cleaned_csv = clean_data(csv_data)
		merged_data = merge_data(cleaned_csv, db_data, "id")
		final_data = aggregate_data(merged_data, "date", "sales_amount")

		# Load
		load_to_db(final_data, "sales_summary", config.data_base)

		logging.info("ETL process completed successfully.")

	except Exception as e:
		logging.error(f"ETL process failed: {str(e)}")


if __name__ == "__main__":
	run_etl()
