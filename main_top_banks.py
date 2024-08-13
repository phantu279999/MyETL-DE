import sqlite3
from src.base_log import BaseLogger
from config.config_banks import *
from etl import extract_banks, transform_banks, load_banks

base_logger = BaseLogger('banks', log_file=log_file)


def run_query(query_statement):
	conn = sqlite3.connect(database_name)
	print(query_statement)
	for it in conn.execute(query_statement).fetchall():
		print(it)


if __name__ == '__main__':
	base_logger.info("----------------- Start ---------------")
	# Extract
	base_logger.info("Start Extract")
	data_extracted = extract_banks.extract(url, table_atrs)
	base_logger.info("Ended Extract")
	# Transform
	base_logger.info("Start Transform")
	df_transformed = transform_banks.transform(data_extracted, exchange_rate_path)
	base_logger.info("Ended Transform")

	# Load
	base_logger.info("Start Loading")
	load_banks.load_to_csv(df_transformed, csv_path)
	load_banks.load_to_db(df_transformed, database_name, table_name)
	base_logger.info("Ended Loading")

	run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks")

	base_logger.info("----------------- End ---------------")
