import pandas as pd
import requests
import psycopg2


def extract_from_csv(file_path: str) -> pd.DataFrame:
	"""Extracts data from a CSV file."""
	return pd.read_csv(file_path)


def extract_from_api(url: str, headers: dict) -> pd.DataFrame:
	"""Extracts data from an API endpoint."""
	response = requests.get(url, headers=headers)
	data = response.json()
	return pd.DataFrame(data)


def extract_from_db(query: str, conn_params: dict) -> pd.DataFrame:
	"""Extracts data from a database."""
	conn = psycopg2.connect(**conn_params)
	df = pd.read_sql(query, conn)
	conn.close()
	return df


def extract_html_from_api(url: str, headers: dict):
	response = requests.get(url, headers=headers)

	if response.status_code != 200:
		return ""
	return response.text
