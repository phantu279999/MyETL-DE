import pandas as pd
import sqlite3
from sqlalchemy import create_engine


def load_to_db(df: pd.DataFrame, table_name: str, conn_params: dict) -> None:
	"""Loads the data into a PostgreSQL database."""
	engine = create_engine(
		f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['dbname']}")
	df.to_sql(table_name, engine, if_exists='replace', index=False)


def load_to_db_sqlite(df: pd.DataFrame, db_name: str, table_name: str):
	"""Load the data into a Sqlile database."""
	conn = sqlite3.connect(db_name)
	df.to_sql(table_name, conn, if_exists='replace', index=False)
	conn.close()


def load_to_file_csv(df: pd.DataFrame, csv_path: str):
	df.to_csv(csv_path)
