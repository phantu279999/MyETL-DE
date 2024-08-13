import pandas as pd
from sqlalchemy import create_engine


def load_to_db(df: pd.DataFrame, table_name: str, conn_params: dict) -> None:
	"""Loads the data into a PostgreSQL database."""
	engine = create_engine(
		f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['dbname']}")
	df.to_sql(table_name, engine, if_exists='replace', index=False)
