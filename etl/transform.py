import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
	"""Cleans data by handling missing values and duplicates."""
	df = df.drop_duplicates()
	df = df.fillna(method='ffill')
	return df


def merge_data(df1: pd.DataFrame, df2: pd.DataFrame, on: str) -> pd.DataFrame:
	"""Merges two dataframes on a common key."""
	return pd.merge(df1, df2, on=on)


def aggregate_data(df: pd.DataFrame, group_by_col: str, agg_col: str) -> pd.DataFrame:
	"""Aggregates data based on a grouping column."""
	return df.groupby(group_by_col).agg({agg_col: 'sum'}).reset_index()
