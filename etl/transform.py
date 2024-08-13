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


def transform_data_top_movies(df: pd.DataFrame, data, count=0):
	tables = data.find_all('tbody')
	rows = tables[0].find_all('tr')

	for row in rows:
		if count < 50:
			col = row.find_all('td')
			if len(col) != 0:
				data_dict = {
					"Average Rank": col[0].contents[0],
					"Film": col[1].contents[0],
					"Year": col[2].contents[0]
				}
				df1 = pd.DataFrame(data_dict, index=[0])
				df = pd.concat([df, df1], ignore_index=True)
				count += 1
		else:
			break

	return df
