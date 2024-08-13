import pandas as pd
import numpy as np


def transform(df, csv_path):
	exchange_rate = pd.read_csv(csv_path).set_index('Currency').to_dict()['Rate']
	df['MC_GBP_Billion'] = [np.round(float(x) * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
	df['MC_EUR_Billion'] = [np.round(float(x) * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
	df['MC_INR_Billion'] = [np.round(float(x) * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

	return df
