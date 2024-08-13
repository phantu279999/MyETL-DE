import pandas as pd
from bs4 import BeautifulSoup

from etl.extract import extract_html_from_api
from etl.transform import transform_data_top_movies
from etl.load import load_to_db_sqlite, load_to_file_csv

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])

if __name__ == '__main__':
	# Extract
	html_page = extract_html_from_api(url, {})
	data = BeautifulSoup(html_page, 'html.parser')

	# Transform
	df = transform_data_top_movies(df, data)
	# print(df)

	# Load
	load_to_file_csv(df, csv_path)
	load_to_db_sqlite(df, db_name, table_name)

