import pandas as pd
import requests
from bs4 import BeautifulSoup


def extract(url, table_atrs):
	html_page = requests.get(url).text
	data = BeautifulSoup(html_page, 'html.parser')
	df = pd.DataFrame(columns=table_atrs)
	table = data.find('table', attrs={'class': 'wikitable sortable mw-collapsible'})
	for it in table.find_all('tr'):
		try:
			columns = it.find_all('td')
			data_dict = {
				"Name": columns[1].text.replace("\n", ""),
				"MC_USD_Billion": columns[2].text.replace("\n", "")
			}
			df1 = pd.DataFrame(data_dict, index=[0])
			df = pd.concat([df, df1], ignore_index=True)
		except Exception as ex:
			# print(ex)
			pass
	return df
