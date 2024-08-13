import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "logs\log_file.txt"
target_file = "transformed_data.csv"


def extract_from_csv(file_to_process):
	dataframe = pd.read_csv(file_to_process)
	return dataframe


def extract_from_json(file_to_process):
	dataframe = pd.read_json(file_to_process, lines=True)
	return dataframe


def extract_from_xml(file_to_process):
	dataframe = pd.DataFrame(columns=["name", "height", "weight"])
	tree = ET.parse(file_to_process)
	root = tree.getroot()
	for person in root:
		name = person.find("name").text
		height = float(person.find("height").text)
		weight = float(person.find("weight").text)
		dataframe = pd.concat(
			[dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True
		)
	return dataframe


def extract():
	# Initialize an empty DataFrame with specified columns
	extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

	# Process CSV files
	for csv_file in glob.glob("data\\raw_data1\\*.csv"):
		new_data = pd.DataFrame(extract_from_csv(csv_file))
		new_data = new_data.dropna(axis=1, how='all')  # Exclude empty or all-NA columns
		extracted_data = pd.concat([extracted_data, new_data], ignore_index=True)

	# Process JSON files
	for json_file in glob.glob("data\\raw_data1\\*.json"):
		new_data = pd.DataFrame(extract_from_json(json_file))
		new_data = new_data.dropna(axis=1, how='all')  # Exclude empty or all-NA columns
		extracted_data = pd.concat([extracted_data, new_data], ignore_index=True)

	# Process XML files
	for xml_file in glob.glob("data\\raw_data1\\*.xml"):
		new_data = pd.DataFrame(extract_from_xml(xml_file))
		new_data = new_data.dropna(axis=1, how='all')  # Exclude empty or all-NA columns
		extracted_data = pd.concat([extracted_data, new_data], ignore_index=True)

	return extracted_data


def transform(data):
	data['height'] = round(data.height * 0.0254, 2)
	data['weight'] = round(data.weight * 0.45359237, 2)
	return data


def load_data(target_file, transformed_data):
	transformed_data.to_csv(target_file)


def log_progress(message):
	timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
	now = datetime.now()  # get current timestamp
	timestamp = now.strftime(timestamp_format)
	with open(log_file, "a") as f:
		f.write(timestamp + ',' + message + '\n')


if __name__ == '__main__':
	log_progress("ETL Job Started")

	log_progress("Extract phase Started")
	extracted_data = extract()
	# Log the completion of the Extraction process
	log_progress("Extract phase Ended")

	# Log the beginning of the Transformation process
	log_progress("Transform phase Started")
	transformed_data = transform(extracted_data)
	print("Transformed Data")
	print(transformed_data)

	# Log the completion of the Transformation process
	log_progress("Transform phase Ended")

	# Log the beginning of the Loading process
	log_progress("Load phase Started")
	load_data(target_file, transformed_data)

	# Log the completion of the Loading process
	log_progress("Load phase Ended")

	# Log the completion of the ETL process
	log_progress("ETL Job Ended")
