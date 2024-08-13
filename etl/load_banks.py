import sqlite3


def load_to_csv(df, output_path):
	df.to_csv(output_path)


def load_to_db(df, database_name, table_name):
	conn = sqlite3.connect(database_name)
	df.to_sql(table_name, conn, if_exists='replace', index=False)
	conn.close()
