"""
Banks_ETL_Pipeline_Project

This script implements an end-to-end ETL (Extract, Transform, Load) pipeline that:
1. Extracts a table of the world's largest banks by market capitalization from a historical snapshot of a Wikipedia page.
2. Transforms the data by cleaning the market cap values and converting them from USD to GBP, EUR, and INR using exchange rates from a CSV file.
3. Loads the transformed data into both a CSV file and a SQLite database.

It also logs progress at each step and runs a few SQL queries on the loaded database to verify the results.

"""

# Importing the required libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import numpy as np

def log_progress(message):
    """Appends a timestamped message to code_log.txt"""
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as log_file:
        log_file.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs=None):
    """Extracts the first wikitable sortable and returns a cleaned DataFrame."""
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")

    tables = soup.find_all("table", class_=lambda x: x and "wikitable" in x and "sortable" in x)
    df = pd.read_html(str(tables[0]))[0]

    # Clean and rename Market Cap column
    df["Market cap (US$ billion)"] = df["Market cap (US$ billion)"].astype(str).str.replace(r'\n', '', regex=True)
    df["Market cap (US$ billion)"] = df["Market cap (US$ billion)"].astype(float)
    df.rename(columns={"Market cap (US$ billion)": "MC_USD_Billion"}, inplace=True)

    return df

def transform(df, csv_path):
    """Adds columns with market cap converted to GBP, EUR, and INR."""
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    """Saves the DataFrame to a CSV file."""
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    """Writes the DataFrame to a database table."""
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    """Executes a SQL query and prints the result."""
    print(f"\nExecuting query:\n{query_statement}\n")
    result = pd.read_sql(query_statement, sql_connection)
    print(result)

# === ETL Process ===

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "exchange_rate.csv"
output_csv_path = "Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url)
log_progress("Data extraction complete. Initiating transformation")

df = transform(df, csv_path)
log_progress("Data transformation complete. Initiating load operations")

load_to_csv(df, output_csv_path)
log_progress("Data saved to CSV file")

db_connection = sqlite3.connect(db_name)
log_progress("SQL connection established")

load_to_db(df, db_connection, table_name)
log_progress("Data loaded to database table")

run_query("SELECT * FROM Largest_banks", db_connection)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", db_connection)
run_query('SELECT "Bank name" FROM Largest_banks LIMIT 5', db_connection)
log_progress("Queries executed successfully")

db_connection.close()
log_progress("SQL connection closed. ETL process completed.")