import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime 
import numpy as np
import csv


# Code for ETL operations on Country-GDP data

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[1].find_all('a') is not None:
                links = col[1].find_all('a')
                #print(links[1].get('title'))
                market_temp = str(col[2].contents[0])
                market_clean=market_temp.rstrip()
                data_dict = {"Name": links[1].get('title'),
                             "MC_USD_Billion": float(market_clean)}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path) 
    reader = csv.reader(open(csv_path, 'r'))
    first_row = next(reader) 
    exchange_rate = {}
    for row in reader:
        X, Y = row
        exchange_rate[X] = Y

    df['MC_GBP_Billion'] = [np.round(x*float(exchange_rate['GBP']),2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*float(exchange_rate['EUR']),2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*float(exchange_rate['INR']),2) for x in df['MC_USD_Billion']]

    return df
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)

''' definition of the required entities and call the relevant
functions in the correct order to complete the project.'''


log_file = "code_log.txt" 
target_file = "./Largest_banks_data.csv"
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = '/home/project/final_project/exchange_rate.csv'
table_attribs = ['Name','MC_USD_Billion']
table_name = 'Largest_banks'
db_name = 'Banks.db'
out_csv_path = '/home/project/final_project/Largest_banks_data.csv'
log_progress("Preliminaries complete. Initiating ETL process") 


extracted_data = extract(url,table_attribs)
log_progress("Data extraction complete. Initiating Transformation process") 
print(extracted_data)

transformed_data=transform(extracted_data, csv_path)
log_progress("Data transformation complete. Initiating Loading process") 
print(transformed_data)

load_to_csv(transformed_data, out_csv_path)
log_progress("Data saved to CSV file") 

conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated") 
load_to_db(transformed_data, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries") 

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, conn)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, conn)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, conn)

log_progress("Process Complete") 
conn.close()
log_progress("Server Connection closed") 

