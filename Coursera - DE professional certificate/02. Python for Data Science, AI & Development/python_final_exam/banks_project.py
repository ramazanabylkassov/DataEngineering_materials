from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3
from datetime import datetime
import io

data_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attr_1 = ['Name', 'MC_USD_Billion']
table_attr_2 = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_bank'
log_file = 'code_log.txt'

def log_progress(message): 
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.'''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table_mark_cap = data.find_all('tbody')[0]
    rows = table_mark_cap.find_all('tr')
    
    for row in rows[1:]:
        col = row.find_all('td')
        bank_name = col[1].find_all('a')[1].text
        market_cap = float(col[2].text[:-1])
        if len(col)!=0:
            data_dict = {"Name": bank_name,
                            "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
    exchange_data = requests.get(exchange_url).text
    exchange_df = pd.read_csv(io.StringIO(exchange_data), sep=",").set_index('Currency')
    for bank in df.index:
        df.loc[bank, ['MC_GBP_Billion']] = round(df.loc[bank, 'MC_USD_Billion'] * float(exchange_df.loc['GBP', 'Rate']), 2)
        df.loc[bank, ['MC_EUR_Billion']] = round(df.loc[bank, 'MC_USD_Billion'] * float(exchange_df.loc['EUR', 'Rate']), 2)
        df.loc[bank, ['MC_INR_Billion']] = round(df.loc[bank, 'MC_USD_Billion'] * float(exchange_df.loc['INR', 'Rate']), 2)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(data_url, table_attr_1)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df, output_csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statements = []
query_statements.append(f"SELECT * FROM {table_name}")
query_statements.append(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}")
query_statements.append(f"SELECT Name from {table_name} LIMIT 5")

for query in query_statements:
    run_query(query, sql_connection)

log_progress('Process Complete')
sql_connection.close()
log_progress('Server Connection closed')

