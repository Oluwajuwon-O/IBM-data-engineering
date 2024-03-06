#!/usr/bin/env python
# coding: utf-8

# Code for ETL operations on Country-GDP data

# Importing the required libraries

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_format = '%Y-%h-%d:%I-%M-%S-%f %p'
    current_time = datetime.now()
    timestamp = current_time.strftime(time_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ' : '+ str(message) + '\n')
        
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    webpage = requests.get(url).text
    soup = BeautifulSoup(webpage,'html.parser')
    table = soup.find('table')
    
    df = pd.DataFrame(columns=table_attribs)

    for col in table.find_all('tr'):
        if len(col.find_all('td')) == 0:
            continue
        bank_dict = {table_attribs[0]: col.find_all('td')[1].get_text().strip(),
                     table_attribs[1]: float(col.find_all('td')[2].get_text().strip())}
        bank_df = pd.DataFrame(bank_dict, index=[0])
        df = pd.concat([df,bank_df], ignore_index=True) 
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    currency = ['MC_EUR_Billion' , 'MC_GBP_Billion', 'MC_INR_Billion']

    for i,j in zip(currency, exchange_rate_dict.keys()):
        df[i] = np.round(df['MC_USD_Billion'] * exchange_rate_dict[j],2)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection,if_exists = 'replace', index= False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement, sql_connection))

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

table_attribs = ['Name', 'MC_USD_Billion']
bank_df = extract(url,table_attribs)

log_progress('extracting data...')
print(bank_df)
log_progress('extraction complete')

log_progress('transforming data...')
transformed_bank_df = transform(bank_df, 'exchange_rate.csv')
print(transformed_bank_df)
print('''df['MC_EUR_Billion'][4]''', bank_df['MC_EUR_Billion'][4])
log_progress('transforming complete')

log_progress('loading to csv...')
csv_path = './Largest_banks_data.csv'
load_to_csv(transformed_bank_df,csv_path)
log_progress('loading to csv complete ...')

db_name = 'Banks.db'
table_name = 'Largest_banks'
conn = sqlite3.connect(db_name)

log_progress('loading to database...')
load_to_db(transformed_bank_df, conn, table_name)
log_progress('loaded to database')

query1 = 'SELECT * FROM Largest_banks'
query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3 = 'SELECT Name FROM largest_banks LIMIT 5'

log_progress('running queries')
run_query(query1, conn)
run_query(query2, conn)
run_query(query3, conn)
log_progress('ran all the queries')