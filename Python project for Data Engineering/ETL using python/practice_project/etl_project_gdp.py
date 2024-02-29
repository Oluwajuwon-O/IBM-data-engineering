#!/usr/bin/env python
# coding: utf-8

# In[151]:


# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    
    gdp_dict = {table_attribs[0]:[], table_attribs[1]:[]}
    
    web_page = requests.get(url).text
    html = BeautifulSoup(web_page,'html.parser')
    table = html.find_all('tbody')[2]
    
    rows = table.find_all('tr')
    for row in rows:
        if (len(row.find_all('td')) == 0):
            continue
        elif (len(row.find_all('td')[0].find_all('a')) == 0):
            continue
        elif (row.find_all('td')[2].get_text() == "—"):
            continue
        else:
            for j in gdp_dict.keys():
                if j == table_attribs[0]:
                    gdp_dict[j].append(row.find_all('td')[0].find_all('a')[0].get_text())
                if j == table_attribs[1]:
                    gdp_dict[j].append(row.find_all('td')[2].get_text())
        df = pd.DataFrame(gdp_dict)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    
    df['GDP_USD_billions'] = round(pd.to_numeric(df['GDP_USD_millions'].str.replace(',',''))/1e3,2)
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists= 'replace', index= False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement, sql_connection))

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    time_format = '%Y-%h-%d-%H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(time_format)
    with open('data\\etl_project_log_file.txt','a') as f:
        f.write(timestamp+' : '+ str(message)+'\n')


# In[3]:


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'data\\World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'data\\Countries_by_GDP.csv'


# In[ ]:


log_progress('Preliminaries complete. Initiating ETL process.')

gdp_data = extract(url,table_attribs)
log_progress('Data extraction complete. Initiating Transformation process.')


gdp_trans = transform(gdp_data)
log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(gdp_trans, csv_path)
log_progress('Data saved to CSV file.')

conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

load_to_db(gdp_trans, conn, table_name)
log_progress('Data loaded to Database as table. Running the query...')


query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement,conn)

conn.close()

