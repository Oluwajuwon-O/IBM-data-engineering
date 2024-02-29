# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 08:02:06 2024

@author: oyalu
"""
import pandas as pd
import sqlite3

conn = sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
attribute_list = ['ID','FNAME','LNAME','CITY','CCODE']

df = pd.read_csv('INSTRUCTOR.csv', names= attribute_list)

df.to_sql(table_name, conn, if_exists= 'replace', index= False)
print('Table is ready')

# Viewing data from the table 
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# How many rows
count_query = f"SELECT COUNT(*) FROM {table_name}"
count_result = pd.read_sql(count_query, conn)
print(count_result)

# Appending data
data_dict = {'ID':[100],
             'FNAME':['John'],
             'LNAME':['Doe'],
             'CITY':['Paris'],
             'CCODE':['FR']}

data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists= 'append', index= False)
print('Data appended successfully')

# view db after append
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# conn.close()

cols = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']
dept_table_name = 'Departments'

dept_df = pd.read_csv('departments.csv', names= cols)

dept_df.to_sql(dept_table_name, conn, if_exists= 'replace', index= False)

dept_entry = pd.DataFrame({'DEPT_ID':[9], 
              'DEP_NAME':['Quality Assurance'], 
              'MANAGER_ID':[30010], 
              'LOC_ID':['L0010']})
dept_entry.to_sql(dept_table_name, conn, if_exists='append',index= False)

# view all entries of departments db
query1 = "SELECT * FROM Departments"
query1_output = pd.read_sql(query1, conn)
print(query1_output)

query2 = "SELECT dep_name from departments"
print(pd.read_sql(query2, conn))

query3 = "select count(*) from departments"
print(pd.read_sql(query3, conn))

conn.close()
