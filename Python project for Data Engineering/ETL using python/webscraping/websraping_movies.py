# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 09:36:09 2024

@author: oyalu
"""

import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_file = 'top_50_films.csv'
df = pd.DataFrame(columns= ['Average_Rank','Film','Year'])
count = 0


# get webpage
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')
tables = 