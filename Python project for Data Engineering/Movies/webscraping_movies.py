
# import libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup



url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_file = 'top_100_films.csv'

df = pd.DataFrame(columns= ['Average_Rank','Film','Year'])


# get webpage and parse
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# get table
table = data.find('tbody')
rows = table.find_all('tr')



# arrange in dataframe
movies_dict = {'Average_Rank':[],'Film':[],'Year':[]}

for row in rows:
    if len(row.find_all('td')) == 0:
        continue
    for i,j in enumerate(movies_dict.keys()):
        movies_dict[j].append(row.find_all('td')[i].get_text())

movies_df = pd.DataFrame(movies_dict)


# save data to csv
movies_df.to_csv(csv_file, index=False)






