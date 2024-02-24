# -*- coding: utf-8 -*-
"""
Created on Sun Feb 18 14:44:29 2024

@author: oyalu
"""
import pandas as pd
import glob as glob
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.txt'
target_file = 'transformed_data.csv'


def extract():
    headers  = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    car_data = headers.copy()
    for df_csv,df_json, in zip(glob.glob("*.csv"),glob.glob('*.json')):
        car_data = pd.concat([car_data,pd.read_csv(df_csv)], ignore_index=True)
        car_data = pd.concat([car_data,pd.read_json(df_json, lines=True)],ignore_index=True)
    

# headers  = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    for xml_file in glob.glob("*.xml"):
        # print(xml_file)
        tree = ET.parse(xml_file) 
        root = tree.getroot()
        for car in root: 
            car_model = car.find("car_model").text
            year =  car.find('year_of_manufacture').text
            price = float(car.find('price').text)
            fuel = car.find('fuel').text
            car_data = pd.concat([car_data, pd.DataFrame([{"car_model":car_model, 
                                                              "year_of_manufacture":year, 
                                                              "price":price,
                                                              "fuel":fuel}])], ignore_index=True)
    return car_data


def transform(data):
    transformed_data = data.copy()
    transformed_data = round(data['price'],2)
    return transformed_data


def load_data(data,target_file):
    return data.to_csv(target_file,index=False)

def log_progress(message):
    time_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(time_format)
    
    with open(log_file, 'a') as f:
        f.write(timestamp + ' ' + message + '\n')


log_progress('ETL started')

log_progress('extract started')
extracted_data = extract()
log_progress('extract ended')

log_progress('transform started')
transformed_data = transform(extracted_data)
log_progress('transformed ended')

log_progress('load started')
load_data(transformed_data,target_file)
log_progress('loaded ended')

log_progress('ETL ended')