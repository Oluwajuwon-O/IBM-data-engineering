# -*- coding: utf-8 -*-
"""
Created on Sun Feb 18 14:44:29 2024

@author: oyalu
"""
import pandas as pd
import xml.etree.ElementTree as ET

log_file = 'log_file.txt'
target_file = 'transformed_data.csv'

def extract_csv(file):
    df = pd.read_csv(file)
    return df

def extract_json(file):
    df = pd.read_json(file, lines= True)
    return df

def extract_xml(file):
    df = pd.DataFrame(columns=[''])
    
def extract()