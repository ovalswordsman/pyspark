import requests 
import pandas as pd
import json
import csv
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

def get_data():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": "8e26c15ff2mshe0e03bd340a6194p10fa52jsnfb48dc150447",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)
    data = response.json()
    data.popitem()
    data.popitem()
    return data

def create_csv(data):
    with open('output.csv', mode='w', newline='') as file:
        
        # create a CSV writer
        writer = csv.writer(file)
        
        # write the header row
        writer.writerow(['slno', 'state', 'confirm','cured','death','total'])
        
        # write the data rows
        for state_data in data.values():
            writer.writerow([state_data['slno'], state_data['state'], state_data['confirm'],state_data['cured'],state_data['death'],state_data['total']])

#data = get_data()
#create_csv(data)

