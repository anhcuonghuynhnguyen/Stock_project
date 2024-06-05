import os
import json
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine

def get_latest_file_in_directory(directory, extension):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    extension = '.json'
    latest_file = get_latest_file_in_directory(directory, extension)
    if latest_file:
        with open(latest_file, 'r') as file:
            data_json = json.load(file)
        print(f"Transforming from file: {latest_file}")
    else:
        print("No file found")
        data_json = []
    return data_json

def cleaned_dataframe(dataframe):
    return dataframe.replace(r'^\s*$', np.nan, regex=True).drop_duplicates().dropna()

def save_to_json(dataframe, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    dataframe.to_json(filename, orient='records', lines=True)
    print(f"Saved dataframe to {filename}")

if __name__ == "__main__":
    markets = read_latest_file_in_directory('etl/data/raw/crawl_apis/markets')
    date = datetime.date.today().strftime("%Y_%m_%d")

    exchanges = cleaned_dataframe(pd.DataFrame([
        {
            "region": item["region"],
            "primary_exchanges": item["primary_exchanges"]
        }
        for item in markets
    ]))

    exchanges = exchanges.assign(
        primary_exchanges=exchanges['primary_exchanges'].str.split(', ')
    )
    exchanges = exchanges.explode('primary_exchanges').reset_index(drop=True)

    username = 'anhcu'
    password = 'admin'
    host = 'localhost'
    port = '5432'
    database = 'datasource'

    connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    query = "SELECT * FROM regions"
    regions = pd.read_sql(query, engine)

    exchanges = pd.merge(
        exchanges, 
        regions, 
        left_on="region", 
        right_on="region_name"
    )[
        ["region_id", "primary_exchanges"]
    ]

    new_columns = {'region_id': 'exchange_region_id', 'primary_exchanges': 'exchange_name'}
    exchanges.rename(columns=new_columns, inplace=True)

    path = r"etl/data/processed/transformed_to_database_exchanges/process_exchanges_" + f"{date}.json"
    save_to_json(exchanges, path)