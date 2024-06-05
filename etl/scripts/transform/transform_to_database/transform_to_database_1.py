import os
import json
import pandas as pd
import numpy as np
import datetime

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
    companies = read_latest_file_in_directory('etl/data/raw/crawl_apis/companies')
    markets = read_latest_file_in_directory('etl/data/raw/crawl_apis/markets')
    # Lấy date của ngày thực hiện truy xuất dữ liệu
    date = datetime.date.today().strftime("%Y_%m_%d")

    regions = cleaned_dataframe(pd.DataFrame([
        {
            "region_name": item["region"],
            "region_local_open": item["local_open"],
            "region_local_close": item["local_close"]
        }
        for item in markets
    ]))
    path = r"etl/data/processed/transformed_to_database_regions/process_regions_" + f"{date}.json"
    save_to_json(regions, path)

    industries = cleaned_dataframe(pd.DataFrame([
        {
            "industry_name": item["industry"],
            "industry_sector": item["sector"]
        }
        for item in companies
    ]))
    path = r"etl/data/processed/transformed_to_database_industries/process_industries_" + f"{date}.json"
    save_to_json(industries, path)

    sicindustries = cleaned_dataframe(pd.DataFrame([
        {
            "sic_id": item["sic"],
            "sic_industry": item["sicIndustry"],
            "sic_sector": item["sicSector"]
        }
        for item in companies
    ]))
    path = r"etl/data/processed/transformed_to_database_sicindustries/process_sicindustries_" + f"{date}.json"
    save_to_json(sicindustries, path)