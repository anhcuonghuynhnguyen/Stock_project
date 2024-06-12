import os
import json
import pandas as pd
import numpy as np
import datetime

def get_latest_file_in_directory(directory, extension):
    """
    Get the latest file in a directory with a specific extension.
    
    :param directory: Directory to search for files.
    :param extension: File extension to look for.
    :return: Path to the latest file or None if no files are found.
    """
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    """
    Read the latest JSON file in a directory.
    
    :param directory: Directory to search for files.
    :return: Data from the JSON file or an empty list if no file is found.
    """
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
    """
    Clean a DataFrame by replacing empty strings with NaN, dropping duplicates, and dropping rows with NaN.
    
    :param dataframe: The DataFrame to clean.
    :return: Cleaned DataFrame.
    """
    return dataframe.replace(r'^\s*$', np.nan, regex=True).drop_duplicates().dropna()

def save_to_json(dataframe, filename):
    """
    Save a DataFrame to a JSON file.
    
    :param dataframe: The DataFrame to save.
    :param filename: The path to the JSON file.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    dataframe.to_json(filename, orient='records', lines=True)
    print(f"Saved dataframe to {filename}")

if __name__ == "__main__":
    # Read latest companies and markets data
    companies = read_latest_file_in_directory('/home/anhcu/Project/Stock_project/backend/data/raw/companies')
    markets = read_latest_file_in_directory('/home/anhcu/Project/Stock_project/backend/data/raw/markets')
    
    # Get the current date for filenames
    date = datetime.date.today().strftime("%Y_%m_%d")

    # Transform and save regions data
    regions = cleaned_dataframe(pd.DataFrame([
        {
            "region_name": item["region"],
            "region_local_open": item["local_open"],
            "region_local_close": item["local_close"]
        }
        for item in markets
    ]))
    regions_path = f"/home/anhcu/Project/Stock_project/backend/data/processed/transformed_to_database_regions/process_regions_{date}.json"
    save_to_json(regions, regions_path)

    # Transform and save industries data
    industries = cleaned_dataframe(pd.DataFrame([
        {
            "industry_name": item["industry"],
            "industry_sector": item["sector"]
        }
        for item in companies
    ]))
    industries_path = f"/home/anhcu/Project/Stock_project/backend/data/processed/transformed_to_database_industries/process_industries_{date}.json"
    save_to_json(industries, industries_path)

    # Transform and save SIC industries data
    sicindustries = cleaned_dataframe(pd.DataFrame([
        {
            "sic_id": item["sic"],
            "sic_industry": item["sicIndustry"],
            "sic_sector": item["sicSector"]
        }
        for item in companies
    ]))
    sicindustries_path = f"/home/anhcu/Project/Stock_project/backend/data/processed/transformed_to_database_sicindustries/process_sicindustries_{date}.json"
    save_to_json(sicindustries, sicindustries_path)