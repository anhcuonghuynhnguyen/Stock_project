import os
import json
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine

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
    # Database connection parameters
    DATABASE_TYPE = 'postgresql'
    ENDPOINT = 'localhost'
    USER = 'anhcu'
    PASSWORD = 'admin'
    PORT = 5432
    DATABASE = 'datasource'

    # Create database engine
    engine = create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

    # Read the latest companies JSON file
    companies_json = read_latest_file_in_directory('/home/anhcu/Project/Stock_project/backend/data/raw/companies')
    date = datetime.date.today().strftime("%Y_%m_%d")

    # Create a DataFrame for companies
    companies = cleaned_dataframe(pd.DataFrame([
        {
            "company_exchange": item["exchange"], 
            "company_industry": item["industry"], 
            "company_sector": item["sector"], 
            "company_sic_id": item["sic"], 
            "company_name": item["name"], 
            "company_ticket": item["ticker"], 
            "company_is_delisted": item["isDelisted"], 
            "company_category": item["category"], 
            "company_currency": item["currency"], 
            "company_location": item["location"]
        }
        for item in companies_json
    ]))

    # Filter companies DataFrame for specific exchanges and currency
    companies = companies[companies["company_exchange"].isin(["NYSE", "NASDAQ"])]
    companies = companies[companies["company_currency"] == "USD"]

    # Read data from exchanges and industries tables into DataFrames
    exchanges = pd.read_sql_query("SELECT * FROM exchanges", engine)
    industries = pd.read_sql_query("SELECT * FROM industries", engine)

    # Merge companies DataFrame with exchanges
    companies = pd.merge(
        companies, 
        exchanges, 
        left_on="company_exchange", 
        right_on="exchange_name"
    )[
        [
            "exchange_id", 
            "company_industry", 
            "company_sector", 
            "company_sic_id", 
            "company_name", 
            "company_ticket", 
            "company_is_delisted", 
            "company_category", 
            "company_currency", 
            "company_location"
        ]
    ]

    # Merge companies DataFrame with industries
    companies = pd.merge(
        companies, 
        industries, 
        left_on=["company_industry", "company_sector"], 
        right_on=["industry_name", "industry_sector"],
        how='left'
    )[
        [
            "exchange_id", 
            "industry_id", 
            "company_sic_id", 
            "company_name", 
            "company_ticket", 
            "company_is_delisted", 
            "company_category", 
            "company_currency", 
            "company_location"
        ]
    ]

    # Clean the final companies DataFrame
    companies = cleaned_dataframe(companies)

    # Rename columns for clarity
    new_columns = {'exchange_id': 'company_exchange_id', 'industry_id': 'company_industry_id'}
    companies.rename(columns=new_columns, inplace=True)

    # Save the final companies DataFrame to a JSON file
    path = f"/home/anhcu/Project/Stock_project/backend/data/processed/transformed_to_database_companies/process_companies_{date}.json"
    save_to_json(companies, path)
