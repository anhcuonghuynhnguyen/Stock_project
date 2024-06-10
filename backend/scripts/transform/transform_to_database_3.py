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
    # Kết nối database
    DATABASE_TYPE = 'postgresql'
    ENDPOINT = 'localhost'
    USER = 'anhcu'
    PASSWORD = 'admin'
    PORT = 5432
    DATABASE = 'datasource'

    engine = create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

    # Đọc file JSON
    companies_json = read_latest_file_in_directory('/home/anhcu/Project/Stock_project/backend/data/raw/companies')
    date = datetime.date.today().strftime("%Y_%m_%d")

    # Tạo DataFrame cho bảng companies
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

    # Lọc dữ liệu companies
    companies = companies[companies["company_exchange"].isin(["NYSE", "NASDAQ"])]
    companies = companies[companies["company_currency"] == "USD"]

    # Đọc dữ liệu từ bảng exchanges và industries vào DataFrame
    exchanges = pd.read_sql_query("SELECT * FROM exchanges", engine)
    industries = pd.read_sql_query("SELECT * FROM industries", engine)

    # Kết hợp DataFrame companies và exchanges
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

    # Kết hợp DataFrame companies và industries
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

    # Làm sạch DataFrame companies
    companies = cleaned_dataframe(companies)

    new_columns = {'exchange_id': 'company_exchange_id', 'industry_id': 'company_industry_id'}
    companies.rename(columns=new_columns, inplace=True)

    # Lưu DataFrame vào file JSON
    path = r"/home/anhcu/Project/Stock_project/backend/data/processed/transformed_to_database_companies/process_companies_" + f"{date}.json"
    save_to_json(companies, path)