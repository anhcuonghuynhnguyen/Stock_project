import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
import os

def get_latest_file_in_directory(directory, extension):
    # Lấy danh sách các file trong thư mục với phần mở rộng cụ thể
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    # Nếu không có file nào trong thư mục, trả về None
    if not files:
        return None
    # Tìm file mới nhất dựa trên thời gian chỉnh sửa
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    # Đường dẫn đến thư mục chứa các file JSON
    extension = '.json'
    # Lấy file mới nhất trong thư mục
    latest_file = get_latest_file_in_directory(directory, extension)
    if latest_file:
        # Đọc file JSON
        with open(latest_file, 'r') as file:
            companies_json = json.load(file)
        print(f"Transforming from file: {latest_file}")
    else:
        print("No founding file")
    return companies_json

# Hàm làm sạch dataframe trước khi đưa vào database
def cleaned_dataframe(dataframe):
    return dataframe\
        .drop_duplicates()\
        .replace({np.nan: None, '': None})

# Kết nói database
# Thông tin kết nối đến PostgreSQL
DATABASE_TYPE = 'postgresql'
ENDPOINT = 'localhost'  # Địa chỉ của PostgreSQL
USER = 'anhcu'  # Tên đăng nhập PostgreSQL
PASSWORD = '225720074'  # Mật khẩu PostgreSQL
PORT = 5432  # Cổng mặc định của PostgreSQL
DATABASE = 'datasource'  # Tên cơ sở dữ liệu

# Tạo engine kết nối đến PostgreSQL
engine = create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

# Đọc file JSON
companies_json = read_latest_file_in_directory(r'etl\data\raw\crawl_apis\companies')

# Tạo DataFrame cho bảng Exchanges
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
print(companies)

companies = companies[companies["company_exchange"].isin((["NYSE", "NASDAQ"]))]
companies = companies[companies["company_currency"] == "USD"]

# Đọc dữ liệu từ bảng exchanges và industries vào DataFrame
exchanges = pd.read_sql_query("SELECT * FROM exchanges", engine)
industries = pd.read_sql_query("SELECT * FROM industries", engine)

# Kết hợp 2 df companies và exchanges để lấy id của exchange gán cho các dòng df companies
companies = pd.merge(
    # Hai bảng muốn kết hợp
    companies, 
    exchanges, 
    # Điều kiện kết hợp
    left_on="company_exchange", 
    right_on="exchange_name"
)[
    # Chỉ lấy các cột cần thiết
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

# Kết hợp 2 df companies và industries để lấy id của industriy gán cho các dòng df companies
companies = pd.merge(
    # Hai bảng muốn kết hợp
    companies, 
    industries, 
    # Điều kiện kết hợp
    left_on=["company_industry", "company_sector"], 
    right_on=["industry_name", "industry_sector"],
    how='left'
)[
    # Chỉ lấy các cột cần thiết
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

companies = cleaned_dataframe(companies)
print(companies)
table_name = "companies"
# Mở kết nối
with engine.connect() as conn:
    for index, row in companies.iterrows():
        # Tạo câu lệnh INSERT
        insert_query = text(f"""
            INSERT INTO {table_name} (company_exchange_id, company_industry_id, company_sic_id, company_name, company_ticket, company_is_delisted, company_category, company_currency, company_location)
            VALUES (:exchange_id, :industry_id, :company_sic_id, :company_name, :company_ticket, :company_is_delisted, :company_category, :company_currency, :company_location)
            ON CONFLICT (company_ticket, company_is_delisted) DO NOTHING
        """)
        # Thực thi câu lệnh INSERT với tham số từ DataFrame
        conn.execute(insert_query, 
                     exchange_id=row['exchange_id'], 
                     industry_id=row['industry_id'], 
                     company_sic_id=row['company_sic_id'], 
                     company_name=row['company_name'], 
                     company_ticket=row['company_ticket'], 
                     company_is_delisted=row['company_is_delisted'], 
                     company_category=row['company_category'], 
                     company_currency=row['company_currency'], 
                     company_location=row['company_location']
                    )