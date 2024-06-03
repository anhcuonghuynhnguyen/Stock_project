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
        .replace(r'^\s*$', np.nan, regex=True)\
        .drop_duplicates()\
        .dropna()

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
companies = read_latest_file_in_directory(r'etl\data\raw\companies')
markets = read_latest_file_in_directory(r'etl\data\raw\markets')

# Tạo DataFrame cho mỗi bảng
regions = cleaned_dataframe(pd.DataFrame([
    {
        "region_name": item["region"], 
        "region_local_open": item["local_open"], 
        "region_local_close": item["local_close"]
    }
    for item in markets
]))
print(regions)

industries = cleaned_dataframe(pd.DataFrame([
    {
        "industry_name": item["industry"], 
        "industry_sector": item["sector"]
    }
    for item in companies
]))
print(industries)

sicindustries = cleaned_dataframe(pd.DataFrame([
    {
        "sic_id": item["sic"], 
        "sic_industry": item["sicIndustry"],
        "sic_sector": item["sicSector"]
    }
    for item in companies
]))
print(sicindustries)

# Chèn dữ liệu từ DataFrame vào bảng, bỏ qua cột 'id'
# Mở kết nối
table_name = "regions"
with engine.connect() as conn:
    for index, row in regions.iterrows():
        # Tạo câu lệnh INSERT
        insert_query = text(f"""
            INSERT INTO {table_name} (region_name, region_local_open, region_local_close)
            VALUES (:region_name, :region_local_open, :region_local_close)
            ON CONFLICT (region_name) DO NOTHING
        """)
        # Thực thi câu lệnh INSERT với tham số từ DataFrame
        conn.execute(insert_query, region_name=row['region_name'], region_local_open=row['region_local_open'], region_local_close=row['region_local_close'])

# Mở kết nối
table_name = "industries"
with engine.connect() as conn:
    for index, row in industries.iterrows():
        # Tạo câu lệnh INSERT
        insert_query = text(f"""
            INSERT INTO {table_name} (industry_name, industry_sector)
            VALUES (:industry_name, :industry_sector)
            ON CONFLICT (industry_name, industry_sector) DO NOTHING
        """)
        # Thực thi câu lệnh INSERT với tham số từ DataFrame
        conn.execute(insert_query, industry_name=row['industry_name'], industry_sector=row['industry_sector'])

# Mở kết nối
table_name = "sicindustries"
with engine.connect() as conn:
    for index, row in sicindustries.iterrows():
        # Tạo câu lệnh INSERT
        insert_query = text(f"""
            INSERT INTO {table_name} (sic_id, sic_industry, sic_sector)
            VALUES (:sic_id, :sic_industry, :sic_sector)
            ON CONFLICT (sic_industry, sic_sector) DO NOTHING
        """)
        # Thực thi câu lệnh INSERT với tham số từ DataFrame
        conn.execute(insert_query, sic_id=row['sic_id'], sic_industry=row['sic_industry'], sic_sector=row['sic_sector'])