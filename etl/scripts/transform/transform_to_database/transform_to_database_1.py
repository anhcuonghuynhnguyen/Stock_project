import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text

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
with open(r'etl\data\raw\companies\crawl_companies_2024-05-31.json', 'r') as file:
    companies = json.load(file)

with open(r'etl\data\raw\markets\crawl_markets_2024-05-31.json', 'r') as file:
    markets = json.load(file)

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
# regions.to_sql("regions", engine, if_exists='append', index=False)
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

# industries.to_sql("industries", engine, if_exists='append', index=False)
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

# sicindustries.to_sql("sicindustries", engine, if_exists='append', index=False)
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