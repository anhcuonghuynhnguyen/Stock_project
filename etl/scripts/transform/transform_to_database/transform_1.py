import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Hàm làm sạch dataframe trước khi đưa vào database
def cleaned_dataframe(dataframe):
    return dataframe.replace(r'^\s*$', np.nan, regex=True).drop_duplicates().dropna()

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

# Hoặc đọc dữ liệu từ truy vấn SQL vào DataFrame
df = pd.read_sql_query("SELECT * FROM companies", engine)

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
        "region_local_close": item["local_close"],
    }
    for item in markets
]))

industries = cleaned_dataframe(pd.DataFrame([
    {
        "industry_name": item["industry"], 
        "industry_sector": item["sector"]
    }
    for item in companies
]))

sicIndustries = cleaned_dataframe(pd.DataFrame([
    {
        "sic_id": item["sic"], 
        "sic_industry": item["sicIndustry"],
        "sic_sector": item["sicSector"],
    }
    for item in companies
]))

# Chèn dữ liệu từ DataFrame vào bảng, bỏ qua cột 'id'
regions.to_sql("regions", engine, if_exists='append', index=False)
industries.to_sql("industries", engine, if_exists='append', index=False)
sicIndustries.to_sql("sicindustries", engine, if_exists='append', index=False)