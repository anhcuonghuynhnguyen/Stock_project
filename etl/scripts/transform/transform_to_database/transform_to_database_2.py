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
markets = read_latest_file_in_directory(r'etl/data/raw/crawl_apis/markets')

# Tạo DataFrame cho bảng Exchanges
exchanges = cleaned_dataframe(pd.DataFrame([
    {
        "region": item["region"], 
        "primary_exchanges": item["primary_exchanges"]
    }
    for item in markets
]))

# Tách giá trị trong cột primary_exchanges thành một danh sách
exchanges = exchanges.assign(
    primary_exchanges=exchanges['primary_exchanges'].str.split(', ')
)
# Tạo các hàng riêng biệt cho mỗi giá trị trong danh sách
exchanges = exchanges\
    .explode('primary_exchanges')\
    .reset_index(drop=True)

# Đọc dữ liệu từ bảng Regions vào DataFrame
regions = pd.read_sql_query("SELECT * FROM Regions", engine)

# Kết hợp 2 df exchanges và regions để lấy id của region gán cho các dòng df exchanges
exchanges = pd.merge(
    # Hai bảng muốn kết hợp
    exchanges, 
    regions, 
    # Điều kiện kết hợp
    left_on="region", 
    right_on="region_name"
)[
    # Chỉ lấy 2 cột region_id và primary_exchanges
    ["region_id", "primary_exchanges"]
]
print(exchanges)
# Chèn dữ liệu từ DataFrame vào bảng, bỏ qua cột 'id'
# exchanges.to_sql("Exchanges", engine, if_exists='fail', index=False)

table_name = "exchanges"
# Mở kết nối
with engine.connect() as conn:
    for index, row in exchanges.iterrows():
        # Tạo câu lệnh INSERT
        insert_query = text(f"""
            INSERT INTO {table_name} (exchange_region_id, exchange_name)
            VALUES (:region_id, :primary_exchanges)
            ON CONFLICT (exchange_name) DO NOTHING
        """)
        # Thực thi câu lệnh INSERT với tham số từ DataFrame
        conn.execute(insert_query, region_id=row['region_id'], primary_exchanges=row['primary_exchanges'])