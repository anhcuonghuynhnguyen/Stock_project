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
with open(r'etl\data\raw\markets\crawl_markets_2024-05-31.json', 'r') as file:
    markets = json.load(file)

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