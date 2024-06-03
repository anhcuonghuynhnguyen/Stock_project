import pandas as pd
import datetime
from sqlalchemy import create_engine

# Kết nối đến PostgreSQL
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

# Đọc truy vấn SQL từ file
def read_query_from_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Thực hiện truy vấn và lưu kết quả vào file Parquet
def query_to_parquet(query, conn, parquet_file_path):
    # Thực hiện truy vấn và lấy kết quả vào DataFrame
    df = pd.read_sql(query, conn)
    print(df.info())
    # Lưu DataFrame vào file Parquet
    df.to_parquet(parquet_file_path, engine='pyarrow')

# Đường dẫn đến file truy vấn SQL
query_file_path = r'etl\scripts\extract\extract_db_to_dl\extract_db_to_parquet.sql'

# Đường dẫn đến file Parquet đầu ra
date = datetime.date.today().strftime("%Y_%m_%d")
parquet_file_path = r'etl\data\completed\load_db_to_dl\load_db_to_dl_' + f"{date}.parquet"

# Đọc truy vấn SQL từ file
query = read_query_from_file(query_file_path)

# Thực hiện truy vấn và lưu kết quả vào file Parquet
query_to_parquet(query, engine, parquet_file_path)
