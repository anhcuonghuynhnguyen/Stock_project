import pandas as pd
from sqlalchemy import create_engine, text
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

# Exchanges = pd.read_sql_query("SELECT * FROM Exchanges", engine)
# print(Exchanges)
sql = "Select * from Exchanges;"

# Thực thi câu lệnh tạo bảng
with engine.connect() as conn:
    conn.execute(text(sql))