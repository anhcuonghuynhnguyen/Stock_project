import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def get_latest_file_in_directory(directory, extension):
    # Lấy danh sách các file trong thư mục với phần mở rộng cụ thể
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    
    # Nếu không có file nào trong thư mục, trả về None
    if not files:
        return None
    
    # Tìm file mới nhất dựa trên thời gian chỉnh sửa
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def load_json_from_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def save_json_to_parquet(data, output_filepath):
    # Chuyển đổi dữ liệu JSON thành pyarrow Table
    table = pa.Table.from_pandas(pd.DataFrame(data))
    
    # Lưu pyarrow Table thành file Parquet
    pq.write_table(table, output_filepath)

def load_db_to_dl(input_directory, output_directory):
    extension = '.json'

    # Lấy file JSON mới nhất trong thư mục
    latest_file = get_latest_file_in_directory(input_directory, extension)

    if latest_file:
        # Đọc file JSON
        data = load_json_from_file(latest_file)
        print(f"Đã đọc file: {latest_file}")
        # Đặt tên file Parquet tương ứng với tên file JSON
        # Đường dẫn đến file Parquet đầu ra
        # date = datetime.now().strftime("%Y_%m_%d")
        filename = os.path.basename(latest_file).replace('.json', ".parquet")
        output_filepath = os.path.join(output_directory, filename)
        
        # Lưu JSON thành file Parquet
        save_json_to_parquet(data, output_filepath)
        print(f"Đã lưu file Parquet: {output_filepath}")
    else:
        print("Không tìm thấy file JSON nào trong thư mục")

# Chuyển đổi cho News
# Đường dẫn đến thư mục chứa các file JSON
input_directory = r'/home/anhcu/Project/Stock_project/elt/data/raw/news'
# Đường dẫn đến thư mục lưu file Parquet
output_directory = r'/home/anhcu/Project/Stock_project/elt/data/completed/load_api_news_to_dl'
load_db_to_dl(input_directory, output_directory)

# Chuyển đổi cho OHLCs
# Đường dẫn đến thư mục chứa các file JSON
input_directory = r'/home/anhcu/Project/Stock_project/elt/data/raw/ohlcs'
# Đường dẫn đến thư mục lưu file Parquet
output_directory = r'/home/anhcu/Project/Stock_project/elt/data/completed/load_api_ohlcs_to_dl'
load_db_to_dl(input_directory, output_directory)