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

print(get_latest_file_in_directory(r'etl\data\raw\OHLCs', '.json'))