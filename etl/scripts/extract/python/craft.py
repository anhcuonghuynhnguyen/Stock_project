import datetime

def get_data_by_time_range():
    """Lấy dữ liệu dựa trên khung giờ hiện tại."""
    now = datetime.datetime.now()

    # Xác định khung giờ hiện tại
    if now.hour < 9.5:
        # 0h00 -> 9h30
        print("Lấy dữ liệu khung giờ 0h00 -> 9h30")
        # Lấy dữ liệu cho khung giờ 0h00 -> 9h30
        time_from = now.strftime("%Y%m%dT"+"0000")
        time_to = now.strftime("%Y%m%dT"+"0930")
    elif now.hour < 16:
        # 9h30 -> 16h
        print("Lấy dữ liệu khung giờ 9h30 -> 16h")
        # Lấy dữ liệu cho khung giờ 9h30 -> 16h
        time_from = now.strftime("%Y%m%dT"+"0930")
        time_to = now.strftime("%Y%m%dT"+"1600")
    else:
        # 16h -> 23h59
        print("Lấy dữ liệu khung giờ 16h -> 23h59")
        # Lấy dữ liệu cho khung giờ 16h -> 23h59
        time_from = now.strftime("%Y%m%dT"+"1600")
        time_to = now.strftime("%Y%m%dT"+"2359")
    return time_from, time_to

# Gọi hàm để lấy dữ liệu
time_from, time_to = get_data_by_time_range()
print(time_from, time_to)
