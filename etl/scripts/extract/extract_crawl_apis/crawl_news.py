import requests
import json
from datetime import datetime

function = "NEWS_SENTIMENT"

# Lấy thời gian hiện tại
# Định dạng chuỗi theo định dạng YYYYMMDDTHHMM
def get_data_by_time_range():
    """Lấy dữ liệu dựa trên khung giờ hiện tại."""
    now = datetime.now()

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

sort = "LATEST"
limit = "1000"
# 8VRGCBIF91MIK8OE
apikey = "8VRGCBIF91MIK8OE"

url = f'https://www.alphavantage.co/query?function={function}&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={apikey}'
r = requests.get(url)
data = r.json()["feed"]
# Serializing json
json_object = json.dumps(data, indent=4)
# Writing to sample.json
date = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
path = r"etl/data/raw/crawl_apis/news/crawl_news_" + f"{date}.json"
with open(path, "w") as outfile:
    outfile.write(json_object)

print(f"The process of crawling {len(data)} news was successful")
print(f"Saving at {path}")