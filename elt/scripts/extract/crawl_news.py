import requests
import json
import datetime

function = "NEWS_SENTIMENT"

# Lấy thời gian hiện tại
# Định dạng chuỗi theo định dạng YYYYMMDDTHHMM
def get_data_by_time_range(time_zone):
    """Lấy dữ liệu dựa trên khung giờ hiện tại."""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    if time_zone == 1:
        time_from = yesterday.strftime("%Y%m%dT"+"0000")
        time_to = yesterday.strftime("%Y%m%dT"+"0929")
    elif time_zone == 2:
        time_from = yesterday.strftime("%Y%m%dT"+"0930")
        time_to = yesterday.strftime("%Y%m%dT"+"1600")
    else:
        time_from = yesterday.strftime("%Y%m%dT"+"1601")
        time_to = yesterday.strftime("%Y%m%dT"+"2359")
    return time_from, time_to

sort = "LATEST"
limit = "1000"
# 8VRGCBIF91MIK8OE
apikey = "8VRGCBIF91MIK8OE"

json_object = []
total = 0
for time_zone in [1,2,3]:
    # Gọi hàm để lấy dữ liệu
    time_from, time_to = get_data_by_time_range(time_zone)
    print(time_from, time_to)

    url = f'https://www.alphavantage.co/query?function={function}&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={apikey}'
    r = requests.get(url)
    data = r.json()["feed"]
    # Serializing json
    # json_news = json.dumps(data, indent=4)
    json_object += data
    total += len(data)

json_object = json.dumps(json_object, indent=4)
# Writing to sample.json
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")
path = r"/home/anhcu/Project/Stock_project/elt/data/raw/news/crawl_news_" + f"{date}.json"
with open(path, "w") as outfile:
    outfile.write(json_object)

print(f"The process of crawling {total} news was successful")
print(f"Saving at {path}")