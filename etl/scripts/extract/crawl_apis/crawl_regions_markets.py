import requests
import json
import datetime

function = "MARKET_STATUS"
# 8VRGCBIF91MIK8OE
apiKey = "8VRGCBIF91MIK8OE"

url = f'https://www.alphavantage.co/query?function={function}&apikey={apiKey}'
r = requests.get(url)
data = r.json()["markets"]
# Serializing json
json_object = json.dumps(data, indent=4)

# Lấy date của ngày thực hiện truy xuất dữ liệu
date = datetime.date.today().strftime("%Y_%m_%d")
path = r"etl\data\raw\markets\crawl_markets_" + f"{date}.json"
# Writing
with open(path, "w") as outfile:
    outfile.write(json_object)

print(f"The process of extracting {len(data)} regions and exchanges has been completed")
