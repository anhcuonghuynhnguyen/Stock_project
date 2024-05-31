import requests
import json
import datetime

# Lấy OHLCs của ngày hôm qua
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# pMiGxCsYmY4lmqL2R5KmyMj0maL9tGGq
apiKey = "pMiGxCsYmY4lmqL2R5KmyMj0maL9tGGq"
# adjusted = "true"
adjusted = "true"
include_otc = "true"

url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted={adjusted}&include_otc={include_otc}&apiKey={apiKey}'
r = requests.get(url)
data = r.json()["results"]
# Serializing json
json_object = json.dumps(data, indent=4)
# Writing to sample.json
path = r"etl\data\raw\OHLCs\crawl_OHLCs_" + f"{date}.json"
with open(path, "w") as outfile:
    outfile.write(json_object)

print("The process of crawling OHLCs was successful")