import requests
import json
import datetime

# Lấy OHLCs của ngày hôm qua
date_crawl = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# pMiGxCsYmY4lmqL2R5KmyMj0maL9tGGq
apiKey = "pMiGxCsYmY4lmqL2R5KmyMj0maL9tGGq"
# adjusted = "true"
adjusted = "true"
include_otc = "true"

url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_crawl}?adjusted={adjusted}&include_otc={include_otc}&apiKey={apiKey}'
r = requests.get(url)
data = r.json()
data = data.get("results", [])
# Serializing json
json_object = json.dumps(data, indent=4)
# Writing to sample.json
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")
path = r"etl/data/raw/crawl_apis/OHLCs/crawl_OHLCs_" + f"{date}.json"
with open(path, "w") as outfile:
    outfile.write(json_object)

print(f"The process of crawling {len(data)} OHLCs was successful")
print(f"Saving at {path}")