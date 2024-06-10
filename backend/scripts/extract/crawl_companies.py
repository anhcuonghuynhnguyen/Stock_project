import requests
import json
import datetime

# 915ea604126681297ae79e0ebcee606ebce85f25b20a6d694f27fe067dc7f926
exchanges = ["nasdaq", "nyse"]

list_companies = []
for e in exchanges:
    url = f'https://api.sec-api.io/mapping/exchange/{e}?token=915ea604126681297ae79e0ebcee606ebce85f25b20a6d694f27fe067dc7f926'
    r = requests.get(url)
    data = r.json()
    list_companies += data
    print(f"The process of extracting {len(data)} companies from the {e.upper()} stock exchange has been completed")

# Lấy date của ngày thực hiện truy xuất dữ liệu
date = datetime.date.today().strftime("%Y_%m_%d")
path = r"/home/anhcu/Project/Stock_project/backend/data/raw/companies/crawl_companies_" + f"{date}.json"
# Serializing json
json_object = json.dumps(list_companies, indent=4)
# Writing to sample.json
with open(path, "w") as outfile:
    outfile.write(json_object)
print(f"Saving at {path}")