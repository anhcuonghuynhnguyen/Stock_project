import requests
import json
import datetime

# API token for SEC API
API_TOKEN = "915ea604126681297ae79e0ebcee606ebce85f25b20a6d694f27fe067dc7f926"

# List of stock exchanges to extract data from
exchanges = ["nasdaq", "nyse"]

# Initialize an empty list to hold company data
list_companies = []

# Iterate over each exchange and fetch company data
for exchange in exchanges:
    url = f'https://api.sec-api.io/mapping/exchange/{exchange}?token={API_TOKEN}'
    response = requests.get(url)
    data = response.json()
    list_companies.extend(data)
    print(f"Extracted {len(data)} companies from the {exchange.upper()} stock exchange.")

# Get the current date for filename
date = datetime.date.today().strftime("%Y_%m_%d")
path = f"/home/anhcu/Project/Stock_project/backend/data/raw/companies/crawl_companies_{date}.json"

# Serialize the list of companies to JSON
json_object = json.dumps(list_companies, indent=4)

# Write the JSON data to a file
with open(path, "w") as outfile:
    outfile.write(json_object)
print(f"Data saved to {path}")