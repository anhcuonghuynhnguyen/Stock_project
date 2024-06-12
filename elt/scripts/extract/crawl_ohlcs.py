import requests
import json
import datetime

# Get the OHLCs of yesterday
date_crawl = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

# API key for authentication
apiKey = "pMiGxCsYmY4lmqL2R5KmyMj0maL9tGGq"

# Set parameters for the API request
adjusted = "true"
include_otc = "true"

# Construct the API URL with query parameters
url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_crawl}?adjusted={adjusted}&include_otc={include_otc}&apiKey={apiKey}'

# Make a GET request to the API
r = requests.get(url)

# Parse the response JSON
data = r.json()
data = data.get("results", [])

# Serialize the JSON object to a formatted string
json_object = json.dumps(data, indent=4)

# Get yesterday's date formatted as YYYY_MM_DD
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")

# Define the file path for saving the JSON data
path = r"/home/anhcu/Project/Stock_project/elt/data/raw/ohlcs/crawl_ohlcs_" + f"{date}.json"

# Write the JSON data to a file
with open(path, "w") as outfile:
    outfile.write(json_object)

# Print success message with total OHLCs and file path
print(f"The process of crawling {len(data)} OHLCs was successful")
print(f"Saving at {path}")