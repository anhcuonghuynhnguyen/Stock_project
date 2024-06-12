import requests
import json
import datetime

# Define function and API key for Alpha Vantage API
FUNCTION = "MARKET_STATUS"
API_KEY = "8VRGCBIF91MIK8OE"

# Construct the API URL
url = f'https://www.alphavantage.co/query?function={FUNCTION}&apikey={API_KEY}'
response = requests.get(url)
data = response.json()["markets"]

# Serialize the market data to JSON
json_object = json.dumps(data, indent=4)

# Get the current date for filename
date = datetime.date.today().strftime("%Y_%m_%d")
path = f"/home/anhcu/Project/Stock_project/backend/data/raw/markets/crawl_markets_{date}.json"

# Write the JSON data to a file
with open(path, "w") as outfile:
    outfile.write(json_object)

# Print completion messages
print(f"Extracted {len(data)} regions and exchanges.")
print(f"Data saved to {path}")