import requests
import json
# 8VRGCBIF91MIK8OE
url = 'https://www.alphavantage.co/query?function=MARKET_STATUS&apikey=8VRGCBIF91MIK8OE'
r = requests.get(url)
data = r.json()
# Serializing json
json_object = json.dumps(data, indent=4)
# Writing to sample.json
with open("C:\Certificate\Project\Regions_Markets\list_regions_markets.json", "w") as outfile:
    outfile.write(json_object)
print(f"The process of extracting {len(data['markets'])} regions and exchanges has been completed")
# The process of extracting 16 regions and exchanges has been completed