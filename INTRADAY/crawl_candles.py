import requests
import json
# pMiGxCsYmY4lmqL2R5KmyMj0maL9tGGq
date = "2024-05-09"
apiKey = "pMiGxCsYmY4lmqL2R5KmyMj0maL9tGGq"

# adjusted = "true"
adjusted = "true"
url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted={adjusted}&apiKey={apiKey}'
r = requests.get(url)
data = r.json()
# Serializing json
json_object = json.dumps(data, indent=4)
# Writing to sample.json
with open("C:\Certificate\Project\INTRADAY\crawl_adjust.json", "w") as outfile:
    outfile.write(json_object)

# adjusted = "false"
adjusted = "false"
url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted={adjusted}&apiKey={apiKey}'
r = requests.get(url)
data = r.json()
# Serializing json
json_object = json.dumps(data, indent=4)
# Writing to sample.json
with open("C:\Certificate\Project\INTRADAY\crawl_astrade.json", "w") as outfile:
    outfile.write(json_object)