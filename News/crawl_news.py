import requests
import json
# 8VRGCBIF91MIK8OE
function = "NEWS_SENTIMENT"
# ticket = ""
# topic = ""
time_from = "20240101T0000"
time_to = "20240131T2359"
sort = "LATEST"
limit = "1000"
apikey = "8VRGCBIF91MIK8OE"

# adjusted = "true"
adjusted = "true"
url = f'https://www.alphavantage.co/query?function={function}&time_from={time_from}&time_to={time_to}&sort{sort}&limit={limit}&apikey={apikey}'
r = requests.get(url)
data = r.json()
# Serializing json
json_object = json.dumps(data, indent=4)
# Writing to sample.json
with open(r"C:\Certificate\Project\News\crawl_news.json", "w") as outfile:
    outfile.write(json_object)