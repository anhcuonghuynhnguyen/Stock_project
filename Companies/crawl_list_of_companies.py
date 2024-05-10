import requests
import json
# 915ea604126681297ae79e0ebcee606ebce85f25b20a6d694f27fe067dc7f926
exchanges = {
    "nasdaq": "C:\Certificate\Project\Companies\list_nasdaq.json",
    "nyse": "C:\Certificate\Project\Companies\list_nyse.json",
    "nysemkt": "C:\Certificate\Project\Companies\list_nysemkt.json",
    "bats": "C:\Certificate\Project\Companies\list_bats.json"
}
for e in exchanges:
    url = f'https://api.sec-api.io/mapping/exchange/{e}?token=915ea604126681297ae79e0ebcee606ebce85f25b20a6d694f27fe067dc7f926'
    r = requests.get(url)
    data = r.json()
    # Serializing json
    json_object = json.dumps(data, indent=4)
    # Writing to sample.json
    with open(exchanges[e], "w") as outfile:
        outfile.write(json_object)
    print(f"The process of extracting {len(data)} companies from the {e.upper()} stock exchange has been completed")
# Result
# The process of extracting 14721 companies from the NASDAQ stock exchange has been completed
# The process of extracting 13410 companies from the NYSE stock exchange has been completed
# The process of extracting 1539 companies from the NYSEMKT stock exchange has been completed
# The process of extracting 937 companies from the BATS stock exchange has been completed