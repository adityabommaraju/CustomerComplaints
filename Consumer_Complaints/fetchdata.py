import requests
import json
import datetime as dt
import pandas as pd
import numpy as np

response = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/")
data = response.json()

date = dt.datetime.now()

date_string = str(date.date())
FILE_PATH = "C:/Users/029338502/Desktop/Data_Upload_Files/data_" + date_string + ".json"

with open(FILE_PATH, "w") as outfile:
    json.dump(data, outfile)

# df = pd.DataFrame()
# for i in range(len(data['hits']['hits'])):
#     df = df.append(pd.json_normalize(data['hits']['hits'][i]['_source']))

print(date.date())
