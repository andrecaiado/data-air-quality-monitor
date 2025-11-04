import os
import requests, time
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()
url = "https://api.openaq.org/v3/sensors/25372/hours"
headers = {"X-API-KEY": os.environ['OPENAQ_API_KEY']}
parameters = {
    #"datetime_from": "2024-10-01T00:00:00Z"
    "datetime_from": datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "datetime_to": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

all_records = []
page = 1
while True:
  response = requests.get(url, headers=headers, params={**parameters, "page": page, "limit": 100})
  if response.status_code != 200 or not response.json:
      break
  data = response.json()
  results = data.get("results", [])
  if not results:
      break
  all_records.extend(results)
  page += 1
  time.sleep(1)

if all_records:
    pdf = pd.json_normalize(all_records)
    spark_df = spark.createDataFrame(pdf)
    spark_df.show()
else:
    print("No results found in API response.")