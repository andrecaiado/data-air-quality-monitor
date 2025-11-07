import os
import time
import uuid
import json
import requests
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pyspark.sql.functions import lit
from pyspark.sql.types import *

# Load environment variables from .env file
load_dotenv()

# --------------------------------------
# Spark setup
# --------------------------------------
spark = SparkSession.builder.appName("OpenAQ_v3_Ingestion").getOrCreate()

# --------------------------------------
# Configurations
# --------------------------------------
DATABASE = "airq"
BRONZE_TABLE = f"{DATABASE}.bronze_openaq_raw"
OPENAQ_BASE = "https://api.openaq.org/v3"
HOURS_BACK = 10  # Fetch last 10 hours of data
PAGE_LIMIT = 1000  # API pagination size
BBOX_PT = "-9.6,36.8,-6.0,42.2"  # roughly Portugal
HEADERS = {'x-api-key': os.getenv("OPENAQ_API_KEY", "")}

# --------------------------------------
# Create database & bronze table if missing
# --------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Define bronze table schema
BRONZE_SCHEMA = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("parameter", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("date_from", StringType(), True),
    StructField("date_to", StringType(), True),
    StructField("rows_fetched", IntegerType(), True),
    StructField("api_payload", StringType(), True)
])

if not spark.catalog.tableExists(BRONZE_TABLE):
    spark.createDataFrame([], BRONZE_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
    print(f"✅ Created empty Delta table: {BRONZE_TABLE}")

# --------------------------------------
# Define ingestion window
# --------------------------------------
date_to = datetime.now(timezone.utc)
date_from = date_to - timedelta(hours=HOURS_BACK)

print(f"📅 Fetching data from {date_from.isoformat()} to {date_to.isoformat()}")

# --------------------------------------
# Helper: Fetch paginated data
# --------------------------------------
def fetch_measurements(sensor_id, start, end):
    """Fetches all measurements for a given sensor within a time window."""
    page = 1
    results = []
    end_formatted = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_formatted = start.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        url = (
            f"{OPENAQ_BASE}/sensors/{sensor_id}/hours"
            f"?limit={PAGE_LIMIT}&page={page}"
            f"&datetime_from={start_formatted}&datetime_to={end_formatted}"
        )
        headers = HEADERS
        time.sleep(1.05)  # be nice to the API
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            print(f"⚠️ Failed for sensor {sensor_id}: {r.status_code}")
            break
        else:
            elapsed = r.elapsed.total_seconds()
            print(f"⏱️ Fetched page {page} for sensor {sensor_id} in {elapsed:.2f}s")

        payload = r.json()
        data = payload.get("results", [])
        if not data:
            break

        results.extend(data)
        if len(data) < PAGE_LIMIT:
            break  # no more pages

        page += 1

    return results

def fetch_locations(bbox):
    """Fetches all locations in a given bounding box."""
    page = 1
    results = []

    while True:
        url = (
            f"{OPENAQ_BASE}/locations"
            f"?bbox={bbox}&limit={PAGE_LIMIT}&page={page}"
        )
        headers = HEADERS
        time.sleep(1.05)  # be nice to the API
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            print(f"⚠️ Failed to fetch locations for bbox {bbox}: {r.status_code}")
            break

        payload = r.json()
        data = payload.get("results", [])
        if not data:
            break

        results.extend(data)
        if len(data) < PAGE_LIMIT:
            break  # no more pages

        page += 1

    print(f"🔍 Fetched {len(results)} locations for bbox {bbox}")
    return results

# --------------------------------------
# Step 1: Fetch locations by bounding box
# --------------------------------------
locations = fetch_locations(BBOX_PT)

# --------------------------------------
# Step 2: Filter locations by country code PT
# --------------------------------------
locations_pt = [loc for loc in locations if loc.get("country", {}).get("code") == "PT"]
print(f"🔍 Found {len(locations_pt)} locations in Portugal")

# --------------------------------------
# Step 3: Fetch sensors list from locations
# -------------------------------------- 
sensors = [sensor for location in locations for sensor in location.get("sensors", [])]
print(f"🔍 Retrieved {len(sensors)} sensors to ingest.")

# --------------------------------------
# Step 1: Fetch sensors list
# --------------------------------------
# sensors_url = f"{OPENAQ_BASE}/sensors?limit=50&page=1"  # adjust limit/pages if needed
# r = requests.get(sensors_url)
# sensors = r.json().get("results", [])

# print(f"🔍 Retrieved {len(sensors)} sensors to ingest.")

# --------------------------------------
# Step 4: Ingest per sensor
# --------------------------------------
batch_id = str(uuid.uuid4())
ingestion_time = datetime.now(timezone.utc)
read_sensors = 0

total_rows = 0
for sensor in sensors:
    if read_sensors >= 5:
        break  # limit to first 5 sensors for testing

    sensor_id = sensor["id"]
    location_id = sensor.get("locationId")
    parameter = sensor.get("parameter", {}).get("name") if isinstance(sensor.get("parameter"), dict) else sensor.get("parameter")

    measurements = fetch_measurements(sensor_id, date_from, date_to)
    read_sensors += 1
    if not measurements:
        continue

    rows_fetched = len(measurements)
    print(f"➡️ Ingesting {rows_fetched} measurements for sensor {sensor_id} (location {location_id}, parameter {parameter})")

    # Create single row with all measurements as JSON array
    row = (
        sensor_id,
        location_id,
        parameter,
        batch_id,
        ingestion_time.isoformat(),
        date_from.isoformat(),
        date_to.isoformat(),
        rows_fetched,
        json.dumps(measurements)  # All measurements as JSON array
    )
    
    df = spark.createDataFrame([row], BRONZE_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    total_rows += 1

print(f"✅ Ingestion complete — {total_rows} sensor batches written to {BRONZE_TABLE}")

# Stop Spark session to avoid Python 3.13 threading cleanup warnings
spark.stop()