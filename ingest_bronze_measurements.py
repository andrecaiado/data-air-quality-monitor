import os
import time
import uuid
import json
import requests
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pyspark.sql.types import *
from pyspark.sql import functions as F

# Load environment variables from .env file
load_dotenv()

# --------------------------------------
# Spark setup
# --------------------------------------
spark = SparkSession.builder.appName("Ingest_Bronze_Measurements").getOrCreate()

# --------------------------------------
# Configurations
# --------------------------------------
DATABASE = os.getenv("DATABASE", "airq")
BRONZE_TABLE_MEASUREMENTS = f"{DATABASE}.bronze_measurements_batches"
DIM_TABLE_SENSORS = f"{DATABASE}.dim_sensors"
OPENAQ_API_BASE_URL = os.getenv("OPENAQ_API_V3_BASE_URL", "https://api.openaq.org/v3")
HOURS_BACK = 4  # Fetch last 4 hours of data
PAGE_LIMIT = 1000  # API pagination size
BBOX_PT = "-9.6,36.8,-6.0,42.2"  # roughly Portugal
HEADERS = {'x-api-key': os.getenv("OPENAQ_API_KEY", "")}

# --------------------------------------
# Create database & bronze table if missing
# --------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Define bronze table schema
SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("parameter_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("date_from", StringType(), True),
    StructField("date_to", StringType(), True),
    StructField("rows_fetched", IntegerType(), True),
    StructField("api_payload", StringType(), True)
])

if not spark.catalog.tableExists(BRONZE_TABLE_MEASUREMENTS):
    spark.createDataFrame([], SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS).write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE_MEASUREMENTS)
    print(f"✅ Created empty Delta table: {BRONZE_TABLE_MEASUREMENTS}")

# --------------------------------------
# Function: Fetch last ingestion date_to
# --------------------------------------
def get_last_ingestion_date_to():
    """Fetches the last ingestion date_to from the bronze table."""
    row = (spark.table(BRONZE_TABLE_MEASUREMENTS)
                 .agg(F.max("date_to").alias("last_date_to"))
                 .collect()[0])
    val = row["last_date_to"]

    return datetime.fromisoformat(val) if val else None

# --------------------------------------
# Define ingestion window
# --------------------------------------
date_to = datetime.now(timezone.utc)
last_ingestion_date_to = get_last_ingestion_date_to()
date_from = last_ingestion_date_to if last_ingestion_date_to is not None else date_to - timedelta(hours=HOURS_BACK)

print(f"📅 Fetching data from {date_from.isoformat()} to {date_to.isoformat()}")

# --------------------------------------
# Function: Fetch measurements
# --------------------------------------
def fetch_measurements(sensor_id, start, end):
    """Fetches all measurements for a given sensor within a time window."""
    page = 1
    results = []
    end_formatted = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_formatted = start.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        url = f"{OPENAQ_API_BASE_URL}/sensors/{sensor_id}/measurements"
        headers = HEADERS
        params = {
            "limit": PAGE_LIMIT,
            "page": page,
            "datetime_from": start_formatted,
            "datetime_to": end_formatted
        }

        time.sleep(1.05)  # # added delay to be nice to the API and avoid rate limits

        r = requests.get(url, headers=headers, params=params)
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

# --------------------------------------
# Function: Fetch sensors
# --------------------------------------
def fetch_sensors():
    """Fetches all sensors from the sensors bronze table."""
    return (spark.table(DIM_TABLE_SENSORS)
                 .select("sensor_id", "location_id", "parameter_id")
                 .collect())

# --------------------------------------
# Step 1: Fetch sensors from dimension table
# --------------------------------------
sensors = fetch_sensors()

# --------------------------------------
# Step 2: Ingest measurements for each sensor into bronze table
# --------------------------------------
batch_id = str(uuid.uuid4())
ingestion_time = datetime.now(timezone.utc)

total_rows = 0
total_sensors = len(sensors)
for idx, sensor_row in enumerate(sensors, start=1):
    sensor_id = sensor_row.sensor_id
    location_id = sensor_row.location_id
    parameter = sensor_row.parameter_id

    print(f"\n🔍 Fetching data for sensor {sensor_id} ({idx} of {total_sensors})")
    measurements = fetch_measurements(sensor_id, date_from, date_to)

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
    
    df = spark.createDataFrame([row], SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS)
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE_MEASUREMENTS)
    total_rows += 1

print(f"✅ Ingestion complete — {total_rows} sensor batches written to {BRONZE_TABLE_MEASUREMENTS}")

# Stop Spark session to avoid Python 3.13 threading cleanup warnings
spark.stop()