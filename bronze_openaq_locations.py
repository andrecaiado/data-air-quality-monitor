# Load environment variables from .env file
import datetime
import os
import time
import uuid
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
from pyspark.sql.types import *

load_dotenv()

# --------------------------------------
# Spark setup
# --------------------------------------
spark = SparkSession.builder.appName("Bronze_OpenAQ_Locations").getOrCreate()

# --------------------------------------
# Configurations
# --------------------------------------
DATABASE = os.getenv("DATABASE", "airq")
BRONZE_TABLE = f"{DATABASE}.bronze_openaq_locations"
OPENAQ_BASE = os.getenv("OPENAQ_V3_BASE", "https://api.openaq.org/v3")
PAGE_LIMIT = 1000  # API pagination size
HEADERS = {'x-api-key': os.getenv("OPENAQ_API_KEY", "")}

# --------------------------------------
# Create database & bronze table if missing
# --------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Define bronze table schema
BRONZE_SCHEMA = StructType([
    StructField("batch_id", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("rows_fetched", IntegerType(), True),
    StructField("api_payload", StringType(), True)
])

if not spark.catalog.tableExists(BRONZE_TABLE):
    spark.createDataFrame([], BRONZE_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
    print(f"✅ Created empty Delta table: {BRONZE_TABLE}")

# --------------------------------------
# Helper: Fetch locations
# --------------------------------------
def fetch_locations():
    """Fetches all locations"""
    page = 1
    results = []

    while True:
        url = (
            f"{OPENAQ_BASE}/locations"
        )
        headers = HEADERS
        params = {
            "limit": PAGE_LIMIT,
            "page": page,
        }
        
        time.sleep(1.05)  # added delay to be nice to the API and avoid rate limits

        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            print(f"⚠️ Failed to fetch locations: {r.status_code}")
            break

        payload = r.json()
        data = payload.get("results", [])
        if not data:
            break

        results.extend(data)
        if len(data) < PAGE_LIMIT:
            break  # no more pages

        page += 1

    print(f"🔍 Fetched {len(results)} locations")
    return results

# --------------------------------------
# Step 1: Fetch locations
# --------------------------------------
locations = fetch_locations()

# --------------------------------------
# Step 2: Ingest locations into bronze table
# --------------------------------------
batch_id = str(uuid.uuid4())
ingestion_time = datetime.now(timezone.utc)
rows_fetched = len(locations)
row = (
            batch_id,
            ingestion_time.isoformat(),
            rows_fetched,
            json.dumps(locations)  # All locations as JSON array
        )
df = spark.createDataFrame([row], BRONZE_SCHEMA)
df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

print(f"✅ Ingestion complete — 1 batch of {rows_fetched} locations written to {BRONZE_TABLE}")

# Stop Spark session to avoid Python 3.13 threading cleanup warnings
spark.stop()