import datetime
import os
import time
import uuid
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime, timezone
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils 

# --------------------------------------
# Spark setup and other initializations
# --------------------------------------
spark = SparkSession.builder.appName("Ingest_Bronze_Measurements").getOrCreate()
dbutils = DBUtils(spark)

# --------------------------------------
# Load environment variables and check requirements
# --------------------------------------
load_dotenv()

# Check if required environment variables are set
# Load env variables from .env file if it exists or from Databricks secrets
required_env_vars = ["OPENAQ_API_KEY"]
missing_vars = [var for var in required_env_vars if not os.getenv(var) and not dbutils.secrets.get(scope="data-air-quality-monitor", key=var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# --------------------------------------
# Set database & table names
# --------------------------------------
DATABASE = os.getenv("DATABASE", "airq")
BRONZE_TABLE_LOCATIONS = f"{DATABASE}.bronze_locations_snapshots"

# --------------------------------------
# Set values for API calls
# --------------------------------------
OPENAQ_API_BASE_URL = os.getenv("OPENAQ_API_V3_BASE_URL", "https://api.openaq.org/v3")
PAGE_LIMIT = 1000  # API pagination size
HEADERS = {'x-api-key': os.getenv("OPENAQ_API_KEY", "")}

# --------------------------------------
# Create database & bronze table if missing
# --------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Define bronze table schema
SCHEMA_BRONZE_TABLE_LOCATIONS = StructType([
    StructField("batch_id", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("rows_fetched", IntegerType(), True),
    StructField("api_payload", StringType(), True)
])

if not spark.catalog.tableExists(BRONZE_TABLE_LOCATIONS):
    spark.createDataFrame([], SCHEMA_BRONZE_TABLE_LOCATIONS).write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE_LOCATIONS)
    print(f"✅ Created empty Delta table: {BRONZE_TABLE_LOCATIONS}")

# --------------------------------------
# Function: Fetch locations
# --------------------------------------
def fetch_locations():
    """Fetches all locations"""
    page = 1
    results = []

    while True:
        url = (
            f"{OPENAQ_API_BASE_URL}/locations"
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
df = spark.createDataFrame([row], SCHEMA_BRONZE_TABLE_LOCATIONS)
df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE_LOCATIONS)

print(f"✅ Ingestion complete — 1 batch of {rows_fetched} locations written to {BRONZE_TABLE_LOCATIONS}")
