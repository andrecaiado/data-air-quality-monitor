import datetime
import uuid
from pandas import DataFrame
import json
from pyspark.sql import SparkSession
from datetime import datetime, timezone
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils

from config.settings import get_config

# --------------------------------------
# Spark setup and other initializations
# --------------------------------------
spark = SparkSession.builder.appName("Ingest_Bronze_Locations").getOrCreate()
dbutils = DBUtils(spark)

# --------------------------------------
# Set database & table names
# --------------------------------------
DATABASE = get_config("DATABASE", default="airq")
BRONZE_TABLE_LOCATIONS = f"{DATABASE}.bronze_locations_snapshots"

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
def fetch_locations() -> DataFrame:
    """Fetches all locations using the OpenAqDatasource"""
    from openaq_datasource import OpenAqDatasource

    options = {
        "endpoint": "locations",
        "headers": {},
        "params": json.dumps({
            "limit": 1000,
        }),
        "delayBetweenRequestsInSeconds": 1.05  # to avoid rate limits
    }

    spark.dataSource.register(OpenAqDatasource)
    results = (
        spark.read.format("openaqdatasource")
            .options(**options).load()
    )

    print(f"🔍 Fetched {results.count()} locations")
    return results

# --------------------------------------
# Step 1: Fetch locations
# --------------------------------------
locations_json_array = fetch_locations().toJSON().collect()

# --------------------------------------
# Step 2: Ingest locations into bronze table
# --------------------------------------
batch_id = str(uuid.uuid4())
ingestion_time = datetime.now(timezone.utc)
rows_fetched = len(locations_json_array)
row = (
            batch_id,
            ingestion_time.isoformat(),
            rows_fetched,
            json.dumps(locations_json_array)  # All locations as JSON array
        )
df = spark.createDataFrame([row], SCHEMA_BRONZE_TABLE_LOCATIONS)
df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE_LOCATIONS)

print(f"✅ Ingestion complete — 1 batch of {rows_fetched} locations written to {BRONZE_TABLE_LOCATIONS}")
