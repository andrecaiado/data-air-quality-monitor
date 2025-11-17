import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils

from config.settings import get_config 

# --------------------------------------
# Spark setup and other initializations
# --------------------------------------
spark = SparkSession.builder.appName("Ingest_Bronze_Measurements").getOrCreate()
dbutils = DBUtils(spark)

# --------------------------------------
# Set database & table names
# --------------------------------------
DATABASE = get_config("DATABASE")
BRONZE_TABLE_LOCATIONS = f"{DATABASE}.bronze_locations_snapshots"
DIM_TABLE_LOCATIONS = f"{DATABASE}.dim_locations"
DIM_TABLE_SENSORS = f"{DATABASE}.dim_sensors"

# --------------------------------------
# Create database & silver tables if missing
# --------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Define locations silver table schema
SCHEMA_DIM_TABLE_LOCATIONS = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("coordinates_latitude", DoubleType(), True),
    StructField("coordinates_longitude", DoubleType(), True),
    StructField("last_updated_utc", StringType(), True),
    StructField("bronze_ingestion_time", StringType(), True),
    StructField("dim_ingested_at", TimestampType(), True)
])

if not spark.catalog.tableExists(DIM_TABLE_LOCATIONS):
    spark.createDataFrame([], SCHEMA_DIM_TABLE_LOCATIONS).write.format("delta").mode("overwrite").saveAsTable(DIM_TABLE_LOCATIONS)
    print(f"✅ Created empty Delta table: {DIM_TABLE_LOCATIONS}")

# Define sensors silver table schema
SCHEMA_DIM_TABLE_SENSORS = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("parameter_id", IntegerType(), True),
    StructField("parameter_name", StringType(), True),
    StructField("parameter_units", StringType(), True),
    StructField("parameter_display_name", StringType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("bronze_ingestion_time", StringType(), True),
    StructField("dim_ingested_at", TimestampType(), True)
])

if not spark.catalog.tableExists(DIM_TABLE_SENSORS):
    spark.createDataFrame([], SCHEMA_DIM_TABLE_SENSORS).write.format("delta").mode("overwrite").saveAsTable(DIM_TABLE_SENSORS)
    print(f"✅ Created empty Delta table: {DIM_TABLE_SENSORS}")

# --------------------------------------
# Helper: Load locations and sensors from bronze table
# --------------------------------------
def load_locations_and_sensors_from_bronze():
    """Loads locations and sensors from bronze table."""
    bronze_df = spark.sql(f"SELECT ingestion_time, api_payload FROM {BRONZE_TABLE_LOCATIONS} ORDER BY ingestion_time DESC LIMIT 1")
    if bronze_df.count() == 0:
        print("No data found in bronze table.")
        return

    row = bronze_df.collect()[0]
    locations = json.loads(row['api_payload'])
    bronze_ingestion_time = row['ingestion_time']
    
    location_rows = []
    sensor_rows = []

    for loc in locations:
        country = loc.get("country") or {}
        coords = loc.get("coordinates") or {}
        dt_last = (loc.get("datetimeLast") or {}).get("utc")  # safe

        # build location row (no sensors embedded)
        location_rows.append((
            loc.get("id"),
            loc.get("name"),
            loc.get("locality"),
            country.get("name"),
            country.get("code"),
            coords.get("latitude"),
            coords.get("longitude"),
            dt_last,
            bronze_ingestion_time,
            None
        ))
        
        # build sensor rows
        for sensor in (loc.get("sensors") or []):
            parameter = sensor.get("parameter") or {}
            sensor_rows.append((
                sensor.get("id"),
                sensor.get("name"),
                parameter.get("id"),
                parameter.get("name"),
                parameter.get("units"),
                parameter.get("displayName"),
                loc.get("id"),
                bronze_ingestion_time,
                None
            ))

    return location_rows, sensor_rows

# --------------------------------------
# Step 1: Load locations and sensors from bronze table
# --------------------------------------
location_rows, sensor_rows = load_locations_and_sensors_from_bronze()

# --------------------------------------
# Step 2: Upsert locations into silver table
# --------------------------------------
if not location_rows:
    print("No locations to upsert.")
    spark.stop()
    exit(0)

df = spark.createDataFrame(location_rows, SCHEMA_DIM_TABLE_LOCATIONS)
df.createOrReplaceTempView("loaded_locations")
print(f"🔍 Upserting {len(location_rows)} locations into {DIM_TABLE_LOCATIONS}")
spark.sql(f"""
MERGE INTO {DIM_TABLE_LOCATIONS} AS target
USING loaded_locations AS src
ON target.location_id = src.location_id
WHEN MATCHED AND target.last_updated_utc <> src.last_updated_utc
  AND src.last_updated_utc IS NOT NULL THEN UPDATE SET
    target.name = src.name,
    target.locality = src.locality,
    target.country_name = src.country_name,
    target.country_code = src.country_code,
    target.coordinates_latitude = src.coordinates_latitude,
    target.coordinates_longitude = src.coordinates_longitude,
    target.last_updated_utc = src.last_updated_utc,
    target.bronze_ingestion_time = src.bronze_ingestion_time,
    target.dim_ingested_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    location_id, name, locality, country_name, country_code,
    coordinates_latitude, coordinates_longitude, last_updated_utc,
    bronze_ingestion_time, dim_ingested_at
) VALUES (
    src.location_id, src.name, src.locality, src.country_name, src.country_code,
    src.coordinates_latitude, src.coordinates_longitude, src.last_updated_utc,
    src.bronze_ingestion_time, current_timestamp()
)
""")
print(f"✅ Upserted {len(location_rows)} locations into {DIM_TABLE_LOCATIONS}")

# --------------------------------------
# Step 3: Upsert sensors into silver table
# --------------------------------------
if sensor_rows:
    df = spark.createDataFrame(sensor_rows, SCHEMA_DIM_TABLE_SENSORS)
    df.createOrReplaceTempView("loaded_sensors")
    print(f"🔍 Upserting {len(sensor_rows)} sensors into {DIM_TABLE_SENSORS}")
    spark.sql(f"""
    MERGE INTO {DIM_TABLE_SENSORS} AS target
    USING loaded_sensors AS src
    ON target.sensor_id = src.sensor_id
    WHEN MATCHED THEN UPDATE SET
        target.name = src.name,
        target.parameter_id = src.parameter_id,
        target.parameter_name = src.parameter_name,
        target.parameter_units = src.parameter_units,
        target.parameter_display_name = src.parameter_display_name,
        target.location_id = src.location_id,
        target.bronze_ingestion_time = src.bronze_ingestion_time,
        target.dim_ingested_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        sensor_id, name, parameter_id, parameter_name, parameter_units,
        parameter_display_name, location_id, bronze_ingestion_time, dim_ingested_at
    ) VALUES (
        src.sensor_id, src.name, src.parameter_id, src.parameter_name, src.parameter_units,
        src.parameter_display_name, src.location_id, src.bronze_ingestion_time, current_timestamp()
    )
    """)
    print(f"✅ Upserted {len(sensor_rows)} sensors into {DIM_TABLE_SENSORS}")
