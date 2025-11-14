from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from pyspark.sql.types import *
from pyspark.sql import functions as F
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
# # Set database & table names
# --------------------------------------
DATABASE = os.getenv("DATABASE", "airq")
BRONZE_TABLE_MEASUREMENTS = f"{DATABASE}.bronze_measurements_batches"
STAGING_TABLE_MEASUREMENTS = f"{DATABASE}.silver_measurements_exploded"
FACT_TABLE_MEASUREMENTS = f"{DATABASE}.fact_measurements"

# --------------------------------------
# Create database & tables if missing
# --------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Schema for one measurement item inside api_payload array
measurement_item_struct = StructType([
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("parameter", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("units", StringType(), True),
        StructField("displayName", StringType(), True),
    ]), True),
    StructField("period", StructType([
        StructField("label", StringType(), True),
        StructField("interval", StringType(), True),  # "HH:MM:SS"
        StructField("datetimeFrom", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True),
        ]), True),
        StructField("datetimeTo", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True),
        ]), True),
    ]), True)
]) 

item_array_schema = ArrayType(measurement_item_struct, True)  # replaces the string schema

# Schema for staging table
staging_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("country_code", StringType(), True),
    StructField("parameter_id", IntegerType(), True),
    StructField("parameter_name", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("measurement_datetime_from_utc", TimestampType(), True),
    StructField("measurement_datetime_to_utc", TimestampType(), True),
    StructField("ingestion_datetime", TimestampType(), True),
    StructField("ingestion_window_datetime_from", TimestampType(), True),
    StructField("ingestion_window_datetime_to", TimestampType(), True),
    StructField("raw_json", StringType(), True),
    StructField("exploded_at", TimestampType(), True)
])

if not spark.catalog.tableExists(STAGING_TABLE_MEASUREMENTS):
    spark.createDataFrame([], staging_schema).write.format("delta").mode("overwrite").saveAsTable(STAGING_TABLE_MEASUREMENTS)
    print(f"✅ Created empty Delta table: {STAGING_TABLE_MEASUREMENTS}")

# Schema for fact table
fact_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("country_code", StringType(), True),
    StructField("parameter_id", IntegerType(), True),
    StructField("parameter_name", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("measurement_datetime_from_utc", TimestampType(), True),
    StructField("measurement_datetime_to_utc", TimestampType(), True),
    StructField("batch_id", StringType(), True),
    StructField("ingestion_datetime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
])

if not spark.catalog.tableExists(FACT_TABLE_MEASUREMENTS):
    spark.createDataFrame([], fact_schema).write.format("delta").mode("overwrite").saveAsTable(FACT_TABLE_MEASUREMENTS)
    print(f"✅ Created empty Delta table: {FACT_TABLE_MEASUREMENTS}")

# --------------------------------------
# 1. Explode bronze → staging (one row per measurement)
# --------------------------------------
# Fetch unprocessed bronze batches
processed_batch_ids = (
    spark.table(FACT_TABLE_MEASUREMENTS)
         .select("batch_id")
         .distinct()
)

new_batches = (
    spark.table(BRONZE_TABLE_MEASUREMENTS)
         .alias("b")
         .join(processed_batch_ids.alias("p"),
               F.col("b.batch_id") == F.col("p.batch_id"),
               "left_anti")  # only batches not yet processed
)

# If none, exit early
if new_batches.count() == 0:
    print(f"No new batches from {BRONZE_TABLE_MEASUREMENTS} table to process.")
    spark.stop()
    exit(0)

staging_df = (
    new_batches
      .select("batch_id","sensor_id","location_id","country_code","parameter_id","ingestion_datetime",
              "ingestion_window_datetime_from","ingestion_window_datetime_to",
              F.from_json("api_payload", item_array_schema).alias("items"))
      .withColumn("item", F.explode("items"))
      .selectExpr(
          "batch_id",
          "sensor_id",
          "location_id",
          "country_code",
          "cast(coalesce(item.parameter.id, parameter_id) as int) as parameter_id",
          "item.parameter.name as parameter_name",
          "item.value as value",
          "item.unit as unit",
          "cast(item.period.datetimeFrom.utc as timestamp) as measurement_datetime_from_utc",
          "cast(item.period.datetimeTo.utc as timestamp) as measurement_datetime_to_utc",
          "cast(ingestion_datetime as timestamp) as ingestion_datetime",
          "cast(ingestion_window_datetime_from as timestamp) as ingestion_window_datetime_from",
          "cast(ingestion_window_datetime_to as timestamp) as ingestion_window_datetime_to",
          "to_json(item) as raw_json",
          "current_timestamp() as exploded_at"
      )
)

if not spark.catalog.tableExists(STAGING_TABLE_MEASUREMENTS):
    staging_df.limit(0).write.format("delta").saveAsTable(STAGING_TABLE_MEASUREMENTS)
staging_df.write.format("delta").mode("append").saveAsTable(STAGING_TABLE_MEASUREMENTS)

print(f"✅ Exploded {staging_df.count()} measurements into staging table: {STAGING_TABLE_MEASUREMENTS}")

# --------------------------------------
# 2. Load staging → fact (dedup via MERGE)
# --------------------------------------
fact_src = staging_df.selectExpr(
    "sensor_id",
    "location_id",
    "country_code",
    "parameter_id",
    "parameter_name",
    "value",
    "unit",
    "measurement_datetime_from_utc",
    "measurement_datetime_to_utc",
    "batch_id",
    "ingestion_datetime",
    "current_timestamp() as loaded_at"
)

if not spark.catalog.tableExists(FACT_TABLE_MEASUREMENTS):
    fact_src.limit(0).write.format("delta").saveAsTable(FACT_TABLE_MEASUREMENTS)

fact_src.createOrReplaceTempView("incoming_measurements")

spark.sql(f"""
MERGE INTO {FACT_TABLE_MEASUREMENTS} t
USING incoming_measurements s
ON t.sensor_id = s.sensor_id
 AND t.parameter_id = s.parameter_id
 AND t.measurement_datetime_from_utc = s.measurement_datetime_from_utc
 AND t.measurement_datetime_to_utc = s.measurement_datetime_to_utc
WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ Merged measurements into fact table: {FACT_TABLE_MEASUREMENTS}")
