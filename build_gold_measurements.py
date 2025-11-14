from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# --------------------------------------
# Load environment variables and check requirements
# --------------------------------------
load_dotenv()

# Check if required environment variables are set
required_env_vars = ["OPENAQ_API_KEY"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# --------------------------------------
# Spark setup
# --------------------------------------
spark = SparkSession.builder.appName("Build_Gold_Measurements").getOrCreate()

# --------------------------------------
# # Set database & table names
# --------------------------------------
DATABASE = os.getenv("DATABASE", "airq")
FACT_TABLE = f"{DATABASE}.fact_measurements"
DAILY_STATS_TABLE = f"{DATABASE}.sensor_daily_completeness"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DAILY_STATS_TABLE} (
    sensor_id INT,
    location_id INT,
    country_code STRING,
    parameter_id INT,
    parameter_name STRING,
    day_start DATE,
    avg_value DOUBLE,
    min_value DOUBLE,
    max_value DOUBLE,
    stddev_value DOUBLE,
    perc95_value DOUBLE,
    perc05_value DOUBLE,
    reading_count INT,
    completeness_ratio DOUBLE
)
""")

# Daily stats aggregation
spark.sql(f"""
MERGE INTO {DAILY_STATS_TABLE} t
USING (
  SELECT
    sensor_id,
    location_id,
    country_code,
    parameter_id,
    parameter_name,
    date(measurement_datetime_from_utc) AS day_start,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    STDDEV_POP(value) AS stddev_value,
    PERCENTILE_APPROX(value, 0.95) AS perc95_value,
    PERCENTILE_APPROX(value, 0.05) AS perc05_value,
    COUNT(*) AS reading_count,
    (COUNT(*) / 24.0) AS completeness_ratio
  FROM {FACT_TABLE}
  WHERE measurement_datetime_from_utc >= date_sub(current_date(), 30)
  GROUP BY sensor_id, location_id, country_code, parameter_id, parameter_name, date(measurement_datetime_from_utc)
) s
ON t.sensor_id = s.sensor_id
 AND t.parameter_id = s.parameter_id
 AND t.day_start = s.day_start
WHEN MATCHED THEN UPDATE SET
  t.avg_value = s.avg_value,
  t.min_value = s.min_value,
  t.max_value = s.max_value,
  t.stddev_value = s.stddev_value,
  t.perc95_value = s.perc95_value,
  t.perc05_value = s.perc05_value,
  t.reading_count = s.reading_count,
  t.completeness_ratio = s.completeness_ratio
WHEN NOT MATCHED THEN INSERT *
""")

print("Gold daily aggregation complete.")
