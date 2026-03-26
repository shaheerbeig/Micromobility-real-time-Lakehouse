from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, window, sum as spark_sum, avg, count, countDistinct,
    when, lit, to_timestamp, date_format, round as spark_round
)
from datetime import datetime

SILVER_PATH = "/opt/lakehouse/silver"
GOLD_PATH = "/opt/lakehouse/gold"

spark = (
    SparkSession.builder
    .appName("Gold_Business_Analytics")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)

rides = spark.readStream.format("delta").load(f"{SILVER_PATH}/ride_events")
telemetry = spark.readStream.format("delta").load(f"{SILVER_PATH}/telemetry")

rides.limit(1).show(truncate=False, vertical=True)

print("\n✓ Sample 1 row from telemetry:")
telemetry.limit(1).show(truncate=False, vertical=True)

spark.streams.awaitAnyTermination()
