from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime,date_trunc,year,month,dayofmonth,dayofweek,hour)

SILVER_PATH = "/opt/lakehouse/silver"
GOLD_PATH = "/opt/lakehouse/gold"

# Dedup watermark for slowly changing dimensions and time dimension.
DEDUP_WATERMARK = "7 days"

spark = (
    SparkSession.builder
    .appName("Gold_Base_Model")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)

rides = spark.readStream.format("delta").load(f"{SILVER_PATH}/ride_events")
telemetry = spark.readStream.format("delta").load(f"{SILVER_PATH}/telemetry")

# converting unixtime into the timestamp
rides_prepared = (
    rides.withColumn("event_ts", from_unixtime((col("timestamp") / 1000)).cast("timestamp"))
    .filter(col("event_ts").isNotNull())
)

telemetry_prepared = (
    telemetry.withColumn("event_ts", from_unixtime((col("timestamp") / 1000)).cast("timestamp"))
    .filter(col("event_ts").isNotNull())
)
# fact table
fact_rides = rides_prepared.select(
    col("ride_id"),
    col("event_ts"),
    col("bronze_ingestion_time"),
    col("user_id"),
    col("scooter_id"),
    col("event_type"),
    col("ride_status"),
    col("city"),
    col("payment_method"),
    col("distance_km"),
    col("duration_mins"),
    col("fare_amount"),
)

fact_telemetry = telemetry_prepared.select(
    col("event_ts"),
    col("bronze_ingestion_time"),
    col("scooter_id"),
    col("latitude"),
    col("longitude"),
    col("battery_level"),
    col("speed_kmh"),
    col("odometer_km"),
    col("temperature_c"),
    col("signal_strength"),
    col("is_locked"),
    col("scooter_status"),
)


# ---------------------------------------------------------------------------
# 4) Gold Dimensions (lightweight, incrementally maintained)
# ---------------------------------------------------------------------------
# dim_payment_method
# Natural key is payment_method. We deduplicate incrementally.
dim_payment_method = (
    rides_prepared.withWatermark("event_ts", DEDUP_WATERMARK)
    .select(col("event_ts"), col("payment_method"))
    .filter(col("payment_method").isNotNull())
    .dropDuplicates(["payment_method"])
    .select(col("payment_method"))
)

# dim_city
# Natural key is city.
dim_city = (
    rides_prepared.withWatermark("event_ts", DEDUP_WATERMARK)
    .select(col("event_ts"), col("city"))
    .filter(col("city").isNotNull())
    .dropDuplicates(["city"])
    .select(col("city"))
)

# dim_time_hourly
# Hour-level time dimension generated only from observed business events.
all_event_times = rides_prepared.select(col("event_ts")).unionByName(
    telemetry_prepared.select(col("event_ts"))
)

dim_time_hourly = (
    all_event_times.withWatermark("event_ts", DEDUP_WATERMARK)
    .withColumn("hour_start", date_trunc("hour", col("event_ts")))
    .dropDuplicates(["hour_start"])
    .select(
        col("hour_start"),
        year(col("hour_start")).alias("year"),
        month(col("hour_start")).alias("month"),
        dayofmonth(col("hour_start")).alias("day"),
        dayofweek(col("hour_start")).alias("day_of_week"),
        hour(col("hour_start")).alias("hour"),
    )
)


def write_gold_stream(df, table_name):
    output_path = f"{GOLD_PATH}/{table_name}"
    checkpoint_path = f"{GOLD_PATH}/checkpoints/{table_name}"

    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )


fact_rides_query = write_gold_stream(fact_rides, "fact_rides")
fact_telemetry_query = write_gold_stream(fact_telemetry, "fact_telemetry")
dim_payment_method_query = write_gold_stream(dim_payment_method, "dim_payment_method")
dim_city_query = write_gold_stream(dim_city, "dim_city")
dim_time_hourly_query = write_gold_stream(dim_time_hourly, "dim_time_hourly")

spark.streams.awaitAnyTermination()
