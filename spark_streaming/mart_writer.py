from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, approx_count_distinct, sum as spark_sum, avg, when
)

GOLD_PATH = "/opt/lakehouse/gold"
MART_PATH = "/opt/lakehouse/mart"

COMPLETED_EVENT_TYPE = "end"
LOW_BATTERY_THRESHOLD = 20
# Fast demo mode: shorter windows make mart rows appear quickly.
MART_WINDOW_DURATION = "1 minute"
MART_WATERMARK = "1 minute"

spark = (
    SparkSession.builder
    .appName("Mart_Business_Finance")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)


fact_rides = spark.readStream.format("delta").load(f"{GOLD_PATH}/fact_rides")
fact_telemetry = spark.readStream.format("delta").load(f"{GOLD_PATH}/fact_telemetry")


#kpi 1 is total revenue calculated by the rides completed
revenue_base = (
    fact_rides
    .filter(col("event_type") == COMPLETED_EVENT_TYPE)
    .filter(col("event_ts").isNotNull())
    .withWatermark("event_ts", MART_WATERMARK)
)

mart_revenue_hourly = (
    revenue_base
    .groupBy(window(col("event_ts"), MART_WINDOW_DURATION).alias("hour_window"))
    .agg(
        approx_count_distinct("ride_id").alias("completed_rides"),
        spark_sum(col("fare_amount")).alias("total_revenue"),
        avg(col("fare_amount")).alias("avg_fare_amount"),
        spark_sum(col("distance_km")).alias("total_distance_km"),
        avg(col("distance_km")).alias("avg_distance_km"),
        avg(col("duration_mins")).alias("avg_duration_mins"),
    )
    .select(
        col("hour_window.start").alias("hour_start"),
        col("hour_window.end").alias("hour_end"),
        col("completed_rides"),
        col("total_revenue"),
        col("avg_fare_amount"),
        col("total_distance_km"),
        col("avg_distance_km"),
        col("avg_duration_mins"),
    )
)

# kpi 2 is fleet health calculated by the telemetry data
fleet_base = (
    fact_telemetry
    .filter(col("event_ts").isNotNull())
    .withWatermark("event_ts", MART_WATERMARK)
)

mart_fleet_health_hourly = (
    fleet_base
    .groupBy(window(col("event_ts"), MART_WINDOW_DURATION).alias("hour_window"))
    .agg(
        approx_count_distinct("scooter_id").alias("fleet_size_seen"),
        avg(col("battery_level")).alias("avg_battery_level"),
        approx_count_distinct(when(col("battery_level") < LOW_BATTERY_THRESHOLD, col("scooter_id"))).alias("low_battery_scooters"),
        approx_count_distinct(when(col("scooter_status") == "in_use", col("scooter_id"))).alias("scooters_in_use"),
        approx_count_distinct(when(col("scooter_status") == "maintenance", col("scooter_id"))).alias("scooters_in_maintenance"),
    )
    .select(
        col("hour_window.start").alias("hour_start"),
        col("hour_window.end").alias("hour_end"),
        col("fleet_size_seen"),
        col("avg_battery_level"),
        col("low_battery_scooters"),
        col("scooters_in_use"),
        col("scooters_in_maintenance"),
    )
)


def write_mart_stream(df, table_name):
    output_path = f"{MART_PATH}/{table_name}"
    checkpoint_path = f"{MART_PATH}/checkpoints/{table_name}"

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )


mart_revenue_hourly_query = write_mart_stream(mart_revenue_hourly, "mart_revenue_hourly")
mart_fleet_health_hourly_query = write_mart_stream(mart_fleet_health_hourly, "mart_fleet_health_hourly")

spark.streams.awaitAnyTermination()

