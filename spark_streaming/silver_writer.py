from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, lit, when, concat_ws, 
    length, abs as spark_abs, to_timestamp
)
from datetime import datetime

KAFKA_BROKER = "kafka:29092"
TELEMETRY_TOPIC = "scooter_telemetry"
RIDE_EVENTS_TOPIC = "ride_events"
BRONZE_PATH = "/opt/lakehouse/bronze"
SILVER_PATH = "/opt/lakehouse/silver"
SILVER_BAD_DATA_PATH = "/opt/lakehouse/silver_bad_data"

spark = (
    SparkSession.builder
    .appName("Silver_Data_Quality")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)

telemetry_Schema = """
{
    "scooter_id": "S-123",
    "battery_level": 75,
    "latitude": 40.7128,
    "longitude": -74.0060,
    "timestamp": 1711425045123,
    "speed_kmh": 18.5,
    "odometer_km": 892.4,
    "temperature_c": 29.1,
    "signal_strength": 4,
    "is_locked": false,
    "scooter_status": "in_use"
}
"""

ride_Schema = """
{
    "ride_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": 523,
    "scooter_id": "S-456",
    "event_type": "start",
    "timestamp": 1711425045123,
    "duration_mins": 14,
    "distance_km": 2.7,
    "fare_amount": 4.53,
    "city": "new_york",
    "payment_method": "card",
    "ride_status": "in_progress"
}
"""
TELEMETRY_SCHEMA = schema_of_json(telemetry_Schema)
RIDE_EVENTS_SCHEMA = schema_of_json(ride_Schema)

def validate_telemetry(df):
    """ 
        Quality Checks:
        1. scooter_id NOT NULL
        2. battery_level between 0-100
        3. latitude between -90 and 90
        4. longitude between -180 and 180
        5. timestamp NOT NULL
        6. Optional fields (only if present) must be valid:
          speed_kmh 0-60, temperature_c -20..70, signal_strength 1-5,
          scooter_status in allowed values, odometer_km >= 0
    """

    allowed_scooter_status = ["available", "in_use", "charging", "maintenance"]

    df_with_checks = df.withColumn(
        "is_valid",
        (col("scooter_id").isNotNull())
        & ((col("battery_level") >= 0) & (col("battery_level") <= 100))
        & ((col("latitude") >= -90) & (col("latitude") <= 90))
        & ((col("longitude") >= -180) & (col("longitude") <= 180))
        & (col("timestamp").isNotNull())
        & ((col("speed_kmh").isNull()) | ((col("speed_kmh") >= 0) & (col("speed_kmh") <= 60)))
        & ((col("odometer_km").isNull()) | (col("odometer_km") >= 0))
        & ((col("temperature_c").isNull()) | ((col("temperature_c") >= -20) & (col("temperature_c") <= 70)))
        & ((col("signal_strength").isNull()) | ((col("signal_strength") >= 1) & (col("signal_strength") <= 5)))
        & ((col("scooter_status").isNull()) | (col("scooter_status").isin(allowed_scooter_status)))
    )

    # if any condition above fails add the error message in the column
    errors = concat_ws("|",
        when(col("scooter_id").isNull(), lit("scooter_id is null")),
           when((col("battery_level") < 0) | (col("battery_level") > 100),
               lit("battery_level out of range [0-100]")),
           when((col("latitude") < -90) | (col("latitude") > 90),
             lit("latitude out of range [-90,90]")),
           when((col("longitude") < -180) | (col("longitude") > 180),
             lit("longitude out of range [-180,180]")),
           when(col("timestamp").isNull(), lit("timestamp is null")),
           when(col("speed_kmh").isNotNull() & ((col("speed_kmh") < 0) | (col("speed_kmh") > 60)),
               lit("speed_kmh out of range [0,60]")),
           when(col("odometer_km").isNotNull() & (col("odometer_km") < 0),
               lit("odometer_km must be >= 0")),
           when(col("temperature_c").isNotNull() & ((col("temperature_c") < -20) | (col("temperature_c") > 70)),
               lit("temperature_c out of range [-20,70]")),
           when(col("signal_strength").isNotNull() & ((col("signal_strength") < 1) | (col("signal_strength") > 5)),
               lit("signal_strength out of range [1,5]")),
           when(col("scooter_status").isNotNull() & (~col("scooter_status").isin(allowed_scooter_status)),
               lit("scooter_status invalid"))
    )

    return df_with_checks.withColumn("error_message", when(~col("is_valid"), errors).otherwise(lit(None)))


def validate_ride_events(df):
    """Apply ride validation with optional checks for new payload fields."""
    allowed_payment_methods = ["card", "wallet", "cash"]
    allowed_ride_status = ["in_progress", "completed", "cancelled"]

    df_with_checks = df.withColumn(
        "is_valid",
        (col("ride_id").isNotNull())
        & (col("user_id").isNotNull())
        & (col("user_id") > 0)
        & (col("scooter_id").isNotNull())
        & (col("event_type").isin(["start", "end"]))
        & (col("timestamp").isNotNull())
        & ((col("duration_mins").isNull()) | ((col("duration_mins") >= 0) & (col("duration_mins") <= 180)))
        & ((col("distance_km").isNull()) | ((col("distance_km") >= 0) & (col("distance_km") <= 100)))
        & ((col("fare_amount").isNull()) | (col("fare_amount") >= 0))
        & ((col("payment_method").isNull()) | (col("payment_method").isin(allowed_payment_methods)))
        & ((col("ride_status").isNull()) | (col("ride_status").isin(allowed_ride_status)))
    )

    errors = concat_ws(
        "|",
        when(col("ride_id").isNull(), lit("ride_id is null")),
        when(col("user_id").isNull(), lit("user_id is null")),
        when(col("user_id") <= 0, lit("user_id must be > 0")),
        when(col("scooter_id").isNull(), lit("scooter_id is null")),
        when(~col("event_type").isin(["start", "end"]), lit("event_type must be 'start' or 'end'")),
        when(col("timestamp").isNull(), lit("timestamp is null")),
        when(col("duration_mins").isNotNull() & ((col("duration_mins") < 0) | (col("duration_mins") > 180)), lit("duration_mins out of range [0,180]")),
        when(col("distance_km").isNotNull() & ((col("distance_km") < 0) | (col("distance_km") > 100)), lit("distance_km out of range [0,100]")),
        when(col("fare_amount").isNotNull() & (col("fare_amount") < 0), lit("fare_amount must be >= 0")),
        when(col("payment_method").isNotNull() & (~col("payment_method").isin(allowed_payment_methods)), lit("payment_method invalid")),
        when(col("ride_status").isNotNull() & (~col("ride_status").isin(allowed_ride_status)), lit("ride_status invalid")),
    )

    return df_with_checks.withColumn("error_message", when(~col("is_valid"), errors).otherwise(lit(None)))


telemetry_bronze = (
    spark.readStream.format("delta").load(f"{BRONZE_PATH}/telemetry")
)
ride_events_bronze = (
    spark.readStream.format("delta").load(f"{BRONZE_PATH}/ride_events")
)
# here we are basically converting the json string into the proper scehma structure columns
telemetry_parsed = telemetry_bronze.select(
    col("kafka_timestamp").alias("bronze_ingestion_time"),
    from_json(col("raw_json"), TELEMETRY_SCHEMA).alias("data")
).select(
    col("bronze_ingestion_time"),
    col("data.*") 
)


ride_events_parsed = ride_events_bronze.select(
    col("kafka_timestamp").alias("bronze_ingestion_time"),
    from_json(col("raw_json"), RIDE_EVENTS_SCHEMA).alias("data")
).select(
    col("bronze_ingestion_time"),
    col("data.*")  
)



telemetry_validated = validate_telemetry(telemetry_parsed)
ride_events_validated = validate_ride_events(ride_events_parsed)


# separate the valid n invlaid data and then storing them spearately for easy degugging
telemetry_valid = telemetry_validated.filter(col("is_valid") == True).drop("is_valid", "error_message")
telemetry_invalid = telemetry_validated.filter(col("is_valid") == False).drop("is_valid")

# Ride Events
ride_events_valid = ride_events_validated.filter(col("is_valid") == True).drop("is_valid", "error_message")
ride_events_invalid = ride_events_validated.filter(col("is_valid") == False).drop("is_valid")


def write_silver_stream(df, table_name, stream_type="valid"):
    if stream_type == "valid":
        output_path = f"{SILVER_PATH}/{table_name}"
        checkpoint_path = f"{SILVER_PATH}/checkpoints/{table_name}"
    else:  # invalid / quarantine
        output_path = f"{SILVER_BAD_DATA_PATH}/{table_name}"
        checkpoint_path = f"{SILVER_BAD_DATA_PATH}/checkpoints/{table_name}"
    
    return (
        df.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )

# Write valid records to Silver
telemetry_query_valid = write_silver_stream(telemetry_valid, "telemetry", "valid")
ride_events_query_valid = write_silver_stream(ride_events_valid, "ride_events", "valid")

# Write invalid records to Quarantine (for debugging)
telemetry_query_invalid = write_silver_stream(telemetry_invalid, "telemetry", "invalid")
ride_events_query_invalid = write_silver_stream(ride_events_invalid, "ride_events", "invalid")

for query in [
    telemetry_query_valid,
    ride_events_query_valid,
    telemetry_query_invalid,
    ride_events_query_invalid,
]:
    query.awaitTermination()

