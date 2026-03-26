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
    .getOrCreate()
)

telemetry_Schema = """
{
    "scooter_id": "S-123",
    "battery_level": 75,
    "latitude": 40.7128,
    "longitude": -74.0060,
    "timestamp": 1711425045123
}
"""

ride_Schema = """
{
    "ride_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": 523,
    "scooter_id": "S-456",
    "event_type": "start",
    "timestamp": 1711425045123
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
    """
    
    df_with_checks = df.withColumn(
        "is_valid",(col("scooter_id").isNotNull()) &((col("battery_level") >= 0) & (col("battery_level") <= 100)) &
        ((col("latitude") >= -90) & (col("latitude") <= 90)) &
        ((col("longitude") >= -180) & (col("longitude") <= 180)) &(col("timestamp").isNotNull())
    )
    
    # if any condition above fails add the error message in the column
    errors = concat_ws("|",
        when(col("scooter_id").isNull(), lit("scooter_id is null")),
        when((col("battery_level") < 0) | (col("battery_level") > 100), 
             lit(f"battery_level out of range [0-100]")),
        when((col("latitude") < -90) | (col("latitude") > 90), 
             lit("latitude out of range [-90,90]")),
        when((col("longitude") < -180) | (col("longitude") > 180), 
             lit("longitude out of range [-180,180]")),
        when(col("timestamp").isNull(), lit("timestamp is null"))
    )
    
    df_with_errors = df_with_checks.withColumn("error_message", 
        when(~col("is_valid"), errors).otherwise(lit(None))
    )
    
    return df_with_errors


def validate_ride_events(df):
    """
    quality checks:
    1. ride_id NOT NULL
    2. user_id NOT NULL or user_id > 0
    3. scooter_id NOT NULL
    4. event_type in ['start', 'end']
    5. timestamp NOT NULL
    """
    
    df_with_checks = df.withColumn(
        "is_valid",
        (col("ride_id").isNotNull()) &
        (col("user_id").isNotNull()) & (col("user_id") > 0) &
        (col("scooter_id").isNotNull()) &
        (col("event_type").isin(["start", "end"])) &
        (col("timestamp").isNotNull())
    )
    
    # Build error message for invalid rows
    errors = concat_ws("|",
        when(col("ride_id").isNull(), lit("ride_id is null")),
        when(col("user_id").isNull(), lit("user_id is null")),
        when(col("user_id") <= 0, lit("user_id must be > 0")),
        when(col("scooter_id").isNull(), lit("scooter_id is null")),
        when(~col("event_type").isin(["start", "end"]), 
             lit(f"event_type must be 'start' or 'end'")),
        when(col("timestamp").isNull(), lit("timestamp is null"))
    )
    
    df_with_errors = df_with_checks.withColumn("error_message", 
        when(~col("is_valid"), errors).otherwise(lit(None))
    )
    
    return df_with_errors


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

spark.streams.awaitAnyTermination()

