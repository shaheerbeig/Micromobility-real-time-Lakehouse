from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA_BROKER = "kafka:29092"

TELEMETRY_TOPIC = "scooter_telemetry"
RIDE_EVENTS_TOPIC = "ride_events"
BRONZE_PATH = "/opt/lakehouse/bronze"


spark = (
    SparkSession.builder
    .appName("Bronze_Data")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

def read_kafka_topic(topic_name):
    """Returns a streaming DataFrame for the given Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic_name)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(col("value").cast("string").alias("raw_json"),
                col("timestamp").alias("kafka_timestamp"))
    )

telemetry_stream = read_kafka_topic(TELEMETRY_TOPIC)
ride_events_stream = read_kafka_topic(RIDE_EVENTS_TOPIC)

def write_bronze(stream_df, table_name):
    """Writes a streaming DataFrame to a Delta Lake Bronze table."""
    checkpoint_path = f"{BRONZE_PATH}/checkpoints/{table_name}"
    output_path = f"{BRONZE_PATH}/{table_name}"

    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )

print("🚀 Starting Bronze Writer — reading from Kafka and writing to Delta Lake...")
print(f"   Telemetry  → {BRONZE_PATH}/telemetry")
print(f"   Ride Events → {BRONZE_PATH}/ride_events")

# Start both streams concurrently
telemetry_query  = write_bronze(telemetry_stream,  "telemetry")
ride_events_query = write_bronze(ride_events_stream, "ride_events")

# Keep the application running until manually stopped (Ctrl+C)
spark.streams.awaitAnyTermination()
