from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, max as spark_max, min as spark_min, 
    when, isnan, isnull
)
from datetime import datetime

GOLD_PATH = "/opt/lakehouse/gold"
MART_PATH = "/opt/lakehouse/mart"

spark = (
    SparkSession.builder
    .appName("Mart_Validation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

fact_rides = spark.read.format("delta").load(f"{GOLD_PATH}/fact_rides")
fact_telemetry = spark.read.format("delta").load(f"{GOLD_PATH}/fact_telemetry")

mart_revenue = spark.read.format("delta").load(f"{MART_PATH}/mart_revenue_hourly")
mart_fleet = spark.read.format("delta").load(f"{MART_PATH}/mart_fleet_health_hourly")


# ============================================================================
# 2. FACT TABLE COUNTS & FRESHNESS
# ============================================================================
print("\n[1] FACT TABLE OVERVIEW")
print("-" * 80)

rides_count = fact_rides.count()
rides_max_ts = fact_rides.agg(spark_max("event_ts")).collect()[0][0]
rides_min_ts = fact_rides.agg(spark_min("event_ts")).collect()[0][0]

telemetry_count = fact_telemetry.count()
telemetry_max_ts = fact_telemetry.agg(spark_max("event_ts")).collect()[0][0]
telemetry_min_ts = fact_telemetry.agg(spark_min("event_ts")).collect()[0][0]

print(f"fact_rides:")
print(f"  Total rows: {rides_count:,}")
print(f"  Time range: {rides_min_ts} → {rides_max_ts}")

print(f"\nfact_telemetry:")
print(f"  Total rows: {telemetry_count:,}")
print(f"  Time range: {telemetry_min_ts} → {telemetry_max_ts}")


print("\n[2] REVENUE MART - AGGREGATION INTEGRITY")
print("-" * 80)

# Compare fact and mart over the same mart window range (not full fact history).
mart_window_bounds = mart_revenue.agg(
    spark_min("hour_start").alias("min_hour_start"),
    spark_max("hour_end").alias("max_hour_end"),
).collect()[0]

mart_min_hour_start = mart_window_bounds["min_hour_start"]
mart_max_hour_end = mart_window_bounds["max_hour_end"]

if mart_min_hour_start is None or mart_max_hour_end is None:
    completed_rides_fact = 0
    mart_revenue_count = 0
    revenue_count_within_tolerance = True
    revenue_diff = 0
    revenue_diff_pct = 0.0
    print("mart_revenue_hourly is empty; skipping revenue count comparison.")
else:
    completed_rides_fact = (
        fact_rides
        .filter(col("event_type") == "end")
        .filter(col("event_ts") >= mart_min_hour_start)
        .filter(col("event_ts") < mart_max_hour_end)
        .count()
    )

    mart_revenue_count = mart_revenue.agg(spark_sum("completed_rides")).collect()[0][0] or 0
    revenue_diff = abs(completed_rides_fact - mart_revenue_count)
    revenue_diff_pct = (revenue_diff / completed_rides_fact * 100) if completed_rides_fact > 0 else 0.0

    # approx_count_distinct is probabilistic; allow small drift.
    REVENUE_DIFF_TOLERANCE_PCT = 5.0
    revenue_count_within_tolerance = revenue_diff_pct <= REVENUE_DIFF_TOLERANCE_PCT

    print(
        f"fact_rides completed in mart window range "
        f"[{mart_min_hour_start} to {mart_max_hour_end}): {completed_rides_fact:,} rows"
    )
    print(f"mart_revenue_hourly (sum of completed_rides): {mart_revenue_count:,} rows")

    if revenue_count_within_tolerance:
        print(
            f"✅ PASS: Counts within tolerance "
            f"({revenue_diff:,} diff, {revenue_diff_pct:.2f}% <= {REVENUE_DIFF_TOLERANCE_PCT:.2f}%)"
        )
    else:
        print(
            f"⚠️  WARN: Counts exceed tolerance "
            f"({revenue_diff:,} diff, {revenue_diff_pct:.2f}%)"
        )

# Revenue stats
revenue_stats = mart_revenue.agg(
    spark_sum("total_revenue").alias("total_rev"),
    spark_sum("total_distance_km").alias("total_dist"),
    count("*").alias("windows"),
).collect()[0]

print(f"\nRevenue aggregates:")
print(f"  Total revenue: ${revenue_stats['total_rev']:.2f}" if revenue_stats['total_rev'] else "  Total revenue: $0.00")
print(f"  Total distance: {revenue_stats['total_dist']:.2f} km" if revenue_stats['total_dist'] else "  Total distance: 0.00 km")
print(f"  Windows generated: {revenue_stats['windows']}")

# ============================================================================
# 4. FLEET HEALTH MART VALIDATION
# ============================================================================
print("\n[3] FLEET HEALTH MART - AGGREGATION INTEGRITY")
print("-" * 80)

# Count distinct scooters in fact_telemetry
telemetry_distinct_scooters = fact_telemetry.select("scooter_id").distinct().count()
print(f"fact_telemetry (distinct scooter_ids): {telemetry_distinct_scooters:,} scooters")

# Check fleet_size_seen from mart (should be <= distinct scooters observed in any window)
mart_fleet_max_size = mart_fleet.agg(spark_max("fleet_size_seen")).collect()[0][0]
print(f"mart_fleet_health (max fleet_size_seen in any window): {mart_fleet_max_size}")

if mart_fleet_max_size <= telemetry_distinct_scooters:
    print(f"✅ PASS: Max fleet size within bounds")
else:
    print(f"⚠️  WARN: Max fleet size exceeds distinct scooters (possible duplicate detection)")

# Fleet health stats
fleet_stats = mart_fleet.agg(
    spark_sum("fleet_size_seen").alias("total_fleet_observations"),
    spark_sum("low_battery_scooters").alias("total_low_battery"),
    spark_sum("scooters_in_use").alias("total_in_use"),
    spark_sum("scooters_in_maintenance").alias("total_maintenance"),
    count("*").alias("windows"),
).collect()[0]

print(f"\nFleet health aggregates:")
print(f"  Total fleet observations: {fleet_stats['total_fleet_observations']}")
print(f"  Total low battery events: {fleet_stats['total_low_battery']}")
print(f"  Total in-use observations: {fleet_stats['total_in_use']}")
print(f"  Total maintenance observations: {fleet_stats['total_maintenance']}")
print(f"  Windows generated: {fleet_stats['windows']}")

# ============================================================================
# 5. NULL CHECKS - FACT TABLES
# ============================================================================
print("\n[4] NULL CHECKS - FACT TABLES")
print("-" * 80)

rides_null_checks = fact_rides.agg(
    count(when(isnull("ride_id"), 1)).alias("null_ride_id"),
    count(when(isnull("event_ts"), 1)).alias("null_event_ts"),
    count(when(isnull("fare_amount"), 1)).alias("null_fare_amount"),
    count(when(isnull("scooter_id"), 1)).alias("null_scooter_id"),
    count(when(isnull("user_id"), 1)).alias("null_user_id"),
).collect()[0]

print("fact_rides null counts:")
print(f"  ride_id: {rides_null_checks['null_ride_id']}")
print(f"  event_ts: {rides_null_checks['null_event_ts']}")
print(f"  fare_amount: {rides_null_checks['null_fare_amount']}")
print(f"  scooter_id: {rides_null_checks['null_scooter_id']}")
print(f"  user_id: {rides_null_checks['null_user_id']}")

if all(v == 0 for v in rides_null_checks.asDict().values()):
    print("✅ PASS: No nulls in critical ride columns")
else:
    print("⚠️  WARN: Nulls found in fact_rides")

telemetry_null_checks = fact_telemetry.agg(
    count(when(isnull("scooter_id"), 1)).alias("null_scooter_id"),
    count(when(isnull("event_ts"), 1)).alias("null_event_ts"),
    count(when(isnull("battery_level"), 1)).alias("null_battery"),
    count(when(isnull("scooter_status"), 1)).alias("null_status"),
).collect()[0]

print("\nfact_telemetry null counts:")
print(f"  scooter_id: {telemetry_null_checks['null_scooter_id']}")
print(f"  event_ts: {telemetry_null_checks['null_event_ts']}")
print(f"  battery_level: {telemetry_null_checks['null_battery']}")
print(f"  scooter_status: {telemetry_null_checks['null_status']}")

if all(v == 0 for v in telemetry_null_checks.asDict().values()):
    print("✅ PASS: No nulls in critical telemetry columns")
else:
    print("⚠️  WARN: Nulls found in fact_telemetry")

# ============================================================================
# 6. DATA QUALITY CHECKS
# ============================================================================
print("\n[5] DATA QUALITY CHECKS")
print("-" * 80)

# Fare amount quality
negative_fares = fact_rides.filter(col("fare_amount") < 0).count()
zero_fares = fact_rides.filter(col("fare_amount") == 0).count()
print(f"fact_rides fare_amount quality:")
print(f"  Negative fares: {negative_fares}")
print(f"  Zero fares: {zero_fares}")

if negative_fares > 0:
    print("⚠️  WARN: Negative fares detected")

# Battery level quality (0-100 range expected)
invalid_battery = fact_telemetry.filter(
    (col("battery_level") < 0) | (col("battery_level") > 100)
).count()
print(f"\nfact_telemetry battery_level quality:")
print(f"  Invalid ranges (< 0 or > 100): {invalid_battery}")

if invalid_battery > 0:
    print("⚠️  WARN: Invalid battery values detected")

# Distance quality
negative_distance = fact_rides.filter(col("distance_km") < 0).count()
print(f"\nfact_rides distance_km quality:")
print(f"  Negative distances: {negative_distance}")

if negative_distance > 0:
    print("⚠️  WARN: Negative distances detected")

# ============================================================================
# 7. MART NULL CHECKS
# ============================================================================
print("\n[6] NULL CHECKS - MART TABLES")
print("-" * 80)

revenue_null_checks = mart_revenue.agg(
    count(when(isnull("completed_rides"), 1)).alias("null_rides"),
    count(when(isnull("total_revenue"), 1)).alias("null_revenue"),
    count(when(isnull("hour_start"), 1)).alias("null_hour_start"),
).collect()[0]

print("mart_revenue_hourly null counts:")
print(f"  completed_rides: {revenue_null_checks['null_rides']}")
print(f"  total_revenue: {revenue_null_checks['null_revenue']}")
print(f"  hour_start: {revenue_null_checks['null_hour_start']}")

if all(v == 0 for v in revenue_null_checks.asDict().values()):
    print("✅ PASS: No nulls in revenue mart")
else:
    print("⚠️  WARN: Nulls found in revenue mart")

fleet_null_checks = mart_fleet.agg(
    count(when(isnull("fleet_size_seen"), 1)).alias("null_fleet"),
    count(when(isnull("avg_battery_level"), 1)).alias("null_battery"),
    count(when(isnull("hour_start"), 1)).alias("null_hour_start"),
).collect()[0]

print("\nmart_fleet_health_hourly null counts:")
print(f"  fleet_size_seen: {fleet_null_checks['null_fleet']}")
print(f"  avg_battery_level: {fleet_null_checks['null_battery']}")
print(f"  hour_start: {fleet_null_checks['null_hour_start']}")

if all(v == 0 for v in fleet_null_checks.asDict().values()):
    print("✅ PASS: No nulls in fleet mart")
else:
    print("⚠️  WARN: Nulls found in fleet mart")

# ============================================================================
# 8. SAMPLE DATA
# ============================================================================
print("\n[7] SAMPLE DATA")
print("-" * 80)

print("\nLatest 3 mart_revenue_hourly rows:")
mart_revenue.orderBy("hour_start", ascending=False).limit(3).show(truncate=False)

print("\nLatest 3 mart_fleet_health_hourly rows:")
mart_fleet.orderBy("hour_start", ascending=False).limit(3).show(truncate=False)

# ============================================================================
# 9. SUMMARY
# ============================================================================
print("\n" + "="*80)
print("VALIDATION SUMMARY")
print("="*80)

all_checks = [
    ("Revenue count within tolerance", revenue_count_within_tolerance),
    ("Fleet max within bounds", mart_fleet_max_size <= telemetry_distinct_scooters),
    ("No nulls in fact_rides", all(v == 0 for v in rides_null_checks.asDict().values())),
    ("No nulls in fact_telemetry", all(v == 0 for v in telemetry_null_checks.asDict().values())),
    ("No nulls in mart_revenue", all(v == 0 for v in revenue_null_checks.asDict().values())),
    ("No nulls in mart_fleet", all(v == 0 for v in fleet_null_checks.asDict().values())),
    ("No negative fares", negative_fares == 0),
    ("No invalid battery levels", invalid_battery == 0),
    ("No negative distances", negative_distance == 0),
]

pass_count = sum(1 for _, passed in all_checks if passed)
total_count = len(all_checks)

for check_name, passed in all_checks:
    status = "✅ PASS" if passed else "⚠️  WARN"
    print(f"{status}: {check_name}")

print(f"\nOverall: {pass_count}/{total_count} checks passed")

if pass_count == total_count:
    print("✅ ALL VALIDATIONS PASSED - Data pipeline is healthy!")
else:
    print(f"⚠️  {total_count - pass_count} issues detected - review above")

print("="*80)
