from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from common import create_spark, BRONZE_PATH, SILVER_PATH, SILVER_CHECKPOINT

spark = create_spark("Uber-Bronze-to-Silver")
spark.sparkContext.setLogLevel("WARN")

ride_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("ride_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("pickup_location", StringType(), True),
    StructField("drop_location", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("status", StringType(), True),
])

bronze = spark.readStream.format("delta").load(BRONZE_PATH)
parsed = (bronze.select(from_json(col("raw_message"), ride_schema).alias("data"))
    .select("data.*").withColumn("event_time", to_timestamp("event_time")))

valid = parsed.filter(
    col("event_id").isNotNull() & col("ride_id").isNotNull()
    & col("driver_id").isNotNull() & col("event_time").isNotNull()
    & col("event_type").isin("RIDE_REQUESTED", "DRIVER_ACCEPTED", "RIDE_STARTED", "RIDE_COMPLETED", "RIDE_CANCELLED")
    & col("fare_amount").isNotNull() & (col("fare_amount") >= 0)
    & col("distance_km").isNotNull() & (col("distance_km") > 0))

deduplicated = valid.withWatermark("event_time", "2 hours").dropDuplicatesWithinWatermark(["event_id"])

query = (deduplicated.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(processingTime="10 seconds").start(SILVER_PATH))

print("Silver stream running. Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
