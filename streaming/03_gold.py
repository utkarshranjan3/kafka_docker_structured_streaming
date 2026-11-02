from delta.tables import DeltaTable
from pyspark.sql.functions import col, count, sum as sum_, avg, round
from common import create_spark, SILVER_PATH, GOLD_RIDE_FACTS_PATH, GOLD_DRIVER_METRICS_PATH, GOLD_CHECKPOINT

spark = create_spark("Uber-Silver-to-Gold")
spark.sparkContext.setLogLevel("WARN")

def process_batch(batch_df, batch_id):
    print(f"Processing Gold micro-batch {batch_id}")
    completed = (batch_df.filter(col("event_type") == "RIDE_COMPLETED")
        .filter(col("ride_id").isNotNull()).filter(col("driver_id").isNotNull())
        .dropDuplicates(["ride_id"]))

    if completed.limit(1).count() == 0:
        print("No completed rides in this batch.")
        return

    ride_facts = completed.select(
        "ride_id", "driver_id", "customer_id", "vehicle_type",
        "pickup_location", "drop_location", "distance_km",
        "fare_amount", "surge_multiplier", "event_time")

    if not DeltaTable.isDeltaTable(spark, GOLD_RIDE_FACTS_PATH):
        ride_facts.limit(0).write.format("delta").mode("overwrite").save(GOLD_RIDE_FACTS_PATH)

    target = DeltaTable.forPath(spark, GOLD_RIDE_FACTS_PATH)
    (target.alias("t").merge(ride_facts.alias("s"), "t.ride_id = s.ride_id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

    all_rides = spark.read.format("delta").load(GOLD_RIDE_FACTS_PATH)
    metrics = (all_rides.groupBy("driver_id", "vehicle_type").agg(
        count("ride_id").alias("total_rides"),
        round(sum_("fare_amount"), 2).alias("total_revenue"),
        round(avg("fare_amount"), 2).alias("average_fare"),
        round(avg("distance_km"), 2).alias("average_distance"),
        round(avg("surge_multiplier"), 2).alias("average_surge")))

    metrics.write.format("delta").mode("overwrite").save(GOLD_DRIVER_METRICS_PATH)
    print(f"Gold updated for batch {batch_id}")

silver_stream = spark.readStream.format("delta").load(SILVER_PATH)
query = (silver_stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .trigger(processingTime="10 seconds").start())

print("Gold stream running. Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
