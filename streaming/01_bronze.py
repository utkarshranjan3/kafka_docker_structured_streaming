from pyspark.sql.functions import col, current_timestamp
from common import create_spark, BRONZE_PATH, BRONZE_CHECKPOINT

spark = create_spark("Uber-Kafka-to-Bronze")
spark.sparkContext.setLogLevel("WARN")

kafka_stream = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "uber_rides")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .load())

bronze_stream = kafka_stream.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_message"),
    col("topic"), col("partition"), col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_timestamp"))

query = (bronze_stream.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .trigger(processingTime="10 seconds").start(BRONZE_PATH))

print("Bronze stream running. Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
