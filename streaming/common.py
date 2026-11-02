import os
from pyspark.sql import SparkSession

ROOT = "/app"
DATA_DIR = os.path.join(ROOT, "data")
CHECKPOINT_DIR = os.path.join(ROOT, "checkpoints")
BRONZE_PATH = os.path.join(DATA_DIR, "bronze")
SILVER_PATH = os.path.join(DATA_DIR, "silver")
GOLD_RIDE_FACTS_PATH = os.path.join(DATA_DIR, "gold_ride_facts")
GOLD_DRIVER_METRICS_PATH = os.path.join(DATA_DIR, "gold_driver_metrics")
BRONZE_CHECKPOINT = os.path.join(CHECKPOINT_DIR, "bronze")
SILVER_CHECKPOINT = os.path.join(CHECKPOINT_DIR, "silver")
GOLD_CHECKPOINT = os.path.join(CHECKPOINT_DIR, "gold")

def create_spark(app_name):
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.2.1"
    return (SparkSession.builder.appName(app_name).master("local[1]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate())
