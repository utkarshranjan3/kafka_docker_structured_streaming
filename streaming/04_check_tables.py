from common import create_spark, BRONZE_PATH, SILVER_PATH, GOLD_RIDE_FACTS_PATH, GOLD_DRIVER_METRICS_PATH

spark = create_spark("Uber-Check-Tables")
spark.sparkContext.setLogLevel("ERROR")

for name, path in [
    ("BRONZE", BRONZE_PATH),
    ("SILVER", SILVER_PATH),
    ("GOLD RIDE FACTS", GOLD_RIDE_FACTS_PATH),
    ("GOLD DRIVER METRICS", GOLD_DRIVER_METRICS_PATH),
]:
    print(f"\\n===== {name} =====")
    try:
        df = spark.read.format("delta").load(path)
        print("Rows:", df.count())
        df.show(10, truncate=False)
    except Exception as exc:
        print(f"Not created yet or unable to read: {exc}")

spark.stop()
