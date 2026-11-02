# Uber Kafka + Spark Structured Streaming + Delta Lake

A local practice project using Apache Kafka, PySpark Structured Streaming and Delta Lake.

## Prerequisites
- Windows, Docker Desktop and WSL2
- VS Code
- Python 3.11+ on Windows for the producer
- Docker Compose

This is a single-node local learning setup, not a production cluster. 8 GB RAM or more is recommended. A 4 GB machine may struggle.

## Layout
```text
uber-kafka-spark-project/
├── docker-compose.yml
├── spark/Dockerfile
├── spark/requirements.txt
├── producer/requirements.txt
├── producer/uber_producer.py
├── streaming/common.py
├── streaming/01_bronze.py
├── streaming/02_silver.py
├── streaming/03_gold.py
├── streaming/04_check_tables.py
├── data/
└── checkpoints/
```

## 1. Start Docker services
Run PowerShell from the folder containing docker-compose.yml:
```powershell
docker compose up -d --build
docker compose ps
```
Windows producer uses `localhost:9092`. Spark container uses `kafka:29092`.

## 2. Create Kafka topic
```powershell
docker exec uber-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic uber_rides --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## 3. Install and run producer on Windows
```powershell
py -3.11 -m venv .venv
.venv\\Scripts\\activate
pip install -r producer/requirements.txt
python producer/uber_producer.py
```
The producer sends 20 rides and four events per ride (80 messages).

## 4. Start the streaming apps
Open three PowerShell terminals from the project root. Start in order:

Terminal A:
```powershell
docker exec -it uber-spark python /app/streaming/01_bronze.py
```
Terminal B:
```powershell
docker exec -it uber-spark python /app/streaming/02_silver.py
```
Terminal C:
```powershell
docker exec -it uber-spark python /app/streaming/03_gold.py
```
Keep all three running. First startup downloads Kafka and Delta JARs from Maven Central.

## 5. Publish events
Run the producer again from Windows:
```powershell
.venv\\Scripts\\activate
python producer/uber_producer.py
```

## 6. Inspect tables
```powershell
docker exec -it uber-spark python /app/streaming/04_check_tables.py
```

## 7. Stop
Press Ctrl+C in each Spark terminal, then:
```powershell
docker compose down
```
This preserves Kafka's named volume and local data/checkpoint directories. `docker compose down -v` also deletes Kafka's stored broker data.

## Notes
- Keep each streaming checkpoint for its query. Do not casually delete or reuse it with a changed query.
- `startingOffsets=earliest` applies on a new Bronze checkpoint; existing checkpoints control restart offsets.
- Silver deduplication is watermark-bounded; very late duplicate events may not be removed.
- Gold merges completed ride facts by ride_id and recomputes driver summaries. It is a practice implementation, not an optimized production aggregate.
- Kafka uses plaintext only inside the local Docker network. Do not expose the broker publicly.
- Spark runs `local[1]`: this is a single-node local Spark process, not a multi-node cluster.
