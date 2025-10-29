# Real-Time Data Lake — Kafka + Spark + Hadoop

End-to-end **data engineering pipeline** built for real-time streaming using:
- **Apache Kafka** → Data ingestion layer  
- **Apache Spark Structured Streaming** → Real-time processing layer  
- **HDFS (Hadoop)** → Data lake storage layer  

---

##  Quick Start

###  Start Kafka Cluster
```bash
docker compose up -d
```
### Create Topics
```
docker exec -it kafka1 kafka-topics --create \
  --topic datalakex \
  --bootstrap-server kafka1:29092 \
  --partitions 3 \
  --replication-factor 3

```

### Run the Kafka Producer
```
python kafka/producer.py
```

### Start Spark Structured Streaming
```
spark-submit spark/spark_hadoop_stream.py
```

### Read Processed Data from HDFS
```
spark-submit spark/read_parquet.py

```

## Requirements

### Install dependencies:
```
pip install -r requirements.txt
```


### Project Structure

kafka/
 └── producer.py              # Streams JSON data into Kafka
spark/
 ├── spark_hadoop_stream.py   # Consumes Kafka stream and writes to HDFS
 └── read_parquet.py          # Reads processed Parquet data
docker-compose.yml            # Kafka + Zookeeper + UI setup
requirements.txt              # Python dependencies





