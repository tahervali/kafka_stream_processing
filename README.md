# Kafka-Spark Streaming Data Processing Project

This project demonstrates a real-time data processing pipeline using Apache Kafka for data ingestion and Apache Spark for stream processing. It simulates a system that generates view logs for advertising campaigns, processes them in real-time, and stores the aggregated results.

## Project Structure

- `kafka_producer/`: Contains the Kafka producer code

  - `config.py`: Configuration settings for the producer
  - `producer.py`: Generates and sends simulated view log data to Kafka
  - `requirements.txt`: Python dependencies for the producer
  - `utils.py`: Utility functions for the producer
  - `wait-for-kafka.sh`: Shell script to wait for Kafka to be ready

- `spark_kafka_consumer/`: Contains the Spark streaming consumer code

  - `config.py`: Configuration settings for the consumer
  - `spark_consumer.py`: Processes streaming data from Kafka using Spark
  - `requirements.txt`: Python dependencies for the consumer
  - `utils.py`: Utility functions for the consumer
  - `wait-for-kafka.sh`: Shell script to wait for Kafka to be ready

- `docker-compose.yml`: Defines and configures the services for the project

## Prerequisites

- Docker
- Docker Compose

## Setup and Running

1. Clone the repository:

   ```
   git clone <repository-url>
   cd <project-directory>
   ```

2. Create a `.env` file in the project root directory with the following content:

   ```
   KAFKA_NODE_ID=1
   KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
   KAFKA_LISTENERS=PLAINTEXT://broker:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://broker:29093
   KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
   KAFKA_CONTROLLER_QUORUM_VOTERS=1@broker:29093
   KAFKA_PROCESS_ROLES=broker,controller
   KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
   KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
   KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
   KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
   KAFKA_NUM_PARTITIONS=1
   KAFKA_TOPIC_CONFIG_retention_ms=60000
   CLUSTER_ID=3mXJP6ZyTHC5LcghVd7j5A
   BROKER_DATA=./data/kafka
   KAFKA_SERVER=broker:9092
   PRODUCER_INTERVAL=1
   CONSUMER_INTERVAL=5
   LOG_DATA=./data
   ```

3. Create the necessary directories:

   ```
   mkdir -p data/kafka data/output_data data/log_data/input_data
   ```

4. Place the `campaigns.csv` file in the `data/log_data/input_data` directory.

5. Build and start the services:
   ```
   docker-compose up --build
   ```

This will start the Kafka broker, the custom producer, and the Spark consumer.

## Components

### Kafka Producer

The Kafka producer (`kafka_producer/producer.py`) generates simulated view log data and sends it to a Kafka topic. It creates records with the following schema:

- `view_id`: Unique identifier for each view
- `start_timestamp`: Timestamp when the view started
- `end_timestamp`: Timestamp when the view ended
- `banner_id`: ID of the banner viewed
- `campaign_id`: ID of the campaign associated with the banner

### Spark Consumer

The Spark consumer (`spark_kafka_consumer/spark_consumer.py`) reads the streaming data from Kafka, processes it, and writes the results to Parquet files. It performs the following operations:

1. Reads the streaming data from Kafka
2. Processes the data to compute average view duration and total view count per minute for each campaign
3. Joins the processed data with campaign information from a CSV file
4. Writes the results to Parquet files, partitioned by network_id and minute_timestamp

## Output

The processed data is written to Parquet files in the `data/output_data` directory. The files are partitioned by `network_id` and `minute_timestamp`.

## Monitoring and Debugging

- Kafka logs can be viewed using `docker-compose logs broker`
- Producer logs can be viewed using `docker-compose logs custom-producer`
- Consumer logs can be viewed using `docker-compose logs custom_consumer`

## Stopping the Project

To stop the project and remove the containers, use:

```
docker-compose down
```

## Customization

You can customize various aspects of the project by modifying the environment variables in the `.env` file or the configuration files (`config.py`) in both the producer and consumer directories.

## Notes

- This project is designed for demonstration purposes and may need additional configuration for production use.
- Ensure that you have sufficient system resources available when running the Spark consumer, as it may require significant memory and CPU.
