# Campaigns Data Processing Project

This project demonstrates a real-time data processing pipeline using Apache Kafka for data ingestion and Apache Spark for stream processing. It simulates a system that generates view logs for advertising campaigns, processes them in real-time, and stores the aggregated results in local file system as parquet format.

## Project Structure

- `kafka_producer/`: Contains the Kafka producer code
  - `src/`: source directory for Kafka producer
    - `config.py`: Configuration settings for the producer
    - `producer.py`: Generates and sends simulated view log data to Kafka
    - `utils.py`: Utility functions for the producer
  - `tests/`: Contains unit test files for producer
    - `test_generate_log_data.py`: test file for generate_log_data function
    - `test_producer.py`: test file for producer file
  - `Dockerfile`: Docker File for the producer
  - `requirements.txt`: Python dependencies for the producer
  - `wait-for-kafka.sh`: Shell script to wait for Kafka to be ready
- `log_date/`: Directory for the input and output data
  - `input_date/`: Contains campaigns.csv file
  - `output_date/`: Stores final data as parquet file
- `spark_kafka_consumer/`: Contains the Spark streaming consumer code
  - `src/`: source directory for Kafka consumer
    - `config.py`: Configuration settings for the consumer
    - `consumer.py`: Processes streaming data from Kafka using Spark
    - `utils.py`: Utility functions for the consumer
  - `tests/`: Contains unit test files for consumer
    - `test_consumer.py`: test file for producer file
  - `Dockerfile`: Docker File for the consumer
  - `requirements.txt`: Python dependencies for the consumer
  - `wait-for-kafka.sh`: Shell script to wait for Kafka to be ready
- `.env`: Variables to be used across the project
- `docker-compose.yml`: Defines and configures the services for the project
- `README.md`: Description about the project

## Prerequisites

- Docker
- Docker Compose

## Setup and Running

1. Create the repository:

2. Create the necessary directories if not present:

   ```
   mkdir -p log_data/input_data log_data/output_data
   ```

3. Place the `campaigns.csv` file in the `log_data/input_data` directory if not present.

4. Build and start the services:
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

The Spark consumer (`spark_kafka_consumer/src/consumer.py`) reads the streaming data from Kafka, processes it, and writes the results to Parquet files. It performs the following operations:

1. Reads the streaming data from Kafka
2. Processes the data to compute average view duration and total view count per minute for each campaign
3. Joins the processed data with campaign information from a CSV file
4. Writes the results to Parquet files, partitioned by network_id and minute_timestamp

## Output

The processed data is written to Parquet files in the `log_data/output_data` directory. The files are partitioned by `network_id` and `minute_timestamp`.

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

- This project is designed for dev purposes and may need additional configuration for production use.
- Ensure that you have sufficient system resources available when running the Spark consumer, as it may require significant memory and CPU.
