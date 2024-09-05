FROM python:3.9-slim-bullseye

# Install OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy resources
COPY spark_consumer.py .
# COPY utils.py .
COPY config.py .
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh
# COPY wait-for-it.sh wait-for-it.sh
# RUN echo "host.docker.internal host-gateway" >> /etc/hosts

CMD ["./wait-for-kafka.sh", "broker", "9092","python","spark_consumer.py"]
