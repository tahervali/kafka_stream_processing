FROM python:3.9-slim-bullseye AS base

# Install OpenJDK 17
RUN apt-get update && \
    apt-get install -y wget gzip openjdk-17-jdk build-essential software-properties-common libc6 cmake && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY tests/ ./tests/
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh
# COPY wait-for-it.sh wait-for-it.sh
# RUN echo "host.docker.internal host-gateway" >> /etc/hosts
ENV PYTHONPATH="${PYTHONPATH}:/app/src"


FROM base AS test
WORKDIR /app
RUN pip install pytest==8.3.2
CMD ["pytest", "/app/tests"]

FROM base AS production
WORKDIR /app
CMD ["./wait-for-kafka.sh", "broker", "9092","python","./src/spark_consumer.py"]
