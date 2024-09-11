FROM python:3.8-slim AS base

RUN apt-get update && apt-get install curl -y
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy resources
# COPY producer.py .
# COPY utils.py .
# COPY config.py .
# COPY __init__.py /app/kafka_producer

COPY src/ ./src/
COPY tests/ ./tests/
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh

ENV PYTHONPATH="${PYTHONPATH}:/app/src"

FROM base AS test
WORKDIR /app
RUN pip install pytest==8.3.2
CMD ["pytest", "/app/tests"]

FROM base AS production
WORKDIR /app
CMD ["./wait-for-kafka.sh", "broker", "9092","python","./src/producer.py"]
