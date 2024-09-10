FROM python:3.8-slim as base

RUN apt-get update && apt-get install curl -y
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy resources
COPY producer.py .
COPY utils.py .
COPY config.py .
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh
# COPY wait-for-it.sh wait-for-it.sh
# RUN echo "host.docker.internal host-gateway" >> /etc/hosts

FROM base as test
RUN pip install pytest pytest-asyncio
CMD ["pytest", "tests"]

FROM base as production
CMD ["./wait-for-kafka.sh", "broker", "9092","python","producer.py"]
