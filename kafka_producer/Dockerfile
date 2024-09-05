FROM python:3.8-slim

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

CMD ["./wait-for-kafka.sh", "broker", "9092","python","producer.py"]
