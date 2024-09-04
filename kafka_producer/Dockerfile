FROM python:3.8-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy resources
WORKDIR /
COPY producer.py .
# COPY wait-for-it.sh wait-for-it.sh
# RUN echo "host.docker.internal host-gateway" >> /etc/hosts

ENTRYPOINT ["python3","producer.py"]
