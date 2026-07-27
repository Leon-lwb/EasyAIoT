FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV KAFKA_BOOTSTRAP_SERVERS=kafka-server:9092 \
    KAFKA_TOPIC=iot-alert-notification \
    KAFKA_GROUP_ID=alert-forwarder-group \
    TARGET_URL=https://testcy.xqbg.net/testapi/api/TransparentKitchen/PutEventList \
    API_ID= \
    SECRET= \
    MINIO_PUBLIC_BASE= \
    MINIO_BUCKET=alert-images \
    REQUEST_TIMEOUT=30 \
    MAX_RETRIES=3

CMD ["python", "-u", "main.py"]
