FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir asyncpg aiokafka

COPY outbox_publisher.py .

CMD ["python", "outbox_publisher.py"]
