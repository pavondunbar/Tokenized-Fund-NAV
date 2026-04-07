FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir asyncpg aiokafka aiohttp

COPY consumer.py .

CMD ["python", "consumer.py"]
