FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir asyncpg

COPY reconciler.py .

CMD ["python", "reconciler.py"]
