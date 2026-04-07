FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp

COPY compliance.py .

CMD ["python", "compliance.py"]
