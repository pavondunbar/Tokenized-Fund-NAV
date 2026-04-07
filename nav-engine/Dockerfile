FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp

COPY nav_engine.py .

CMD ["python", "nav_engine.py"]
