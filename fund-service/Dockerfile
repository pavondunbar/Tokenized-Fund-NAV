FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir asyncpg aiohttp

COPY rbac.py .
COPY fund_service.py .

CMD ["python", "fund_service.py"]
