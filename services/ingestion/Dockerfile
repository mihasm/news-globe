FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY services/ingestion /app

ENV PYTHONPATH=/app/shared:$PYTHONPATH

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "entrypoint.py"]