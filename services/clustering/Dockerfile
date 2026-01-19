FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for spaCy
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy shared code first (for better caching)
COPY shared /app/shared

# Copy service code
COPY services/clustering /app

# Ensure shared is in Python path
ENV PYTHONPATH=/app/shared:$PYTHONPATH

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Download spaCy models
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download en_core_web_md

# Run service
CMD ["python", "main.py"]
