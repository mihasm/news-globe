FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared modules
COPY shared/ ./shared/

# Copy connectors
COPY connectors/ ./connectors/

# Copy service code
COPY services/supervisor/ ./

# Create state directory
RUN mkdir -p /app/state

# Run supervisor
CMD ["python", "supervisor.py"]