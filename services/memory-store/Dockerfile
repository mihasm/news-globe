FROM python:3.11-slim

WORKDIR /app

# Copy service code
COPY services/memory-store /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 6379

# Run service
CMD ["python", "main.py"]
