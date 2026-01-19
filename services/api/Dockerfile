FROM python:3.11-slim

WORKDIR /app

# Copy shared code first (for better caching)
COPY shared /app/shared

# Copy .env file for configuration
COPY .env /app/.env

# Copy service code
COPY services/api /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8080

# Run service
CMD ["python", "app.py"]
