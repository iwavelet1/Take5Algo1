FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install all dependencies
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Set environment variable to indicate running in Docker
ENV RUN_MODE=docker

# Expose port for health checks
EXPOSE 8080

CMD ["python", "main.py"]
