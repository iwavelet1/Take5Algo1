FROM python:3.11-slim

WORKDIR /app

# Install Kafka client
RUN pip install confluent-kafka==2.3.0

# Copy application code
COPY . .

# Expose port for health checks
EXPOSE 8080

CMD ["python", "main.py"]
