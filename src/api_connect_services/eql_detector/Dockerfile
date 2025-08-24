FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY src/api_connect_services/eql_detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for logs and config
RUN mkdir -p /app/logs /app/config

# Copy application code
COPY src/api_connect_services/eql_detector/eql_detector.py .

# Set up volumes for config and logs
VOLUME ["/app/config", "/app/logs"]

# Run the service
CMD ["python", "eql_detector.py", "--config", "/app/config/config.yaml"]