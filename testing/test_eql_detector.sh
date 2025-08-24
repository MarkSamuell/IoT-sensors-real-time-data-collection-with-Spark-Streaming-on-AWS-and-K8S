#!/bin/bash
# Script to test EQL Detector with Docker connecting to remote Elasticsearch via SSH tunnel (PuTTY)

# Ensure SSH tunnel is active, adjust credentials and ports as needed
if ! curl -s --head http://localhost:9002 | grep "200 OK" > /dev/null; then
  echo "SSH tunnel to Elasticsearch doesn't seem to be active on port 9002."
  echo "Please ensure PuTTY is running with an SSH tunnel configured:"
  echo "  Source port: 9002"
  echo "  Destination: elasticsearch-host:9200"
  exit 1
fi

# Ensure required directories exist
mkdir -p logs

# Change to the eql_detector directory for docker-compose
cd src/api_connect_services/eql_detector || exit

# Start Docker Compose environment
echo "Starting EQL Detector container..."
docker-compose up -d

# Change back to root directory
cd ../../../ || exit

# Add test data to remote Elasticsearch
echo "Adding test data to Elasticsearch..."
python3 - << 'EOF'
import requests
import json
import time

ES_HOST = "127.0.0.1"
ES_PORT = "9002"
INDEX_NAME = "api_logs"
SEVS_INDEX = "api_sevs"

# Function to create an index if it doesn't exist
def ensure_index_exists(index_name, mappings):
    response = requests.head(f"http://{ES_HOST}:{ES_PORT}/{index_name}")
    if response.status_code != 200:
        requests.put(
            f"http://{ES_HOST}:{ES_PORT}/{index_name}",
            headers={"Content-Type": "application/json"},
            json={"mappings": mappings}
        )
        print(f"Created index {index_name}")

# Index mappings
api_logs_mappings = {
    "properties": {
        "request.source_ip": {"type": "ip"},
        "request.path": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "is_response": {"type": "boolean"},
        "response.status_code": {"type": "integer"}
    }
}

api_sevs_mappings = {
    "properties": {
        "sev_id": {"type": "keyword"},
        "sev_name": {"type": "keyword"},
        "description": {"type": "text"},
        "severity": {"type": "keyword"},
        "type": {"type": "keyword"},
        "detection_time": {"type": "date"},
        "timestamp": {"type": "date"},
        "source_ip": {"type": "ip"},
        "iocs": {"type": "nested"}
    }
}

# Ensure indices exist
ensure_index_exists(INDEX_NAME, api_logs_mappings)
ensure_index_exists(SEVS_INDEX, api_sevs_mappings)

# Add brute force test data
bulk_data = []
for i in range(12):
    bulk_data.append({"index": {"_index": INDEX_NAME}})
    bulk_data.append({
        "request.source_ip": "192.168.1.100",
        "request.path": "/login",
        "is_response": False,
        "timestamp": f"2025-03-24T10:{i:02d}:00.000Z"
    })

# Add successful login
bulk_data.append({"index": {"_index": INDEX_NAME}})
bulk_data.append({
    "request.source_ip": "192.168.1.100",
    "request.path": "/login",
    "response.status_code": 200,
    "is_response": True,
    "timestamp": "2025-03-24T10:30:00.000Z"
})

# Add SQL injection test
bulk_data.append({"index": {"_index": INDEX_NAME}})
bulk_data.append({
    "request.source_ip": "192.168.1.101",
    "request.path": "/search?q=' OR 1=1 --",
    "is_response": False,
    "timestamp": "2025-03-24T11:00:00.000Z"
})

# Send data to Elasticsearch
bulk_body = "\n".join([json.dumps(item) for item in bulk_data]) + "\n"
response = requests.post(
    f"http://{ES_HOST}:{ES_PORT}/_bulk",
    headers={"Content-Type": "application/x-ndjson"},
    data=bulk_body
)

if response.status_code >= 200 and response.status_code < 300:
    print("Successfully inserted test data")
else:
    print(f"Error inserting data: {response.text}")

# Refresh index to make data immediately available
requests.post(f"http://{ES_HOST}:{ES_PORT}/{INDEX_NAME}/_refresh")
print("Index refreshed")
EOF

# Watch the logs
echo "Watching EQL Detector logs (Ctrl+C to exit)..."
docker logs -f eql-detector

# Instructions for cleanup
echo "To stop the environment, run: cd src/api_connect_services/eql_detector && docker-compose down"
