#!/usr/bin/env python3
"""
Generate test data for EQL Detector testing
"""
import argparse
import json
import time
import random
from datetime import datetime, timedelta
import requests

def generate_brute_force_data(es_host, es_port, index_name, source_ip=None):
    """Generate data simulating a brute force attack"""
    if not source_ip:
        source_ip = f"192.168.1.{random.randint(10, 254)}"
    
    now = datetime.now()
    
    # Failed login attempts
    failed_attempts = []
    for i in range(12):  # 12 attempts to ensure we trigger the 10-attempt threshold
        timestamp = (now - timedelta(seconds=30-i*2)).isoformat()
        failed_attempts.append({
            "request.source_ip": source_ip,
            "request.path": "/login",
            "is_response": False,
            "timestamp": timestamp
        })
    
    # Successful login
    success_attempt = {
        "request.source_ip": source_ip,
        "request.path": "/login",
        "response.status_code": 200,
        "is_response": True,
        "timestamp": now.isoformat()
    }
    
    # Create bulk data
    bulk_data = []
    for attempt in failed_attempts + [success_attempt]:
        bulk_data.append({"index": {"_index": index_name}})
        bulk_data.append(attempt)
    
    # Send to Elasticsearch
    bulk_body = "\n".join([json.dumps(item) for item in bulk_data]) + "\n"
    response = requests.post(
        f"http://{es_host}:{es_port}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=bulk_body
    )
    
    if response.status_code >= 200 and response.status_code < 300:
        print(f"✅ Inserted brute force test data from IP {source_ip}")
    else:
        print(f"❌ Error inserting data: {response.text}")
    
    return source_ip

def generate_sql_injection_data(es_host, es_port, index_name, source_ip=None):
    """Generate data simulating SQL injection attempts"""
    if not source_ip:
        source_ip = f"192.168.1.{random.randint(10, 254)}"
    
    now = datetime.now()
    
    # SQL injection payloads
    sql_injections = [
        {"path": "/api/search", "param": "query", "value": "' OR 1=1 --"},
        {"path": "/api/users", "param": "id", "value": "'; DROP TABLE users; --"},
        {"path": "/api/products", "param": "category", "value": "' UNION SELECT username,password FROM users --"}
    ]
    
    # Create events
    bulk_data = []
    for i, injection in enumerate(sql_injections):
        timestamp = (now - timedelta(seconds=10-i*3)).isoformat()
        event = {
            "request.source_ip": source_ip,
            "request.path": injection["path"],
            "is_response": False,
            "timestamp": timestamp,
            "request.query_params": {
                injection["param"]: injection["value"]
            }
        }
        
        bulk_data.append({"index": {"_index": index_name}})
        bulk_data.append(event)
    
    # Send to Elasticsearch
    bulk_body = "\n".join([json.dumps(item) for item in bulk_data]) + "\n"
    response = requests.post(
        f"http://{es_host}:{es_port}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=bulk_body
    )
    
    if response.status_code >= 200 and response.status_code < 300:
        print(f"✅ Inserted SQL injection test data from IP {source_ip}")
    else:
        print(f"❌ Error inserting data: {response.text}")
    
    return source_ip

def ensure_index_exists(es_host, es_port, index_name):
    """Ensure the index exists and has the right mappings"""
    # Check if index exists
    response = requests.head(f"http://{es_host}:{es_port}/{index_name}")
    
    if response.status_code == 200:
        print(f"Index {index_name} already exists")
        return True
    
    # Create the index with appropriate mappings
    mappings = {
        "mappings": {
            "properties": {
                "request.source_ip": {"type": "ip"},
                "request.path": {"type": "keyword"},
                "request.query_params": {"type": "object"},
                "request.body": {"type": "object"},
                "response.status_code": {"type": "integer"},
                "is_response": {"type": "boolean"},
                "timestamp": {"type": "date"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    
    response = requests.put(
        f"http://{es_host}:{es_port}/{index_name}",
        headers={"Content-Type": "application/json"},
        data=json.dumps(mappings)
    )
    
    if response.status_code >= 200 and response.status_code < 300:
        print(f"✅ Created index {index_name}")
        return True
    else:
        print(f"❌ Error creating index: {response.text}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Generate test data for EQL Detector")
    parser.add_argument("--host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--port", default="9002", help="Elasticsearch port")
    parser.add_argument("--index", default="api_logs", help="Index name")
    parser.add_argument("--type", choices=["brute-force", "sql-injection", "all"], default="all", 
                        help="Type of test data to generate")
    
    args = parser.parse_args()
    
    # Ensure the index exists
    if not ensure_index_exists(args.host, args.port, args.index):
        return
    
    # Generate test data based on type
    if args.type in ["brute-force", "all"]:
        ip = generate_brute_force_data(args.host, args.port, args.index)
        print(f"Generated brute force attack from {ip}")
    
    if args.type in ["sql-injection", "all"]:
        ip = generate_sql_injection_data(args.host, args.port, args.index)
        print(f"Generated SQL injection attempts from {ip}")
    
    print("\nTest data generation complete!")
    print("Run the EQL detector to see if it detects these events.")
    print("Check the Elasticsearch index for created security events.")

if __name__ == "__main__":
    main()