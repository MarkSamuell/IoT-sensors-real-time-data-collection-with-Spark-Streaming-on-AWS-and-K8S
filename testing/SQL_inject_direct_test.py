#!/usr/bin/env python3
"""
Elasticsearch SQL Injection Attack Simulator

This script simulates SQL injection attack attempts by sending data directly to Elasticsearch.
It will create documents with SQL injection patterns in query parameters and request body.
"""

import json
import time
import uuid
import random
import argparse
import datetime
import requests
from typing import List, Dict, Any

def generate_timestamp() -> str:
    """Generate ISO timestamp"""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def generate_request_id() -> int:
    """Generate a unique request ID"""
    return random.randint(100000, 999999)

def create_sql_injection_request(
    source_ip: str, 
    injection_pattern: str,
    endpoint: str = "/api/search",
    in_query: bool = True
) -> Dict[str, Any]:
    """
    Create a simulated SQL injection request document for Elasticsearch
    
    Args:
        source_ip: The IP address to use as the source
        injection_pattern: The SQL injection pattern to use
        endpoint: The API endpoint to target
        in_query: Whether to put the injection in query params (True) or body (False)
    
    Returns:
        Dict containing the document to be sent to Elasticsearch
    """
    request_id = generate_request_id()
    current_timestamp = generate_timestamp()
    
    # Randomly determine if this will be blocked (typical WAF behavior)
    is_blocked = random.choice([True, False])
    status_code = 403 if is_blocked else random.choice([200, 500])
    
    document = {
        "request_id": request_id,
        "timestamp": current_timestamp,
        "ingestion_time": current_timestamp,
        "has_response": True,
        "is_response": False,
        "request": {
            "method": random.choice(["GET", "POST"]),
            "path": endpoint,
            "source_ip": source_ip,
            "service_port": 443,
            "campaign_id": "test-campaign",
            "agent_id": f"test-agent-{uuid.uuid4().hex[:8]}"
        },
        "response": {
            "status_code": status_code,
            "headers": {
                "Content-Type": "application/json",
                "Connection": "keep-alive",
                "Content-Length": str(random.randint(20, 100)),
                "Date": current_timestamp,
                "Server": "nginx/1.18.0"
            }
        },
        "sevs": 0  # No SEVs initially
    }
    
    # Add injection pattern to either query_params or body
    if in_query:
        document["request"]["query_params"] = {
            "query": injection_pattern,
            "category": "products" if random.random() > 0.5 else "users",
            "id": str(random.randint(1, 100))
        }
        document["request"]["query_string"] = f"query={injection_pattern}&category=products&id=1"
    else:
        document["request"]["body"] = {
            "searchTerm": injection_pattern,
            "filters": {
                "category": "products" if random.random() > 0.5 else "users"
            }
        }
    
    return document

def simulate_sql_injection(
    es_host: str,
    es_port: int,
    index_name: str,
    source_ip: str,
    attack_count: int = 5,
    time_between_requests: float = 1.0,
    es_username: str = None,
    es_password: str = None
) -> None:
    """
    Simulate SQL injection attacks by sending requests with injection patterns
    
    Args:
        es_host: Elasticsearch host
        es_port: Elasticsearch port
        index_name: Target index name
        source_ip: Source IP for the attack
        attack_count: Number of attack attempts
        time_between_requests: Seconds to wait between requests
        es_username: Optional Elasticsearch username for authentication
        es_password: Optional Elasticsearch password for authentication
    """
    base_url = f"http://{es_host}:{es_port}"
    auth = (es_username, es_password) if es_username and es_password else None
    
    # SQL injection patterns to test
    sql_injection_patterns = [
        "' OR 1=1 --",
        "' OR '1'='1",
        "'; DROP TABLE users; --",
        "' UNION SELECT username, password FROM users --",
        "admin' --",
        "1' OR '1' = '1'",
        "1'; SELECT * FROM information_schema.tables; --",
        "' OR 1=1 LIMIT 1; --",
        "' OR 'x'='x",
        "'; exec master..xp_cmdshell 'ping 192.168.1.1'; --"
    ]
    
    # API endpoints that might be vulnerable
    endpoints = [
        "/api/search",
        "/api/users",
        "/api/products",
        "/api/query",
        "/api/data"
    ]
    
    # Check if index exists
    try:
        response = requests.head(
            f"{base_url}/{index_name}",
            auth=auth
        )
        
        if response.status_code != 200:
            print(f"Warning: Index '{index_name}' does not exist. Creating it...")
            # Create index with basic mapping
            mapping = {
                "mappings": {
                    "properties": {
                        "request": {
                            "properties": {
                                "source_ip": {"type": "keyword"},
                                "path": {"type": "keyword"},
                                "method": {"type": "keyword"},
                                "query_params": {"type": "object"},
                                "query_string": {"type": "text"},
                                "body": {"type": "object"}
                            }
                        },
                        "response": {
                            "properties": {
                                "status_code": {"type": "integer"},
                                "headers": {"type": "object"}
                            }
                        },
                        "timestamp": {"type": "date"},
                        "ingestion_time": {"type": "date"},
                        "request_id": {"type": "long"}
                    }
                }
            }
            
            response = requests.put(
                f"{base_url}/{index_name}",
                json=mapping,
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code >= 400:
                print(f"Failed to create index: {response.text}")
                return
    except requests.exceptions.RequestException as e:
        print(f"Error checking/creating index: {e}")
        return
    
    print(f"Starting SQL injection simulation against {es_host}:{es_port}/{index_name}")
    print(f"Source IP: {source_ip}")
    print(f"Sending {attack_count} SQL injection attempts")
    
    # Generate and send SQL injection requests
    for i in range(attack_count):
        # Choose a random pattern, endpoint, and placement (query or body)
        pattern = random.choice(sql_injection_patterns)
        endpoint = random.choice(endpoints)
        in_query = random.choice([True, False])
        
        doc = create_sql_injection_request(
            source_ip=source_ip,
            injection_pattern=pattern,
            endpoint=endpoint,
            in_query=in_query
        )
        
        try:
            response = requests.post(
                f"{base_url}/{index_name}/_doc",
                json=doc,
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                location = "query" if in_query else "body"
                print(f"Sent SQL injection attempt {i+1}/{attack_count}: '{pattern}' in {location} to {endpoint}")
            else:
                print(f"Failed to send document {i+1}: {response.text}")
                
            # Sleep between requests to spread them out
            if i < attack_count - 1:
                time.sleep(time_between_requests)
                
        except requests.exceptions.RequestException as e:
            print(f"Error sending SQL injection attempt {i+1}: {e}")
    
    print("\nSQL injection attack simulation completed!")
    print(f"Check your EQL detector service for alerts with source IP: {source_ip}")

def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(description='Elasticsearch SQL Injection Attack Simulator')
    parser.add_argument('--host', default='localhost', help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--index', default='api_logs', help='Target index name')
    parser.add_argument('--ip', default='192.168.1.200', help='Source IP for the attack')
    parser.add_argument('--count', type=int, default=5, help='Number of attack attempts')
    parser.add_argument('--delay', type=float, default=0.5, help='Seconds between requests')
    parser.add_argument('--es-username', help='Elasticsearch username')
    parser.add_argument('--es-password', help='Elasticsearch password')
    
    args = parser.parse_args()
    
    simulate_sql_injection(
        es_host=args.host,
        es_port=args.port,
        index_name=args.index,
        source_ip=args.ip,
        attack_count=args.count,
        time_between_requests=args.delay,
        es_username=args.es_username,
        es_password=args.es_password
    )

if __name__ == "__main__":
    main()