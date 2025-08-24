#!/usr/bin/env python3
"""
Elasticsearch Brute Force Attack Simulator

This script simulates a brute force login attack by sending data directly to Elasticsearch.
It will create a sequence of failed login attempts followed by a successful one.
"""

import json
import time
import uuid
import random
import argparse
import datetime
import requests
from typing import List, Dict, Any

def generate_timestamp(offset_seconds: int = 0) -> str:
    """Generate ISO timestamp with an optional offset in seconds"""
    dt = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=offset_seconds)
    return dt.isoformat()

def generate_request_id() -> int:
    """Generate a unique request ID"""
    return random.randint(100000, 999999)

def create_login_request(
    source_ip: str, 
    status_code: int, 
    timestamp_offset: int = 0,
    username: str = None
) -> Dict[str, Any]:
    """
    Create a simulated login request document for Elasticsearch
    
    Args:
        source_ip: The IP address to use as the source
        status_code: HTTP status code (401 for failed, 200 for successful)
        timestamp_offset: Seconds to offset from current time
        username: Optional username to include in the request body
    
    Returns:
        Dict containing the document to be sent to Elasticsearch
    """
    # Use provided username or generate a random one for failed attempts
    if not username and status_code != 200:
        username = f"user{random.randint(1, 1000)}"
    elif not username:
        username = "admin"  # Successful login usually has a valid username
    
    request_id = generate_request_id()
    current_timestamp = generate_timestamp(timestamp_offset)
    
    document = {
        "request_id": request_id,
        "timestamp": current_timestamp,
        "ingestion_time": current_timestamp,
        "has_response": True,
        "is_response": False,
        "request": {
            "method": "POST",
            "path": "/login",
            "source_ip": source_ip,
            "service_port": 443,
            "campaign_id": "test-campaign",
            "agent_id": f"test-agent-{uuid.uuid4().hex[:8]}",
            "body": {
                "username": username,
                "password": "****" if status_code == 200 else f"wrong_password_{random.randint(1, 100)}"
            }
        },
        "response": {
            "status_code": status_code,
            "headers": {
                "Content-Type": "application/json",
                "Connection": "keep-alive",
                "Content-Length": "25",
                "Date": current_timestamp,
                "Server": "nginx/1.18.0"
            }
        },
        "sevs": 0  # No SEVs initially
    }
    
    return document

def simulate_brute_force(
    es_host: str,
    es_port: int,
    index_name: str,
    source_ip: str,
    failed_attempts: int = 10,
    time_between_requests: float = 1.0,
    username: str = None,
    es_username: str = None,
    es_password: str = None
) -> None:
    """
    Simulate a brute force attack by sending multiple failed login attempts
    followed by a successful one to Elasticsearch
    
    Args:
        es_host: Elasticsearch host
        es_port: Elasticsearch port
        index_name: Target index name
        source_ip: Source IP for the attack
        failed_attempts: Number of failed login attempts
        time_between_requests: Seconds to wait between requests
        username: Username to use in the attack (random if not specified)
        es_username: Optional Elasticsearch username for authentication
        es_password: Optional Elasticsearch password for authentication
    """
    base_url = f"http://{es_host}:{es_port}"
    auth = (es_username, es_password) if es_username and es_password else None
    total_requests = failed_attempts + 1  # Include the final successful attempt
    
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
                                "body": {
                                    "properties": {
                                        "username": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                                        "password": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
                                    }
                                }
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
    
    print(f"Starting brute force simulation against {es_host}:{es_port}/{index_name}")
    print(f"Source IP: {source_ip}")
    print(f"Sending {failed_attempts} failed login attempts followed by 1 successful login")
    
    # Generate documents for failed attempts
    failed_docs = []
    for i in range(failed_attempts):
        offset = int(-30 + i * (30 / failed_attempts))  # Spread over last 30 seconds
        doc = create_login_request(
            source_ip=source_ip,
            status_code=401,
            timestamp_offset=offset,
            username=username
        )
        failed_docs.append(doc)
    
    # Generate successful login document
    success_doc = create_login_request(
        source_ip=source_ip,
        status_code=200,
        timestamp_offset=0,  # Current time
        username=username
    )
    
    # Send failed login attempts
    for i, doc in enumerate(failed_docs):
        try:
            response = requests.post(
                f"{base_url}/{index_name}/_doc",
                json=doc,
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                print(f"Sent failed login attempt {i+1}/{failed_attempts} (Status: {doc['response']['status_code']})")
            else:
                print(f"Failed to send document {i+1}: {response.text}")
                
            # Sleep between requests to spread them out
            if i < len(failed_docs) - 1:
                time.sleep(time_between_requests)
                
        except requests.exceptions.RequestException as e:
            print(f"Error sending failed login attempt {i+1}: {e}")
    
    # Send successful login attempt
    try:
        response = requests.post(
            f"{base_url}/{index_name}/_doc",
            json=success_doc,
            auth=auth,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Sent successful login attempt (Status: {success_doc['response']['status_code']})")
        else:
            print(f"Failed to send successful login document: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"Error sending successful login attempt: {e}")
    
    print("\nBrute force attack simulation completed!")
    print(f"Check your EQL detector service for alerts with source IP: {source_ip}")

def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(description='Elasticsearch Brute Force Attack Simulator')
    parser.add_argument('--host', default='localhost', help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--index', default='api_logs', help='Target index name')
    parser.add_argument('--ip', default='192.168.1.100', help='Source IP for the attack')
    parser.add_argument('--attempts', type=int, default=10, help='Number of failed login attempts')
    parser.add_argument('--delay', type=float, default=0.5, help='Seconds between requests')
    parser.add_argument('--username', help='Username to use (random if not specified)')
    parser.add_argument('--es-username', help='Elasticsearch username')
    parser.add_argument('--es-password', help='Elasticsearch password')
    
    args = parser.parse_args()
    
    simulate_brute_force(
        es_host=args.host,
        es_port=args.port,
        index_name=args.index,
        source_ip=args.ip,
        failed_attempts=args.attempts,
        time_between_requests=args.delay,
        username=args.username,
        es_username=args.es_username,
        es_password=args.es_password
    )

if __name__ == "__main__":
    main()