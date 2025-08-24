import argparse
import json
import random
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
import time
import ipaddress
import uuid

def create_request_message(id, source_ip, path="/login", username=None):
    """Create a request message with the specified format."""
    username = username or f'user{random.randint(1,5)}'
    return {
        'ID': id,
        'Request Data': {
            'Source IP': source_ip,
            'Method': 'POST',
            'Service Port': 8002,
            'Agent ID': 'agent_done',
            'Path': path,
            'Campaign ID': f'campaign-{random.randint(100, 999)}',
            'requested_details': {
                'Body': {
                    'username': username,
                    'password': '********'
                },
                'Query Params': {'src': 'web', 'session': str(uuid.uuid4())[:8]}
            }
        },
        'sevs': [random.randint(1,3)],
        'Timestamp': int(time.time() * 1000)
    }

def create_response_message(request_msg, status_code=401):
    """Create a response message based on the request."""
    response_msg = request_msg.copy()
    response_msg['Response Data'] = {
        'Response Data': {
            'status_code': status_code,
            'headers': {
                'Server': 'Werkzeug/3.0.4 Python/3.12.7',
                'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
                'Content-Type': 'text/html; charset=utf-8',
                'Content-Length': str(random.randint(20, 500)),
                'Connection': 'close'
            }
        }
    }
    return response_msg

def create_multi_attacker_scenario():
    """
    Create a scenario with multiple attackers targeting different users
    with overlapping time periods.
    """
    events = []
    base_time = int(time.time() * 1000)

    # Generate attacker IPs
    attacker1_ip = str(ipaddress.IPv4Address(random.randint(0x01000000, 0xCFFFFFFF)))
    attacker2_ip = str(ipaddress.IPv4Address(random.randint(0x01000000, 0xCFFFFFFF)))
    attacker3_ip = str(ipaddress.IPv4Address(random.randint(0x01000000, 0xCFFFFFFF)))

    # Legitimate IPs
    legitimate_ip1 = str(ipaddress.IPv4Address(random.randint(0x01000000, 0xCFFFFFFF)))
    legitimate_ip2 = str(ipaddress.IPv4Address(random.randint(0x01000000, 0xCFFFFFFF)))

    print(f"Attacker 1: {attacker1_ip} targeting user1")
    print(f"Attacker 2: {attacker2_ip} targeting user2")
    print(f"Attacker 3: {attacker3_ip} targeting user3")

    # Scenario 1: Attacker 1 - brute force against user1 with success
    for i in range(12):
        req_id = random.randint(1000000000, 9999999999)
        event_time = base_time + (i * random.randint(1, 2) * 1000)

        # Create request message
        request_msg = create_request_message(req_id, attacker1_ip, "/login", "user1")
        request_msg['Timestamp'] = event_time
        events.append((request_msg, event_time))

        # Create response message (last one succeeds)
        status_code = 200 if i == 11 else 401
        response_msg = create_response_message(request_msg, status_code)
        response_msg['Timestamp'] = event_time + 200  # 200ms later
        events.append((response_msg, event_time + 200))

    # Scenario 2: Attacker 2 - starts 5 seconds later, targeting user2 (all fail)
    start_offset = 5000  # 5 seconds
    for i in range(15):
        req_id = random.randint(1000000000, 9999999999)
        event_time = base_time + start_offset + (i * random.randint(1, 2) * 1000)

        # Create request message
        request_msg = create_request_message(req_id, attacker2_ip, "/login", "user2")
        request_msg['Timestamp'] = event_time
        events.append((request_msg, event_time))

        # Create failed response message
        response_msg = create_response_message(request_msg, 401)
        response_msg['Timestamp'] = event_time + 150  # 150ms later
        events.append((response_msg, event_time + 150))

    # Scenario 3: Attacker 3 - starts 10 seconds later, short burst against user3 with success
    start_offset = 10000  # 10 seconds
    for i in range(10):
        req_id = random.randint(1000000000, 9999999999)
        event_time = base_time + start_offset + (i * 1000)  # 1 second between attempts

        # Create request message
        request_msg = create_request_message(req_id, attacker3_ip, "/login", "user3")
        request_msg['Timestamp'] = event_time
        events.append((request_msg, event_time))

        # Create response message (last one succeeds)
        status_code = 200 if i == 9 else 401
        response_msg = create_response_message(request_msg, status_code)
        response_msg['Timestamp'] = event_time + 180  # 180ms later
        events.append((response_msg, event_time + 180))

    # Add some legitimate traffic
    for i in range(5):
        req_id = random.randint(1000000000, 9999999999)
        event_time = base_time + (i * random.randint(3, 7) * 1000)

        # Create request message
        request_msg = create_request_message(req_id, legitimate_ip1, "/login", f"legitimate_user{i+1}")
        request_msg['Timestamp'] = event_time
        events.append((request_msg, event_time))

        # Create response message (random success/failure)
        status_code = random.choice([200, 401])
        response_msg = create_response_message(request_msg, status_code)
        response_msg['Timestamp'] = event_time + 100  # 100ms later
        events.append((response_msg, event_time + 100))

    # Add some non-login traffic
    for i in range(5):
        req_id = random.randint(1000000000, 9999999999)
        event_time = base_time + (i * random.randint(2, 6) * 1000)
        random_path = f"/api/resource/{random.randint(1, 100)}"

        # Create request message
        request_msg = create_request_message(req_id, legitimate_ip2, random_path)
        request_msg['Timestamp'] = event_time
        events.append((request_msg, event_time))

        # Create response message (usually success)
        response_msg = create_response_message(request_msg, 200)
        response_msg['Timestamp'] = event_time + 120  # 120ms later
        events.append((response_msg, event_time + 120))

    # Sort all events by timestamp
    events.sort(key=lambda x: x[1])
    return events

def create_topic_if_not_exists(bootstrap_servers, topic_name):
    """Create Kafka topic if it doesn't exist."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            api_version=(3, 7, 1)  # MSK Kafka Version
        )
        topic_list = admin_client.list_topics()
        if topic_name not in topic_list:
            topic = NewTopic(
                name=topic_name,
                num_partitions=4,
                replication_factor=2
            )
            try:
                admin_client.create_topics([topic])
                print(f"Topic '{topic_name}' created successfully.")
            except TopicAlreadyExistsError:
                print(f"Topic '{topic_name}' already exists.")
            except KafkaError as e:
                print(f"Failed to create topic '{topic_name}': {e}")
        else:
            print(f"Topic '{topic_name}' already exists.")
    finally:
        admin_client.close()

def send_messages(bootstrap_servers, topic):
    """Send login events to Kafka topic."""
    try:
        # Ensure topic exists
        create_topic_if_not_exists(bootstrap_servers, topic)

        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(3, 7, 1)  # MSK Kafka Version
        )

        # Generate multi-attacker scenario
        print("\nSimulating multi-attacker brute force scenario with request-response pairs")
        events = create_multi_attacker_scenario()

        # Track IPs and users for reporting
        ip_user_map = {}

        # Send all events with their timestamps
        start_time = events[0][1]
        for idx, (message, event_time) in enumerate(events):
            # Send message
            future = producer.send(topic, message)
            record_metadata = future.get(timeout=10)

            # Track IP and user if it's a request
            if 'Request Data' in message and 'Response Data' not in message:
                source_ip = message['Request Data']['Source IP']
                username = message['Request Data']['requested_details']['Body'].get('username', 'unknown')
                path = message['Request Data']['Path']
                request_id = message['ID']

                if source_ip not in ip_user_map:
                    ip_user_map[source_ip] = set()
                ip_user_map[source_ip].add(username)

                # Calculate elapsed time from first event
                elapsed_seconds = (event_time - start_time) / 1000

                print(f"Event {idx+1:>3} - [REQUEST] ID: {request_id} {source_ip:>15} -> {path:<12} user: {username:<15} +{elapsed_seconds:>5.1f}s")

            # If it's a response, print the status
            elif 'Response Data' in message:
                source_ip = message['Request Data']['Source IP']
                status_code = message['Response Data']['Response Data']['status_code']
                request_id = message['ID']

                elapsed_seconds = (event_time - start_time) / 1000
                print(f"Event {idx+1:>3} - [RESPONSE] ID: {request_id} {source_ip:>15} -> Status: {status_code} +{elapsed_seconds:>5.1f}s")

            # Small delay between sends to avoid overwhelming the producer
            time.sleep(0.1)

        print("\nBrute force scenario complete. Events sent to Kafka.")
        print("\nSummary:")
        for ip, users in ip_user_map.items():
            print(f"IP {ip:>15} targeted users: {', '.join(users)}")

    except Exception as e:
        print(f"Error sending messages: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        producer.flush()
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send login events to simulate brute force attacks.')
    parser.add_argument('topic', type=str, help='Kafka topic name')
    parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_servers.split(',')
    send_messages(bootstrap_servers, args.topic)