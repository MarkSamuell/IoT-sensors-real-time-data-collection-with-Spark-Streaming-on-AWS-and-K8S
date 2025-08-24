import argparse
import json
import random
import time
import csv
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

parser = argparse.ArgumentParser(description='Kafka producer that simulates events_logs_data with VIN numbers.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

bootstrap_servers = args.bootstrap_servers.split(',')

def generate_message_key():
    return str(uuid.uuid4())

def load_vin_numbers(csv_file):
    vin_numbers = []
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
            for row in reader:
                if len(row) > 1:
                    vin_numbers.append(row[1])
                else:
                    print(f"Skipping row with insufficient columns: {row}")
    except FileNotFoundError:
        print(f"CSV file {csv_file} not found, using dummy data")
        return ["1HGCM82633A123456", "JH4KA7532NC123456", "WAUAC48H86K123456"]
    return vin_numbers

def load_business_ids(csv_file):
    business_ids = []
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
            business_id_index = header.index('business_id') if 'business_id' in header else 3
            for row in reader:
                if len(row) > business_id_index:
                    business_ids.append(row[business_id_index])
                else:
                    print(f"Skipping row with insufficient columns: {row}")
    except FileNotFoundError:
        print(f"CSV file {csv_file} not found, using dummy business_ids")
        return ["3eca77ff0718031656572b287facf0eb", "c04299cbe9186f44fddd707725ccca00", "30d2478c2d26f919396245b0bd340654"]
    return business_ids

def generate_random_data_entry(business_ids):
    data_types = ["Unsigned Short Int", "Double", "Short Int", "Boolean", "String"]
    
    data_name = random.choice(business_ids)
    data_type = random.choice(data_types)
    
    if data_type == "Boolean":
        data_value = random.choice([0, 1])
    elif data_type == "String":
        data_value = f"value_{random.randint(1, 100)}"
    elif data_type == "Double":
        data_value = random.randint(-10, 60000)
    else:
        data_value = random.randint(0, 20000)

    return {
        "DataName": data_name,
        "DataType": data_type,
        "DataValue": str(data_value),
        "TimeStamp": str(int(time.time() * 1000))
    }

def generate_campaign_data(vin_number, message_key, business_ids):
    num_entries = random.randint(50, 100)
    data_entries = [generate_random_data_entry(business_ids) for _ in range(num_entries)]
    return {
        "message_key": message_key,  # Include message key in payload for debugging
        "Campaign_ID": f"Campaign_ID_{random.randint(1, 10)}",
        "DataEntries": data_entries,
        "VinNumber": vin_number
    }

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

    csv_file = './car_models_vin.csv'
    signal_csv_file = './business_ids.csv'
    vin_numbers = load_vin_numbers(csv_file)
    business_ids = load_business_ids(signal_csv_file)
    vin_index = 0
    total_data_entries_count = 0
    total_messages_sent = 0

    # Send a test message first
    try:
        print(f"Sending test message to topic: {topic}")
        test_message = {
            "test": True,
            "timestamp": int(time.time() * 1000),
            "message": "Test message"
        }
        future = producer.send(topic, key="test-key", value=test_message)
        record_metadata = future.get(timeout=30)
        print(f"Test message delivered to {record_metadata.topic} [{record_metadata.partition}]")
        print("=" * 80)
    except Exception as e:
        print(f"Failed to send test message: {e}")
        print("Continuing with regular messages anyway...")

    try:
        while True:
            message_key = generate_message_key()
            campaign_data = generate_campaign_data(vin_numbers[vin_index], message_key, business_ids)
            data_entries_count = len(campaign_data["DataEntries"])
            total_data_entries_count += data_entries_count

            try:
                future = producer.send(
                    topic,
                    key=message_key,
                    value=campaign_data
                )
                record_metadata = future.get(timeout=30)
                total_messages_sent += 1

                print(f"Message Key: {message_key}")
                print(f'Delivered to {record_metadata.topic} [{record_metadata.partition}]')
                print(f"DataEntries Count: {data_entries_count}")
                print(f"Total DataEntries: {total_data_entries_count}")
                print(f"Total Messages: {total_messages_sent}")
                print("=" * 80)
            except KafkaError as e:
                print(f'Message delivery failed: {e}')

            vin_index = (vin_index + 1) % len(vin_numbers)
            time.sleep(random.uniform(1, 3))
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