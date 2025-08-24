import argparse
import json
import time
import csv
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
import pandas as pd
from datetime import datetime

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer for CAN data from CSV file')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

# Split the bootstrap_servers string into a list
bootstrap_servers = args.bootstrap_servers.split(',')

def load_can_data(csv_file):
    """Load CAN data from CSV file"""
    try:
        df = pd.read_csv(csv_file)
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        print(f"Loaded {len(records)} records from CSV file")
        return records
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return []

def create_topic_if_not_exists(admin_client, topic_name):
    """Create Kafka topic if it doesn't exist"""
    topic_list = admin_client.list_topics()
    if topic_name not in topic_list:
        topic = NewTopic(name=topic_name, num_partitions=2, replication_factor=2)
        try:
            admin_client.create_topics([topic])
            print(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' already exists.")
        except KafkaError as e:
            print(f"Failed to create topic '{topic_name}': {e}")
    else:
        print(f"Topic '{topic_name}' already exists.")

def setup_kafka_producer(bootstrap_servers):
    """Setup Kafka admin client and producer"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 7, 1)  # MSK Kafka Version
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 7, 1)
    )

    return admin_client, producer

def produce_messages(topic, bootstrap_servers):
    """Main function to produce messages to Kafka"""
    admin_client, producer = setup_kafka_producer(bootstrap_servers)
    create_topic_if_not_exists(admin_client, topic)

    # Load CAN data from CSV
    records = load_can_data('./Fin_host_session_submit_S.csv')
    if not records:
        print("No records loaded. Exiting.")
        return

    record_index = 0
    total_messages_sent = 0

    while True:
        record = records[record_index]
        
        try:
            # Format the message
            message = {
                "Timestamp": str(record['Timestamp']),
                "Arbitration_ID": str(record['Arbitration_ID']),
                "DLC": str(record['DLC']),
                "Data": str(record['Data']),
                "Class": str(record.get('Class', '')),
                "SubClass": str(record.get('SubClass', ''))
            }

            # Send the message
            message_bytes = json.dumps(message).encode('utf-8')
            future = producer.send(topic, message_bytes)
            record_metadata = future.get(timeout=10)
            
            total_messages_sent += 1
            
            if total_messages_sent % 100 == 0:  # Print status every 100 messages
                print(f"Messages sent: {total_messages_sent}")
                print(f"Last message: {message}")
                print(f"Delivered to {record_metadata.topic} [{record_metadata.partition}]")
                print("=" * 80)

        except KafkaError as e:
            print(f'Message delivery failed: {e}')

        # Move to next record, cycle back to beginning if at end
        record_index = (record_index + 1) % len(records)
        
        # Add a small delay between messages
        time.sleep(0.1)  # 100ms delay between messages

def main():
    try:
        print(f"Starting CAN data producer to topic: {args.topic}")
        print(f"Using bootstrap servers: {args.bootstrap_servers}")
        produce_messages(args.topic, bootstrap_servers)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()