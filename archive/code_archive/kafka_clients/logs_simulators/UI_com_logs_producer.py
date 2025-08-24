import argparse
import json
import random
import time
import csv
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer that simulates events_logs_data with VIN numbers.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

# Split the bootstrap_servers string into a list
bootstrap_servers = args.bootstrap_servers.split(',')

# Load VIN numbers from CSV file
def load_vin_numbers(csv_file):
    vin_numbers = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip the header row
        for row in reader:
            if len(row) > 1:
                vin_numbers.append(row[1])  # VIN is in the second column
            else:
                print(f"Skipping row with insufficient columns: {row}")
    return vin_numbers

# Generate random DataEntry
def generate_random_data_entry():
    data_options = [
        ("101", "Unsigned Short Int", random.randint(0, 100)),
        ("102", "Double", random.randint(-10, 60)),
        ("103", "Unsigned Short Int", random.randint(0, 200))
    ]

    data_name, data_type, data_value = random.choice(data_options)

    return {
        "DataName": data_name,
        "DataType": data_type,
        "DataValue": str(data_value),
        "TimeStamp": str(int(time.time() * 1000))  # Current time in milliseconds
    }

# Generate Campaign data with multiple DataEntries
def generate_campaign_data(vin_number):
    num_entries = random.randint(50, 100)  # Random number of entries between 50 and 100
    data_entries = [generate_random_data_entry() for _ in range(num_entries)]
    return {
        "Campaign_ID": f"Campaign_ID_{random.randint(1, 10)}",  # Random Campaign ID
        "DataEntries": data_entries,
        "VinNumber": vin_number
    }

# Kafka topic creation function
def create_topic_if_not_exists(admin_client, topic_name):
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

# Kafka admin and producer setup
def setup_kafka_producer(bootstrap_servers):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1)  # MSK Kafka Version
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1)
    )

    return admin_client, producer

# Main function to produce messages to Kafka
def produce_messages(topic, bootstrap_servers):
    admin_client, producer = setup_kafka_producer(bootstrap_servers)
    create_topic_if_not_exists(admin_client, topic)

    # CSV file path
    csv_file = './car_models_vin.csv'
    vin_numbers = load_vin_numbers(csv_file)

    # Initialize index to cycle through VIN numbers
    vin_index = 0

    # Track total count of DataEntries
    total_data_entries_count = 0

    # New counter for total messages sent to Kafka
    total_messages_sent = 0

    while True:
        if not vin_numbers:
            print("No VIN numbers loaded. Exiting.")
            break

        print(f"VIN index: {vin_index}, Total VINs: {len(vin_numbers)}")

        # Generate campaign data
        campaign_data = generate_campaign_data(vin_numbers[vin_index])
        data_entries_count = len(campaign_data["DataEntries"])
        total_data_entries_count += data_entries_count

        # Prepare and send the message
        campaign_message = json.dumps(campaign_data).encode('utf-8')
        try:
            future_campaign = producer.send(topic, campaign_message)
            record_metadata = future_campaign.get(timeout=10)
            total_messages_sent += 1  # Increment the total messages sent counter
            print(f'Campaign Message delivered to {record_metadata.topic} [{record_metadata.partition}]')
            print(f"Count of DataEntries: {data_entries_count}")
            print(f"Total DataEntries Count: {total_data_entries_count}")
            print(f"Total Messages Sent to Kafka: {total_messages_sent}")  # Print total messages sent
            print("=================================================================================")
        except KafkaError as e:
            print(f'Campaign Message delivery failed: {e}')

        vin_index = (vin_index + 1) % len(vin_numbers)  # Cycle through VIN numbers

        time.sleep(random.uniform(1, 3))  # Random delay between 1 and 3 seconds

# Run the producer only when the script is executed directly
if __name__ == "__main__":
    produce_messages(args.topic, bootstrap_servers)