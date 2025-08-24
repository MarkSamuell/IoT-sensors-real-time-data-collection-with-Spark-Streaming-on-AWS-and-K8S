import argparse
import json
import random
import time
import csv
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# Late events configuration
LATE_EVENTS_CONFIG = {
    'LATE_EVENTS_RATIO': 0.2,       # 20% of events will be late
    'MIN_HOURS_LATE': 2,            # Minimum hours in the past
    'MAX_HOURS_LATE': 4,            # Maximum hours in the past
    'INCLUDE_DAYS_LATE': False,      # Use days instead of hours for late events
    'MIN_DAYS_LATE': 1,            # Minimum days in the past
    'MAX_DAYS_LATE': 7             # Maximum days in the past
}

# Argument parsing for Kafka configuration
parser = argparse.ArgumentParser(description='Kafka producer that simulates events_logs_data with VIN numbers.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

# Split the bootstrap_servers string into a list
bootstrap_servers = args.bootstrap_servers.split(',')

def load_vin_numbers(csv_file):
    """Load VIN numbers from CSV file"""
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

def generate_timestamp(is_late: bool = False) -> int:
    """
    Generate timestamp, either current or late based on configuration.
    
    Args:
        is_late: Whether to generate a late timestamp
        
    Returns:
        int: Timestamp in milliseconds
    """
    current_time = datetime.now()
    
    if is_late:
        if LATE_EVENTS_CONFIG['INCLUDE_DAYS_LATE']:
            # Generate timestamp days late
            days_late = random.randint(
                LATE_EVENTS_CONFIG['MIN_DAYS_LATE'],
                LATE_EVENTS_CONFIG['MAX_DAYS_LATE']
            )
            time_delta = timedelta(
                days=days_late,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
        else:
            # Generate timestamp hours late
            hours_late = random.randint(
                LATE_EVENTS_CONFIG['MIN_HOURS_LATE'],
                LATE_EVENTS_CONFIG['MAX_HOURS_LATE']
            )
            time_delta = timedelta(
                hours=hours_late,
                minutes=random.randint(0, 59)
            )
        
        timestamp = current_time - time_delta
    else:
        timestamp = current_time
    
    return int(timestamp.timestamp() * 1000)  # Convert to milliseconds

def generate_random_data_entry(is_late: bool = False):
    """Generate random DataEntry with optional late timestamp"""
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
        "TimeStamp": str(generate_timestamp(is_late))
    }

def generate_campaign_data(vin_number):
    """Generate Campaign data with mix of current and late events"""
    num_entries = random.randint(50, 100)  # Random number of entries between 50 and 100
    data_entries = []
    
    for _ in range(num_entries):
        # Determine if this entry should be late based on configured ratio
        is_late = random.random() < LATE_EVENTS_CONFIG['LATE_EVENTS_RATIO']
        data_entries.append(generate_random_data_entry(is_late))
    
    return {
        "Campaign_ID": f"Campaign_ID_{random.randint(1, 10)}",
        "DataEntries": data_entries,
        "VinNumber": vin_number
    }

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
        api_version=(3, 7, 1)
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

    csv_file = './car_models_vin.csv'
    vin_numbers = load_vin_numbers(csv_file)
    
    if not vin_numbers:
        print("No VIN numbers loaded. Exiting.")
        return

    vin_index = 0
    total_data_entries_count = 0
    total_messages_sent = 0
    late_entries_count = 0

    print("\nProducing messages with the following configuration:")
    print(f"Late events ratio: {LATE_EVENTS_CONFIG['LATE_EVENTS_RATIO'] * 100}%")
    if LATE_EVENTS_CONFIG['INCLUDE_DAYS_LATE']:
        print(f"Late events will be {LATE_EVENTS_CONFIG['MIN_DAYS_LATE']} to "
              f"{LATE_EVENTS_CONFIG['MAX_DAYS_LATE']} days late")
    else:
        print(f"Late events will be {LATE_EVENTS_CONFIG['MIN_HOURS_LATE']} to "
              f"{LATE_EVENTS_CONFIG['MAX_HOURS_LATE']} hours late")
    print("=" * 80)

    while True:
        # Generate campaign data
        campaign_data = generate_campaign_data(vin_numbers[vin_index])
        data_entries_count = len(campaign_data["DataEntries"])
        total_data_entries_count += data_entries_count

        # Count late entries in this batch
        min_time = int(time.time() * 1000)
        if LATE_EVENTS_CONFIG['INCLUDE_DAYS_LATE']:
            min_time -= LATE_EVENTS_CONFIG['MIN_DAYS_LATE'] * 24 * 3600 * 1000
        else:
            min_time -= LATE_EVENTS_CONFIG['MIN_HOURS_LATE'] * 3600 * 1000

        late_entries = sum(
            1 for entry in campaign_data["DataEntries"]
            if int(entry["TimeStamp"]) < min_time
        )
        late_entries_count += late_entries

        # Send message to Kafka
        try:
            campaign_message = json.dumps(campaign_data).encode('utf-8')
            future_campaign = producer.send(topic, campaign_message)
            record_metadata = future_campaign.get(timeout=10)
            total_messages_sent += 1

            print(f"\nMessage delivered to {record_metadata.topic} [{record_metadata.partition}]")
            print(f"Total DataEntries in this message: {data_entries_count}")
            print(f"Late entries in this message: {late_entries}")
            print(f"Total messages sent: {total_messages_sent}")
            print(f"Total DataEntries sent: {total_data_entries_count}")
            print(f"Total late entries sent: {late_entries_count}")
            print(f"Current late ratio: {(late_entries_count/total_data_entries_count)*100:.2f}%")
            print("=" * 80)

        except KafkaError as e:
            print(f'Message delivery failed: {e}')

        vin_index = (vin_index + 1) % len(vin_numbers)
        time.sleep(random.uniform(1, 3))

if __name__ == "__main__":
    produce_messages(args.topic, bootstrap_servers)