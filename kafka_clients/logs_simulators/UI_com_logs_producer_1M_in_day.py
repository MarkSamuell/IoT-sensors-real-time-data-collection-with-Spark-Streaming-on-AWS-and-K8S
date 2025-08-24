import argparse
import json
import random
import csv
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('topic', type=str, help='Kafka topic name')
parser.add_argument('bootstrap_servers', type=str, help='Kafka bootstrap servers')
parser.add_argument('--target_date', type=str, default=datetime.now().strftime("%Y-%m-%d"))
parser.add_argument('--max_batch_size', type=int, default=10000)
args = parser.parse_args()

def create_topic_if_not_exists(admin_client, topic_name):
    topic_list = admin_client.list_topics()
    if topic_name not in topic_list:
        topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=2)
        try:
            admin_client.create_topics([topic])
            print(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' already exists.")
        except KafkaError as e:
            print(f"Failed to create topic '{topic_name}': {e}")
    else:
        print(f"Topic '{topic_name}' already exists.")

def load_vin_numbers(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        vin_numbers = [row[1] for row in reader if len(row) > 1]
    weights = np.random.pareto(a=1.5, size=len(vin_numbers))
    return vin_numbers, (weights / weights.sum()).tolist()

def generate_data_entry(timestamp):
    data_options = [
        ("101", "Unsigned Short Int", random.randint(0, 10000000)),
        ("102", "Double", random.randint(-100000, 6000000)),
        ("103", "Unsigned Short Int", random.randint(0, 20000000))
    ]
    name, type_, value = random.choice(data_options)
    return {
        "DataName": name,
        "DataType": type_,
        "DataValue": str(value),
        "TimeStamp": str(int(timestamp.timestamp() * 1000))
    }

def generate_campaign_data(vin_number, timestamp, entries_count):
    return {
        "Campaign_ID": f"Campaign_ID_{random.randint(1, 10)}",
        "DataEntries": [generate_data_entry(timestamp) for _ in range(entries_count)],
        "VinNumber": vin_number
    }

def setup_producer(bootstrap_servers):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers.split(','),
        api_version=(3, 7, 1)
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(','),
        api_version=(3, 7, 1),
        acks='all',
        retries=5,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=30000,
        retry_backoff_ms=100,
        batch_size=1048576,
        linger_ms=50
    )
    return admin_client, producer

def produce_messages(topic, bootstrap_servers, target_date, max_batch_size):
    admin_client, producer = setup_producer(bootstrap_servers)
    create_topic_if_not_exists(admin_client, topic)

    vin_numbers, weights = load_vin_numbers('./car_models_vin.csv')
    target_datetime = datetime.strptime(target_date, "%Y-%m-%d")
    total_entries = 1000000
    remaining_entries = total_entries
    sent_entries = 0
    start_time = time.time()

    print(f"Starting production of {total_entries:,} entries for {target_date}")

    while remaining_entries > 0:
        batch_size = min(max_batch_size, remaining_entries)
        batch_messages = []

        for _ in range(batch_size):
            entries_this_message = min(random.randint(50, 100), remaining_entries)
            timestamp = target_datetime + timedelta(seconds=random.randint(0, 86399))
            vin = random.choices(vin_numbers, weights=weights, k=1)[0]

            message = generate_campaign_data(vin, timestamp, entries_this_message)
            batch_messages.append(json.dumps(message).encode('utf-8'))

            sent_entries += entries_this_message
            remaining_entries -= entries_this_message

        for msg in batch_messages:
            producer.send(topic, msg)

        rate = sent_entries / (time.time() - start_time)
        print(f"Progress: {(sent_entries/total_entries)*100:.1f}% | Rate: {rate:.0f} entries/sec")

    producer.flush(timeout=60)

    producer.close()
    admin_client.close()

    duration = time.time() - start_time
    print(f"\nCompleted {total_entries:,} entries in {duration:.1f} seconds")
    print(f"Average rate: {total_entries/duration:.0f} entries/sec")

if __name__ == "__main__":
    produce_messages(args.topic, args.bootstrap_servers, args.target_date, args.max_batch_size)