# --start_date 2024-10-01 --end_date 2024-10-7 --messages_per_day 5000 --batch_size 100

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

# Argument parsing (unchanged)
parser = argparse.ArgumentParser(description='Kafka producer that simulates events_logs_data with VIN numbers over a month, compressed to 5 minutes per day.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
parser.add_argument('--start_date', type=str, default=None, help='Start date for simulation (YYYY-MM-DD)')
parser.add_argument('--end_date', type=str, default=None, help='End date for simulation (YYYY-MM-DD)')
parser.add_argument('--messages_per_day', type=int, default=10000, help='Number of messages to produce per day')
parser.add_argument('--batch_size', type=int, default=100, help='Number of messages to send in each batch')
args = parser.parse_args()

bootstrap_servers = args.bootstrap_servers.split(',')

# Load VIN numbers from CSV file and assign weights
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
    
    # Assign random weights to VINs
    weights = np.random.pareto(a=1.5, size=len(vin_numbers))
    weights /= weights.sum()  # Normalize weights
    
    return vin_numbers, weights.tolist()

# Generate random DataEntry (unchanged)
def generate_random_data_entry(timestamp):
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
        "TimeStamp": str(int(timestamp.timestamp() * 1000))  # Timestamp in milliseconds
    }

# Generate Campaign data with multiple DataEntries (unchanged)
def generate_campaign_data(vin_number, timestamp):
    num_entries = random.randint(50, 100)  # Random number of entries between 50 and 100
    data_entries = [generate_random_data_entry(timestamp) for _ in range(num_entries)]
    return {
        "Campaign_ID": f"Campaign_ID_{random.randint(1, 10)}",  # Random Campaign ID
        "DataEntries": data_entries,
        "VinNumber": vin_number
    }

# Kafka topic creation function (unchanged)
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

# Kafka admin and producer setup (unchanged)
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
def produce_messages(topic, bootstrap_servers, start_date, end_date, messages_per_day, batch_size):
    admin_client, producer = setup_kafka_producer(bootstrap_servers)
    create_topic_if_not_exists(admin_client, topic)

    # CSV file path
    csv_file = './car_models_vin.csv'
    vin_numbers, weights = load_vin_numbers(csv_file)

    # Track total count of DataEntries and messages
    total_data_entries_count = 0
    total_messages_sent = 0
    vin_data_entries = {vin: 0 for vin in vin_numbers}

    current_date = start_date
    while current_date <= end_date:
        day_start_time = time.time()
        
        # Introduce high daily variation
        variation_factors = [0.2, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
        daily_factor = random.choice(variation_factors)
        daily_messages = max(batch_size, int(messages_per_day * daily_factor))  # Ensure at least one batch
        
        print(f"Simulating data for date: {current_date}")
        print(f"Messages for today: {daily_messages} (Factor: {daily_factor})")
        
        # Calculate the time interval between batches
        seconds_per_simulated_day = 5 * 60  # 5 minutes
        batches_per_day = daily_messages // batch_size
        interval = seconds_per_simulated_day / batches_per_day

        for batch in range(batches_per_day):
            batch_start_time = time.time()
            
            for _ in range(batch_size):

            # Calculate sleep time to maintain the desired rate
                elapsed_time = time.time() - batch_start_time
                sleep_time = max(0, interval - elapsed_time)
                time.sleep(sleep_time)

                # Generate a timestamp within the 24-hour period of the current date
                simulated_seconds = random.randint(0, 24 * 60 * 60 - 1)
                timestamp = current_date + timedelta(seconds=simulated_seconds)
                
                # Select a VIN based on weights
                vin_number = random.choices(vin_numbers, weights=weights, k=1)[0]
                
                # Generate campaign data
                campaign_data = generate_campaign_data(vin_number, timestamp)
                data_entries_count = len(campaign_data["DataEntries"])
                total_data_entries_count += data_entries_count
                vin_data_entries[vin_number] += data_entries_count

                # Prepare and send the message
                campaign_message = json.dumps(campaign_data).encode('utf-8')
                try:
                    future_campaign = producer.send(topic, campaign_message)
                    record_metadata = future_campaign.get(timeout=10)
                    total_messages_sent += 1
                except KafkaError as e:
                    print(f'Campaign Message delivery failed: {e}')

            # Flush the producer to ensure all messages are sent
            producer.flush()

            # Print batch statistics
            print(f"Batch {batch + 1}/{batches_per_day} complete. Total messages sent: {total_messages_sent}")
            print(f"Total DataEntries Count: {total_data_entries_count}")

            # Calculate sleep time to maintain the desired rate
            elapsed_time = time.time() - batch_start_time
            sleep_time = max(0, interval - elapsed_time)
            time.sleep(sleep_time)

        day_end_time = time.time()
        day_duration = day_end_time - day_start_time
        print(f"Day {current_date} simulated in {day_duration:.2f} seconds")
        print("=================================================================================")

        current_date += timedelta(days=1)

    producer.close()
    print(f"Simulation complete. Total messages sent: {total_messages_sent}")
    
    # Print data entry distribution for VINs
    print("\nData Entry Distribution per VIN:")
    for vin, count in sorted(vin_data_entries.items(), key=lambda x: x[1], reverse=True):
        print(f"VIN: {vin}, Data Entries: {count}")

# Run the producer only when the script is executed directly
if __name__ == "__main__":
    # Parse start and end dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else datetime.now().replace(day=1)
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else start_date + timedelta(days=30)

    produce_messages(args.topic, bootstrap_servers, start_date, end_date, args.messages_per_day, args.batch_size)