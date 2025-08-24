import csv
import argparse
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import uuid

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer that simulates events_logs_data with VIN numbers.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

# Convert current time to epoch format in milliseconds
def get_epoch_time():
    return int(datetime.utcnow().timestamp() * 1000)

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

# Function to create topic if it does not exist
def create_topic_if_not_exists(admin_client, topic_name):
    try:
        topics = admin_client.list_topics()
        if topic_name not in topics:
            print(f"Creating topic: {topic_name}")
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic {topic_name} created.")
        else:
            print(f"Topic {topic_name} already exists.")
    except KafkaError as e:
        print(f"Failed to create topic {topic_name}: {e}")

# Generate random parameter
def generate_random_parameter(vin_number, param_name, param_type, param_value, param_unit):
    return {
        "Timestamp": get_epoch_time(),
        "InternalParameter": {
            "ParameterName": param_name,
            "ParameterType": param_type,
            "ParameterValue": str(param_value),
            "ParameterUnit": param_unit
        }
    }

def create_security_event(vin_number, rule_id, cpu_data, temp_data, speed_data, network_type, network_id):
    origins = ["AC ECU", "Motor ECU", "Brakes ECU", "Transmission ECU", "Battery Management System"]
    severities = ["Low", "Medium", "High"]

    # Randomly decide if IoC parameters should be empty
    if random.choice([True, False]):
        ioc_parameters = []
    else:
        # Randomly select parameters for IoC
        parameters = [cpu_data, temp_data, speed_data]
        selected_params = random.sample(parameters, k=min(len(parameters), 2))  # Select up to 2 parameters

        # Generate a random number of additional IoC parameters (0 to 5)
        additional_ioc_params = [
            generate_random_parameter(vin_number, f"Random_Param_{i}", "Type", random.randint(0, 100), "unit")
            for i in range(random.randint(0, 5))
        ]

        ioc_parameters = selected_params + additional_ioc_params

    return {
        "ID": rule_id,  # Use the predefined RULE_ID
        "IoC": {
            "Parameters": ioc_parameters
        },
        "NetworkID": network_id,
        "NetworkType": network_type,
        "Origin": random.choice(origins),
        "SEV_Msg": "Invalid Can ID Message",
        "Severity": random.choice(severities),
        "Timestamp": str(get_epoch_time()),
        "VinNumber": vin_number
    }

def main():
    bootstrap_servers = args.bootstrap_servers.split(',')
    network_types = ["CAN", "Ethernet"]
    network_ids = ["12", "36", "65", "78", "90"]

    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1)
    )

    topic = args.topic
    create_topic_if_not_exists(admin_client, topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1)
    )

    csv_file = './car_models_vin.csv'
    vin_numbers = load_vin_numbers(csv_file)

    if not vin_numbers:
        print("No VIN numbers loaded. Exiting.")
        return

    # Predefined list of SEV_ID / RULE_ID (matching with the 20 values in the lookup table)
    predefined_rule_ids = [
        '001', '002', '003', '004', '005', '006', '007', '008', '009', '010',
        '011', '012', '013', '014', '015', '016', '017', '018', '019', '020'
    ]

    vin_index = 0
    rule_index = 0
    total_vins = len(vin_numbers)
    total_security_events = 0
    total_ioc_parameters = 0
    total_messages_sent = 0  # New counter for total messages sent

    while True:
        # Generate a random number of events to accumulate (between 50 and 100)
        num_events = random.randint(50, 100)
        accumulated_events = []

        for _ in range(num_events):
            vin_number = vin_numbers[vin_index]
            rule_id = predefined_rule_ids[rule_index]  # Use predefined RULE_ID
            cpu_data = generate_random_parameter(vin_number, "CPU", "Usage", random.randint(0, 100), "%")
            temp_data = generate_random_parameter(vin_number, "Global_Temperature", "Temperature", random.randint(-10, 60), "celsius_degree")
            speed_data = generate_random_parameter(vin_number, "Vehicle_Speed", "Speed", random.randint(0, 200), "km/h")

            network_type = random.choice(network_types)
            network_id = random.choice(network_ids)

            security_event = create_security_event(vin_number, rule_id, cpu_data, temp_data, speed_data, network_type, network_id)
            accumulated_events.append(security_event)

            # Count IoC parameters
            total_ioc_parameters += len(security_event["IoC"]["Parameters"])

            # Move to the next VIN and RULE_ID in a circular way
            vin_index = (vin_index + 1) % total_vins
            rule_index = (rule_index + 1) % len(predefined_rule_ids)

        if accumulated_events:
            message = json.dumps(accumulated_events).encode('utf-8')
            try:
                future = producer.send(topic, message)
                record_metadata = future.get(timeout=10)
                total_messages_sent += 1  # Increment the total messages sent counter
                print("=======================================SECURITY=EVENTS=====================================")
                print(f'Security Events Message delivered to {record_metadata.topic} [{record_metadata.partition}]')
                print(f"Number of events in this batch: {len(accumulated_events)}")
                print("Sending Security Events message: ", json.dumps(accumulated_events, indent=2))
                print("==========================================================================================")
                total_security_events += len(accumulated_events)
                print(f"Total Security Events Generated: {total_security_events}")
                print(f"Total IoC Parameters Generated: {total_ioc_parameters}")
                print(f"Total Messages Sent to Kafka: {total_messages_sent}")  # Print total messages sent
            except KafkaError as e:
                print("=======================================ERROR==============================================")
                print(f'Security Events Message delivery failed: {e}')
                print("==========================================================================================")

        time.sleep(2)

if __name__ == '__main__':
    main()