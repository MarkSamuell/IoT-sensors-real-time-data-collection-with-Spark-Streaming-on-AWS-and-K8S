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

def get_epoch_time():
    """Convert current time to epoch format in milliseconds"""
    return int(datetime.utcnow().timestamp() * 1000)

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

def create_topic_if_not_exists(admin_client, topic_name):
    """Create topic if it does not exist"""
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

def generate_parameter(vin_number, param_config):
    """Generate parameter with correct field mappings"""
    return {
        "Timestamp": get_epoch_time(),
        "InternalParameter": {
            "ParameterName": param_config["name"],
            "ParameterType": param_config["type"],
            "ParameterValue": str(param_config["value"]),
            "ParameterUnit": param_config["unit"]
        }
    }

def generate_random_monitoring_parameter(index):
    """Generate a random monitoring parameter with meaningful values"""
    parameter_types = [
        {
            "name": f"Monitor_{index}",
            "type": "Temperature",
            "value": random.randint(-10, 60),
            "unit": "celsius"
        },
        {
            "name": f"Monitor_{index}",
            "type": "Pressure",
            "value": random.randint(0, 100),
            "unit": "psi"
        },
        {
            "name": f"Monitor_{index}",
            "type": "Voltage",
            "value": random.randint(0, 24),
            "unit": "volts"
        }
    ]
    return random.choice(parameter_types)

def create_security_event(vin_number, rule_id, timestamp):
    """Create a security event with properly structured IoC parameters"""
    origins = ["AC ECU", "Motor ECU", "Brakes ECU", "Transmission ECU"]
    severities = ["Low", "Medium", "High"]
    network_types = ["CAN", "Ethernet"]
    network_ids = ["12", "36", "65", "78", "90"]

    # Define standard parameters
    standard_params = [
        {
            "name": "CPU",
            "type": "Usage",
            "value": random.randint(0, 100),
            "unit": "percent"
        },
        {
            "name": "Global_Temperature",
            "type": "Temperature",
            "value": random.randint(-10, 60),
            "unit": "celsius"
        },
        {
            "name": "Vehicle_Speed",
            "type": "Speed",
            "value": random.randint(0, 200),
            "unit": "km/h"
        }
    ]

    # Determine SEV_Msg based on rule_id
    sev_messages = {
        "001": "Invalid Can ID Message",
        "002": "High CPU Usage Message",
        "003": "Bus Flood Attack Message",
        "004": "IdsM SEV_MSG",
        "005": "Negative Response Code"
    }
    sev_msg = sev_messages.get(rule_id, "Unknown Security Event")

    # Handle IoC parameters based on rule_id
    if rule_id == "005":
        # For ID 005, always include exactly one standard parameter
        ioc_parameters = [generate_parameter(vin_number, random.choice(standard_params))]
    else:
        if random.choice([True, False]):
            ioc_parameters = []
        else:
            # Select up to 2 standard parameters
            selected_params = random.sample(standard_params, k=random.randint(1, 2))
            ioc_parameters = [generate_parameter(vin_number, param) for param in selected_params]

            # Add random monitoring parameters (0 to 3)
            num_additional = random.randint(0, 3)
            for i in range(num_additional):
                monitor_param = generate_random_monitoring_parameter(i)
                ioc_parameters.append(generate_parameter(vin_number, monitor_param))

    return {
        "ID": rule_id,
        "IoC": {
            "Parameters": ioc_parameters
        },
        "NetworkID": random.choice(network_ids),
        "NetworkType": random.choice(network_types),
        "Origin": random.choice(origins),
        "SEV_Msg": sev_msg,
        "Severity": random.choice(severities),
        "Timestamp": str(timestamp),
        "VinNumber": vin_number
    }

def main():
    bootstrap_servers = args.bootstrap_servers.split(',')

    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 7, 1)
    )

    topic = args.topic
    create_topic_if_not_exists(admin_client, topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 7, 1)
    )

    csv_file = './car_models_vin.csv'
    vin_numbers = load_vin_numbers(csv_file)

    if not vin_numbers:
        print("No VIN numbers loaded. Exiting.")
        return

    predefined_rule_ids = ['001', '002', '003', '004', '005']

    vin_index = 0
    rule_index = 0
    total_vins = len(vin_numbers)
    total_security_events = 0
    total_ioc_parameters = 0
    total_messages_sent = 0

    try:
        while True:
            num_events = random.randint(50, 100)
            accumulated_events = []

            for _ in range(num_events):
                vin_number = vin_numbers[vin_index]
                rule_id = predefined_rule_ids[rule_index]
                timestamp = get_epoch_time()

                security_event = create_security_event(vin_number, rule_id, timestamp)
                accumulated_events.append(security_event)

                # Count IoC parameters
                total_ioc_parameters += len(security_event["IoC"]["Parameters"])

                # Move to the next VIN and RULE_ID in a circular way
                vin_index = (vin_index + 1) % total_vins
                rule_index = (rule_index + 1) % len(predefined_rule_ids)

            if accumulated_events:
                try:
                    future = producer.send(topic, accumulated_events)
                    record_metadata = future.get(timeout=10)
                    total_messages_sent += 1
                    total_security_events += len(accumulated_events)
                    
                    print("=" * 40, "SECURITY EVENTS", "=" * 40)
                    print(f'Message delivered to {record_metadata.topic} [{record_metadata.partition}]')
                    print(f"Events in batch: {len(accumulated_events)}")
                    print(f"Total Security Events: {total_security_events}")
                    print(f"Total IoC Parameters: {total_ioc_parameters}")
                    print(f"Total Messages Sent: {total_messages_sent}")
                    print("=" * 100)
                    
                except KafkaError as e:
                    print("=" * 40, "ERROR", "=" * 40)
                    print(f'Message delivery failed: {e}')
                    print("=" * 100)

            time.sleep(2)
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()
        admin_client.close()

if __name__ == '__main__':
    main()