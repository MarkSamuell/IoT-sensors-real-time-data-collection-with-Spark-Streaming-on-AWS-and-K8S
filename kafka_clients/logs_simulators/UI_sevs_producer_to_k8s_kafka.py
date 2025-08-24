
import argparse
import json
import random
import time
import csv
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer that simulates security events with VIN numbers.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

bootstrap_servers = args.bootstrap_servers.split(',')

def get_epoch_time():
    """Convert current time to epoch format in milliseconds"""
    return int(datetime.utcnow().timestamp() * 1000)

def load_vin_numbers(csv_file):
    """Load VIN numbers from CSV file"""
    vin_numbers = []
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Skip the header row
            for row in reader:
                if len(row) > 1:
                    vin_numbers.append(row[1])  # VIN is in the second column
                else:
                    print(f"Skipping row with insufficient columns: {row}")
    except FileNotFoundError:
        print(f"CSV file {csv_file} not found, using dummy data")
        return ["1HGCM82633A123456", "JH4KA7532NC123456", "WAUAC48H86K123456"]
    return vin_numbers

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

def produce_messages(topic, bootstrap_servers):
    print(f"Connecting to bootstrap servers: {bootstrap_servers}")

    # Create producer with basic settings and longer timeouts
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=60000,      # 60 seconds
        reconnect_backoff_ms=1000,     # 1 second
        max_block_ms=120000,           # 2 minutes
        metadata_max_age_ms=300000,    # 5 minutes
        connections_max_idle_ms=60000  # 60 seconds
    )

    print(f"Producer created successfully!")

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

    # Send a test message first
    try:
        print(f"Sending test message to topic: {topic}")
        test_event = create_security_event(
            vin_numbers[0],
            predefined_rule_ids[0],
            get_epoch_time()
        )
        future = producer.send(topic, value=[test_event])
        record_metadata = future.get(timeout=30)
        print(f"Test message delivered to {record_metadata.topic} [{record_metadata.partition}]")
        print("=" * 80)
    except Exception as e:
        print(f"Failed to send test message: {e}")
        print("Continuing with regular messages anyway...")

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
                    future = producer.send(topic, value=accumulated_events)
                    record_metadata = future.get(timeout=30)
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