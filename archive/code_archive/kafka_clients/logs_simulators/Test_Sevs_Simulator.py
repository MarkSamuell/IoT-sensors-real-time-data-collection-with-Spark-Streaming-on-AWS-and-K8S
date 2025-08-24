## Simple SEVs simulator that sends logs of static list of vin numbers and adds counters for log

import argparse
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import uuid  # For generating unique IDs
from collections import defaultdict

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer that simulates events_logs_data with VIN numbers.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
args = parser.parse_args()

# Convert current time to epoch format
def get_epoch_time():
    return int(datetime.utcnow().timestamp())

# Dummy function to generate random parameters
def generate_random_parameter(vin_number, param_name, param_type, param_value, param_unit):
    return {
        "timestamp": get_epoch_time(),  # Include timestamp in epoch format
        "InternalParameter": {
            "ParameterName": param_name,
            "ParamterType": param_type,
            "ParameterValue": param_value,
            "ParameterUnit": param_unit
        }
    }

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

def create_security_event(vin_number, cpu_data, temp_data, speed_data, network_type=None, network_id=None):
    # Generate a timestamp for this message
    timestamp = get_epoch_time()

    # Randomly decide if IoC parameters should be empty
    if random.choice([True, False]):
        ioc_parameters = []
    else:
        # Randomly select parameters for IoC
        parameters = [cpu_data, temp_data, speed_data]
        selected_params = random.sample(parameters, k=min(len(parameters), 2))  # Select up to 2 parameters

        # Generate a random number of additional IoC parameters (0 to 10)
        additional_ioc_params = [generate_random_parameter(vin_number, f"Random_Param_{i}", "Type", random.randint(0, 100), "unit") for i in range(random.randint(0, 5))]

        ioc_parameters = selected_params + additional_ioc_params

    # Define severity and message
    severity = "low"  # Default to "low" to ensure at least some events are generated
    severity_message = "Event recorded."
    origin = "Unknown"

    # Extract parameter values for condition checks
    selected_param_values = {param["InternalParameter"]["ParameterName"]: int(param["InternalParameter"]["ParameterValue"]) for param in ioc_parameters}

    # Apply conditions based on selected parameters
    if "CPU" in selected_param_values and "Global_Temperature" in selected_param_values:
        origin = "ECU"
        if selected_param_values["CPU"] > 90 and selected_param_values["Global_Temperature"] > 60:
            severity = "Critical"
            severity_message = "Critical alert: Extremely high CPU usage and temperature."
        elif selected_param_values["CPU"] > 80 and selected_param_values["Global_Temperature"] > 50:
            severity = "High"
            severity_message = "High alert: High CPU usage and temperature."
        elif selected_param_values["CPU"] > 70 and selected_param_values["Global_Temperature"] > 40:
            severity = "Medium"
            severity_message = "Medium alert: Elevated CPU usage and temperature."
        elif selected_param_values["CPU"] > 60 and selected_param_values["Global_Temperature"] > 30:
            severity = "Low"
            severity_message = "Low alert: Mildly elevated CPU usage and temperature."
    elif "CPU" in selected_param_values and "Vehicle_Speed" in selected_param_values:
        origin = "TCU"
        if selected_param_values["CPU"] > 90 and selected_param_values["Vehicle_Speed"] > 150:
            severity = "Critical"
            severity_message = "Critical alert: Extremely high CPU usage and vehicle speed."
        elif selected_param_values["CPU"] > 80 and selected_param_values["Vehicle_Speed"] > 120:
            severity = "High"
            severity_message = "High alert: High CPU usage and vehicle speed."
        elif selected_param_values["CPU"] > 70 and selected_param_values["Vehicle_Speed"] > 100:
            severity = "Medium"
            severity_message = "Medium alert: Elevated CPU usage and vehicle speed."
        elif selected_param_values["CPU"] > 60 and selected_param_values["Vehicle_Speed"] > 80:
            severity = "Low"
            severity_message = "Low alert: Mildly elevated CPU usage and vehicle speed."
    elif "Global_Temperature" in selected_param_values and "Vehicle_Speed" in selected_param_values:
        origin = "Radiator"
        if selected_param_values["Global_Temperature"] > 60 and selected_param_values["Vehicle_Speed"] > 150:
            severity = "Critical"
            severity_message = "Critical alert: Extremely high temperature and vehicle speed."
        elif selected_param_values["Global_Temperature"] > 50 and selected_param_values["Vehicle_Speed"] > 120:
            severity = "High"
            severity_message = "High alert: High temperature and vehicle speed."
        elif selected_param_values["Global_Temperature"] > 40 and selected_param_values["Vehicle_Speed"] > 100:
            severity = "Medium"
            severity_message = "Medium alert: Elevated temperature and vehicle speed."
        elif selected_param_values["Global_Temperature"] > 30 and selected_param_values["Vehicle_Speed"] > 80:
            severity = "Low"
            severity_message = "Low alert: Mildly elevated temperature and vehicle speed."

    # Define security event structure
    security_event = {
        "SEV_ID": f"ID_{uuid.uuid4().hex[:8]}",  # Unique ID for the event
        "VinNumber": vin_number,
        "Timestamp": timestamp,
        "Name": "",  # Define as needed
        "Severity": severity,
        "SEV_Msg": severity_message,
        "Origin": origin,
        "IoC": {
            "Parameters": ioc_parameters  # Include the selected parameters and additional IoC parameters
        }
    }

    # Conditionally include NetworkType and NetworkID
    if network_type and network_id:
        security_event["NetworkType"] = network_type
        security_event["NetworkID"] = network_id

    return security_event

def main():
    # Split the bootstrap_servers string into a list
    bootstrap_servers = args.bootstrap_servers.split(',')

    # Define possible values for NetworkType and NetworkID
    network_types = ["Ethernet", "CAN"]
    network_ids = ["0001", "0002", "0003", "0004", "0005"]

    # Kafka admin and producer setup
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1)  # MSK Kafka Version
    )

    topic = args.topic  # Use command-line argument for topic
    create_topic_if_not_exists(admin_client, topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1)
    )

    # Predefined list of VIN numbers
    vin_numbers = [
        "1HGCM82633A004352",
        "1HGCM82633A004353",
        "1HGCM82633A004354",
        "1HGCM82633A004355",
        "1HGCM82633A004356"
    ]

    # Initialize indices and counters
    vin_index = 0
    network_type_index = 0
    network_id_index = 0

    total_vins = len(vin_numbers)
    security_event_count = 0  # Counter for security events
    vin_event_counts = defaultdict(int)  # Counter for security events per VIN

    while True:
        print(f"VIN index: {vin_index}, Total VINs: {total_vins}")

        # Generate random parameters
        cpu_data = generate_random_parameter(vin_numbers[vin_index], "CPU", "Usage", random.randint(0, 100), "%")
        temp_data = generate_random_parameter(vin_numbers[vin_index], "Global_Temperature", "Temperature", random.randint(-10, 60), "celsius_degree")
        speed_data = generate_random_parameter(vin_numbers[vin_index], "Vehicle_Speed", "Speed", random.randint(0, 200), "km/h")

        # Randomly decide whether to include network info
        include_network_info = random.choice([True, False])
        if include_network_info:
            network_type = network_types[network_type_index % len(network_types)]
            network_id = network_ids[network_id_index % len(network_ids)]
            print(f"Selected Network Type: {network_type}")
            print(f"Selected Network ID: {network_id}")
        else:
            network_type = None
            network_id = None
            print("No Network Type and Network ID included.")

        # Generate a security event
        event = create_security_event(
            vin_numbers[vin_index],
            cpu_data,
            temp_data,
            speed_data,
            network_type,
            network_id
        )

        # Produce the message
        try:
            producer.send(topic, key=vin_numbers[vin_index].encode(), value=json.dumps(event).encode())
            producer.flush()
            print(f"Produced event for VIN: {vin_numbers[vin_index]}")

            # Update counters
            security_event_count += 1
            vin_event_counts[vin_numbers[vin_index]] += 1

            print(f"Total security events produced: {security_event_count}")
            print(f"Security events count per VIN: {dict(vin_event_counts)}")

        except KafkaError as e:
            print(f"Error producing message: {e}")

        # Rotate through VIN numbers and network types/IDs
        vin_index = (vin_index + 1) % total_vins
        network_type_index = (network_type_index + 1) % len(network_types)
        network_id_index = (network_id_index + 1) % len(network_ids)

        # Sleep for 2 seconds before producing the next event
        time.sleep(2)

if __name__ == "__main__":
    main()

    