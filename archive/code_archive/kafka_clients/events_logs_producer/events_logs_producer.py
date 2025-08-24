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

# Generate random parameter message with VIN number
def generate_random_parameter(vin_number, param_name, param_value, param_unit):
    return {
        "timestamp": str(datetime.utcnow().isoformat()),
        "VIN": vin_number,
        "InternalParameter": {
            "ParameterName": param_name,
            "ParameterValue": str(param_value),
            "ParameterUnit": param_unit
        }
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

# CSV file path
csv_file = './car_models_vin.csv'
vin_numbers = load_vin_numbers(csv_file)

# Initialize index to cycle through VIN numbers
vin_index = 0

while True:
    if not vin_numbers:
        print("No VIN numbers loaded. Exiting.")
        break
    
    print(f"VIN index: {vin_index}, Total VINs: {len(vin_numbers)}")


    
    # Generate random parameters
    cpu_data = generate_random_parameter(vin_numbers[vin_index], "CPU", random.randint(0, 100), "%")
    temp_data = generate_random_parameter(vin_numbers[vin_index+1], "Global_Temperature", random.randint(-10, 60), "celsius_degree")
    speed_data = generate_random_parameter(vin_numbers[vin_index+2], "Vehicle_Speed", random.randint(0, 200), "km/h")
    
    # Prepare and send the CPU message
    cpu_message = json.dumps(cpu_data).encode('utf-8')
    try:
        future_cpu = producer.send(topic, cpu_message)
        record_metadata = future_cpu.get(timeout=10)
        print(f'CPU Message delivered to {record_metadata.topic} [{record_metadata.partition}]')
        print("Sending CPU message: ", json.dumps(cpu_data, indent=2))
    except KafkaError as e:
        print(f'CPU Message delivery failed: {e}')
    
    time.sleep(2)
    
    # Prepare and send the Global Temperature message
    temp_message = json.dumps(temp_data).encode('utf-8')
    try:
        future_temp = producer.send(topic, temp_message)
        record_metadata = future_temp.get(timeout=10)
        print(f'Global Temperature Message delivered to {record_metadata.topic} [{record_metadata.partition}]')
        print("Sending Global Temperature message: ", json.dumps(temp_data, indent=2))
    except KafkaError as e:
        print(f'Global Temperature Message delivery failed: {e}')
    
    time.sleep(2)
    
    # Prepare and send the Vehicle Speed message
    speed_message = json.dumps(speed_data).encode('utf-8')
    try:
        future_speed = producer.send(topic, speed_message)
        record_metadata = future_speed.get(timeout=10)
        print(f'Vehicle Speed Message delivered to {record_metadata.topic} [{record_metadata.partition}]')
        print("Sending Vehicle Speed message: ", json.dumps(speed_data, indent=2))
    except KafkaError as e:
        print(f'Vehicle Speed Message delivery failed: {e}')
    
    vin_index = (vin_index + 3) % len(vin_numbers)  # Cycle through VIN numbers
    
    time.sleep(2)


