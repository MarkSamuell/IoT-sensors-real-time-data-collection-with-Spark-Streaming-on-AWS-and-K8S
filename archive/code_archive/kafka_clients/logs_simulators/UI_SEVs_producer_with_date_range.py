# --start_date 2024-10-01 --end_date 2024-10-07 --events_per_day 1000 --batch_size 100

import csv
import argparse
import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import numpy as np

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer that simulates SEV data with VIN numbers over a specified period.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
parser.add_argument('--start_date', type=str, default=None, help='Start date for simulation (YYYY-MM-DD)')
parser.add_argument('--end_date', type=str, default=None, help='End date for simulation (YYYY-MM-DD)')
parser.add_argument('--events_per_day', type=int, default=10000, help='Base number of security events to produce per day')
parser.add_argument('--batch_size', type=int, default=100, help='Number of events to send in each batch')
args = parser.parse_args()

def load_vin_numbers(csv_file):
    vin_numbers = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            if len(row) > 1:
                vin_numbers.append(row[1])  # VIN is in the second column
            else:
                print(f"Skipping row with insufficient columns: {row}")
    
    # Assign weights to VINs using a power law distribution
    weights = np.random.power(0.5, size=len(vin_numbers))
    weights /= weights.sum()  # Normalize weights
    
    return vin_numbers, weights.tolist()

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

def generate_random_parameter(param_name, param_type, param_value, param_unit, timestamp):
    return {
        "Timestamp": timestamp,
        "InternalParameter": {
            "ParameterName": param_name,
            "ParameterType": param_type,
            "ParameterValue": str(param_value),
            "ParameterUnit": param_unit
        }
    }

def create_security_event(vin_number, rule_id, cpu_data, temp_data, speed_data, network_type, network_id, timestamp):
    origins = ["AC ECU", "Motor ECU", "Brakes ECU", "Transmission ECU"]
    severities = ["Low", "Medium", "High"]

    # Determine SEV_Msg based on rule_id
    sev_messages = {
        "001": "Invalid Can ID Message",
        "002": "High CPU Usage Message",
        "003": "Bus Flood Attack Message"
    }
    sev_msg = sev_messages.get(rule_id, "Unknown Security Event")

    # Randomly decide if IoC parameters should be empty
    if random.choice([True, False]):
        ioc_parameters = []
    else:
        # Randomly select parameters for IoC
        parameters = [cpu_data, temp_data, speed_data]
        selected_params = random.sample(parameters, k=min(len(parameters), 2))  # Select up to 2 parameters

        # Generate a random number of additional IoC parameters (0 to 5)
        additional_ioc_params = [
            generate_random_parameter(f"Random_Param_{i}", "Type", random.randint(0, 100), "unit", timestamp)
            for i in range(random.randint(0, 5))
        ]

        ioc_parameters = selected_params + additional_ioc_params

    return {
        "ID": rule_id,
        "IoC": {
            "Parameters": ioc_parameters
        },
        "NetworkID": network_id,
        "NetworkType": network_type,
        "Origin": random.choice(origins),
        "SEV_Msg": sev_msg,
        "Severity": random.choice(severities),
        "Timestamp": str(timestamp),
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
    vin_numbers, weights = load_vin_numbers(csv_file)

    if not vin_numbers:
        print("No VIN numbers loaded. Exiting.")
        return

    predefined_rule_ids = ['001', '002', '003']

    total_security_events = 0
    total_ioc_parameters = 0
    total_messages_sent = 0
    vin_event_counts = {vin: 0 for vin in vin_numbers}

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else datetime.now().replace(day=1)
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else start_date + timedelta(days=30)

    current_date = start_date
    while current_date <= end_date:
        day_start_time = time.time()
        
        # Introduce high daily variation
        variation_factors = [0.2, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
        daily_factor = random.choice(variation_factors)
        daily_events = max(1, int(args.events_per_day * daily_factor))  # Ensure at least 1 event
        
        print(f"Simulating data for date: {current_date}")
        print(f"Events for today: {daily_events} (Factor: {daily_factor})")
        
        seconds_per_simulated_day = 10  # 10 seconds per day
        events_per_second = daily_events / seconds_per_simulated_day
        accumulated_events = []

        for second in range(seconds_per_simulated_day):
            second_start_time = time.time()
            events_this_second = int(events_per_second) + (1 if random.random() < (events_per_second % 1) else 0)

            for _ in range(events_this_second):
                simulated_seconds = random.randint(0, 24 * 60 * 60 - 1)
                timestamp = int((current_date + timedelta(seconds=simulated_seconds)).timestamp() * 1000)

                vin_number = np.random.choice(vin_numbers, p=weights)
                rule_id = random.choice(predefined_rule_ids)
                
                cpu_data = generate_random_parameter("CPU", "Usage", random.randint(0, 100), "%", timestamp)
                temp_data = generate_random_parameter("Global_Temperature", "Temperature", random.randint(-10, 60), "celsius_degree", timestamp)
                speed_data = generate_random_parameter("Vehicle_Speed", "Speed", random.randint(0, 200), "km/h", timestamp)

                network_type = random.choice(network_types)
                network_id = random.choice(network_ids)

                security_event = create_security_event(vin_number, rule_id, cpu_data, temp_data, speed_data, network_type, network_id, timestamp)
                accumulated_events.append(security_event)

                total_ioc_parameters += len(security_event["IoC"]["Parameters"])
                vin_event_counts[vin_number] += 1

                if len(accumulated_events) >= args.batch_size:
                    message = json.dumps(accumulated_events).encode('utf-8')
                    try:
                        future = producer.send(topic, message)
                        record_metadata = future.get(timeout=10)
                        total_messages_sent += 1
                        total_security_events += len(accumulated_events)
                    except KafkaError as e:
                        print(f"Error sending batch to Kafka: {e}")
                    accumulated_events = []

            # Calculate sleep time to maintain 1 second per iteration
            elapsed_time = time.time() - second_start_time
            sleep_time = max(0, 1 - elapsed_time)
            time.sleep(sleep_time)

        # Send any remaining events
        if accumulated_events:
            message = json.dumps(accumulated_events).encode('utf-8')
            try:
                future = producer.send(topic, message)
                record_metadata = future.get(timeout=10)
                total_messages_sent += 1
                total_security_events += len(accumulated_events)
            except KafkaError as e:
                print(f"Error sending batch to Kafka: {e}")

        day_end_time = time.time()
        day_duration = day_end_time - day_start_time
        print(f"Day {current_date} simulated in {day_duration:.2f} seconds")
        print(f"Total Security Events Generated: {total_security_events}")
        print(f"Total IoC Parameters Generated: {total_ioc_parameters}")
        print(f"Total Messages Sent to Kafka: {total_messages_sent}")
        print("==========================================================================================")

        current_date += timedelta(days=1)

    producer.close()
    print(f"Simulation complete. Total security events sent: {total_security_events}")

    # Print distribution of events per VIN
    print("\nDistribution of events per VIN:")
    for vin, count in sorted(vin_event_counts.items(), key=lambda x: x[1], reverse=True)[:10]:  # Top 10 VINs
        print(f"VIN: {vin}, Events: {count}")

    # Print some statistics
    event_counts = list(vin_event_counts.values())
    print(f"\nEvent count statistics:")
    print(f"Min events per VIN: {min(event_counts)}")
    print(f"Max events per VIN: {max(event_counts)}")
    print(f"Average events per VIN: {sum(event_counts) / len(event_counts):.2f}")

if __name__ == '__main__':
    main()