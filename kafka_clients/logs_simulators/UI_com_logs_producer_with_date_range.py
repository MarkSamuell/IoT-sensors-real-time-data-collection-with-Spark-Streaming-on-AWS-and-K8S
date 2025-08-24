# --start_date 2025-05-01 --end_date 2025-05-10 --messages_per_day 10 --batch_size 100

import argparse
import json
import random
import csv
import time
import os
import socket
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError, NoBrokersAvailable
import numpy as np
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_producer')

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer that simulates events_logs_data with VIN numbers over a date range, compatible with Kubernetes.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
parser.add_argument('--start_date', type=str, default=None, help='Start date for simulation (YYYY-MM-DD)')
parser.add_argument('--end_date', type=str, default=None, help='End date for simulation (YYYY-MM-DD)')
parser.add_argument('--messages_per_day', type=int, default=10000, help='Number of messages to produce per day')
parser.add_argument('--batch_size', type=int, default=100, help='Number of messages to send in each batch')
parser.add_argument('--csv_path', type=str, default='./car_models_vin.csv', help='Path to CSV file with VIN numbers')
parser.add_argument('--replication_factor', type=int, default=1, help='Replication factor for topic creation')
parser.add_argument('--partitions', type=int, default=3, help='Number of partitions for topic creation')
args = parser.parse_args()

bootstrap_servers = args.bootstrap_servers.split(',')

# Load VIN numbers from CSV file and assign weights
def load_vin_numbers(csv_file):
    try:
        vin_numbers = []
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Skip the header row
            for row in reader:
                if len(row) > 1:
                    vin_numbers.append(row[1])  # VIN is in the second column
                else:
                    logger.warning(f"Skipping row with insufficient columns: {row}")
                    
        if not vin_numbers:
            logger.warning("No VIN numbers found in CSV. Using fallback sample VINs.")
            
        # Assign random weights to VINs using Pareto distribution for more realistic frequency
        weights = np.random.pareto(a=1.5, size=len(vin_numbers))
        weights /= weights.sum()  # Normalize weights
        
        logger.info(f"Loaded {len(vin_numbers)} VIN numbers from {csv_file}")
        return vin_numbers, weights.tolist()
    except FileNotFoundError:
        logger.error(f"CSV file {csv_file} not found, using fallback data")
    except Exception as e:
        logger.error(f"Error loading VIN numbers: {e}")

# Generate random DataEntry with Vehicle Signal Specification (VSS) data names
def generate_random_data_entry(timestamp, vss_data=None):
    if vss_data is None:
        # Default VSS data if none provided
        data_options = [
            # Steering and speed
            ("Vehicle.Chassis.SteeringWheel.Angle", "Double", random.uniform(-720.0, 720.0)),
            ("Vehicle.Speed", "Double", random.uniform(0, 180.0)),
            
            # Powertrain and transmission
            ("Vehicle.Powertrain.Transmission.IsParkLockEngaged", "Boolean", random.choice([0, 1])),
            ("Vehicle.Powertrain.ElectricMotor.Temperature", "Double", random.uniform(20.0, 90.0)),
            ("Vehicle.Powertrain.TractionBattery.Charging.IsCharging", "Boolean", random.choice([0, 1])),
            
            # OBD data
            ("Vehicle.OBD.ControlModuleVoltage", "Double", random.uniform(11.0, 14.5)),
            ("Vehicle.OBD.AmbientAirTemperature", "Double", random.uniform(-30.0, 45.0)),
            ("Vehicle.OBD.EngineLoad", "Double", random.uniform(0.0, 100.0)),
            
            # Chassis and brakes
            ("Vehicle.Chassis.Axle.Row1.Wheel.Left.Brake.IsFluidLevelLow", "Boolean", random.choice([0, 1])),
            
            # Body and mirrors
            ("Vehicle.Body.Windshield.Front.WasherFluid.IsLevelLow", "Boolean", random.choice([0, 1])),
            ("Vehicle.Body.Trunk.Rear.IsOpen", "Boolean", random.choice([0, 1])),
                   
            # Trailer
            ("Vehicle.Trailer.IsConnected", "Boolean", random.choice([0, 1])),
            
            # Added acceleration signals
            ("Vehicle.Acceleration.Lateral", "Double", random.uniform(-5.0, 5.0)),
            ("Vehicle.Acceleration.Longitudinal", "Double", random.uniform(-10.0, 5.0)),
            ("Vehicle.Acceleration.Vertical", "Double", random.uniform(-3.0, 3.0))
        ]
        data_name, data_type, data_value = random.choice(data_options)
    else:
        # Use provided VSS data
        data_name, data_type = random.choice(vss_data)
        # Generate appropriate random value based on data type
        if data_type == "Boolean":
            data_value = random.choice([0, 1])
        elif data_type == "Integer" or data_type == "Int":
            data_value = random.randint(-100, 1000)
        elif data_name == "Vehicle.Acceleration.Lateral":
            data_value = random.uniform(-5.0, 5.0)
        elif data_name == "Vehicle.Acceleration.Longitudinal":
            data_value = random.uniform(-10.0, 5.0)
        elif data_name == "Vehicle.Acceleration.Vertical":
            data_value = random.uniform(-3.0, 3.0)
        else:  # Default to Double
            data_value = random.uniform(-100.0, 1000.0)
    
    # Convert data type to appropriate string representation
    if data_type == "Boolean":
        string_value = str(data_value)
        display_type = "Short Int"
    else:
        string_value = str(round(data_value, 2) if isinstance(data_value, float) else data_value)
        display_type = data_type
        
    return {
        "DataName": data_name,
        "DataType": display_type,
        "DataValue": string_value,
        "TimeStamp": str(int(timestamp.timestamp() * 1000))  # Timestamp in milliseconds
    }

# Generate Campaign data with multiple DataEntries
def generate_campaign_data(vin_number, timestamp):
    num_entries = random.randint(50, 100)  # Random number of entries between 50 and 100
    data_entries = [generate_random_data_entry(timestamp) for _ in range(num_entries)]
    return {
        "Campaign_ID": f"Campaign_ID_{random.randint(1, 10)}",  # Random Campaign ID
        "DataEntries": data_entries,
        "VinNumber": vin_number
    }

# Kafka topic creation function with retry logic for Kubernetes environments
def create_topic_if_not_exists(admin_client, topic_name, replication_factor, partitions):
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            topic_list = admin_client.list_topics()
            if topic_name not in topic_list:
                topic = NewTopic(
                    name=topic_name, 
                    num_partitions=partitions, 
                    replication_factor=replication_factor, 
                    topic_configs={"cleanup.policy": "delete", "retention.ms": "604800000"}
                )
                try:
                    admin_client.create_topics([topic])
                    logger.info(f"Topic '{topic_name}' created successfully with {partitions} partitions and replication factor {replication_factor}")
                    return True
                except TopicAlreadyExistsError:
                    logger.info(f"Topic '{topic_name}' already exists.")
                    return True
                except KafkaError as e:
                    logger.error(f"Failed to create topic '{topic_name}': {e}")
                    raise
            else:
                logger.info(f"Topic '{topic_name}' already exists.")
                return True
        except NoBrokersAvailable:
            logger.warning(f"No brokers available, retrying in {retry_delay} seconds (attempt {attempt+1}/{max_retries})")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        except Exception as e:
            logger.error(f"Error checking/creating topic: {e}, retrying in {retry_delay} seconds (attempt {attempt+1}/{max_retries})")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
    
    logger.error(f"Failed to create/check topic after {max_retries} attempts")
    return False

# Kafka admin and producer setup with retry logic for Kubernetes
def setup_kafka_clients(bootstrap_servers):
    max_retries = 5
    retry_delay = 2
    
    # Get pod hostname for clientId
    client_id = f"producer-{socket.gethostname()}-{os.getpid()}"
    logger.info(f"Initializing Kafka clients with client ID: {client_id}")
    
    for attempt in range(max_retries):
        try:
            # Extended timeouts for Kubernetes environments
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id=f"admin-{client_id}",
                request_timeout_ms=60000,
                connections_max_idle_ms=180000,
                metadata_max_age_ms=300000
            )
            
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=5,   # Retry failed sends
                request_timeout_ms=60000,
                batch_size=16384,  # Default is 16KB
                linger_ms=10,      # Small delay for batching
                max_in_flight_requests_per_connection=5,
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=10000,
                max_block_ms=120000,
                metadata_max_age_ms=300000,
                connections_max_idle_ms=180000
            )
            
            # Test the connection
            admin_client.list_topics()
            logger.info("Successfully connected to Kafka cluster")
            return admin_client, producer
            
        except NoBrokersAvailable:
            logger.warning(f"No brokers available, retrying in {retry_delay} seconds (attempt {attempt+1}/{max_retries})")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}, retrying in {retry_delay} seconds (attempt {attempt+1}/{max_retries})")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
    
    logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
    raise ConnectionError(f"Could not connect to Kafka brokers at {bootstrap_servers}")

# Main function to produce messages to Kafka
def produce_messages(topic, bootstrap_servers, start_date, end_date, messages_per_day, batch_size, csv_path, replication_factor, partitions):
    try:
        # Setup Kafka admin client and producer with retry logic
        admin_client, producer = setup_kafka_clients(bootstrap_servers)
        
        # Create topic if it doesn't exist
        if not create_topic_if_not_exists(admin_client, topic, replication_factor, partitions):
            logger.error("Failed to create/verify topic. Exiting.")
            return
        
        # CSV file path
        vin_numbers, weights = load_vin_numbers(csv_path)

        # Track total count of DataEntries and messages
        total_data_entries_count = 0
        total_messages_sent = 0
        vin_data_entries = {vin: 0 for vin in vin_numbers}

        # Send a test message
        logger.info(f"Sending test message to topic: {topic}")
        try:
            test_timestamp = datetime.now()
            test_vin = vin_numbers[0]
            test_data = generate_campaign_data(test_vin, test_timestamp)
            test_timestamp_ms = int(test_timestamp.timestamp() * 1000)
            future = producer.send(topic, test_data, timestamp_ms=test_timestamp_ms)
            record_metadata = future.get(timeout=30)
            logger.info(f"Test message delivered to {record_metadata.topic} [partition={record_metadata.partition}, offset={record_metadata.offset}]")
        except Exception as e:
            logger.error(f"Failed to send test message: {e}")
            logger.info("Continuing with regular messages anyway...")

        current_date = start_date
        while current_date <= end_date:
            day_start_time = time.time()
            
            # Introduce high daily variation
            variation_factors = [0.2, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
            daily_factor = random.choice(variation_factors)
            daily_messages = max(batch_size, int(messages_per_day * daily_factor))  # Ensure at least one batch
            
            logger.info(f"Simulating data for date: {current_date}")
            logger.info(f"Messages for today: {daily_messages} (Factor: {daily_factor})")
            
            # Calculate the time interval between batches
            seconds_per_simulated_day = 10  # Compress a day into this many seconds
            batches_per_day = daily_messages // batch_size
            interval = seconds_per_simulated_day / batches_per_day if batches_per_day > 0 else 1

            for batch in range(batches_per_day):
                batch_start_time = time.time()
                batch_messages = []
                
                # Generate batch_size messages
                for _ in range(batch_size):
                    # Generate a timestamp within the 24-hour period of the current date
                    simulated_seconds = random.randint(0, 24 * 60 * 60 - 1)
                    timestamp = current_date + timedelta(seconds=simulated_seconds)
                    message_timestamp_ms = int(timestamp.timestamp() * 1000)
                    
                    # Select a VIN based on weights
                    vin_number = random.choices(vin_numbers, weights=weights, k=1)[0]
                    
                    # Generate campaign data
                    campaign_data = generate_campaign_data(vin_number, timestamp)
                    data_entries_count = len(campaign_data["DataEntries"])
                    total_data_entries_count += data_entries_count
                    vin_data_entries[vin_number] += data_entries_count
                    
                    # Add to batch with timestamp
                    batch_messages.append((topic, campaign_data, message_timestamp_ms))

                # Send batch of messages
                send_futures = []
                for msg_topic, msg_data, msg_timestamp in batch_messages:
                    future = producer.send(msg_topic, msg_data, timestamp_ms=msg_timestamp)
                    send_futures.append(future)
                
                # Wait for all messages in batch to be sent
                try:
                    for future in send_futures:
                        future.get(timeout=10)
                    total_messages_sent += len(batch_messages)
                    producer.flush()  # Ensure all messages are sent
                except KafkaError as e:
                    logger.error(f'Batch delivery failed: {e}')

                # Print batch statistics
                logger.info(f"Batch {batch + 1}/{batches_per_day} complete. Total messages sent: {total_messages_sent}")
                logger.info(f"Total DataEntries Count: {total_data_entries_count}")

                # Calculate sleep time to maintain the desired rate
                elapsed_time = time.time() - batch_start_time
                sleep_time = max(0, interval - elapsed_time)
                time.sleep(sleep_time)

            day_end_time = time.time()
            day_duration = day_end_time - day_start_time
            logger.info(f"Day {current_date} simulated in {day_duration:.2f} seconds")
            logger.info("=================================================================================")

            current_date += timedelta(days=1)

        # Print summary
        logger.info(f"Simulation complete. Total messages sent: {total_messages_sent}")
        
        # Print data entry distribution for VINs (top 10)
        logger.info("\nData Entry Distribution per VIN (top 10):")
        for vin, count in sorted(vin_data_entries.items(), key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"VIN: {vin}, Data Entries: {count}")
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer encountered an error: {e}")
    finally:
        # Ensure resources are properly closed
        try:
            producer.close()
            admin_client.close()
            logger.info("Kafka clients closed")
        except Exception as e:
            logger.error(f"Error closing Kafka clients: {e}")


# Run the producer only when the script is executed directly
if __name__ == "__main__":
    # Parse start and end dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else datetime.now().replace(day=1)
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else start_date + timedelta(days=30)

    logger.info(f"Starting Kafka producer for topic: {args.topic}")
    logger.info(f"Bootstrap servers: {args.bootstrap_servers}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Messages per day: {args.messages_per_day}, Batch size: {args.batch_size}")
    
    produce_messages(
        args.topic, 
        bootstrap_servers, 
        start_date, 
        end_date, 
        args.messages_per_day, 
        args.batch_size,
        args.csv_path,
        args.replication_factor,
        args.partitions
    )