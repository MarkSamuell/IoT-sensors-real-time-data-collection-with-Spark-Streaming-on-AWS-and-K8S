import argparse
import json
import random
import time
import csv
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import numpy as np

# Argument parsing
parser = argparse.ArgumentParser(description='Kafka producer that simulates security events with VIN numbers over a date range.')
parser.add_argument('topic', type=str, help='Kafka topic name to produce messages to')
parser.add_argument('bootstrap_servers', type=str, help='Comma-separated list of Kafka bootstrap servers')
parser.add_argument('--start_date', type=str, default=None, help='Start date for simulation (YYYY-MM-DD)')
parser.add_argument('--end_date', type=str, default=None, help='End date for simulation (YYYY-MM-DD)')
parser.add_argument('--events_per_day', type=int, default=10000, help='Base number of security events to produce per day')
parser.add_argument('--batch_size', type=int, default=100, help='Number of events to send in each batch')
args = parser.parse_args()

bootstrap_servers = args.bootstrap_servers.split(',')

def load_vin_numbers(csv_file):
   """Load VIN numbers from CSV file with weights"""
   try:
       vin_numbers = []
       with open(csv_file, 'r') as file:
           reader = csv.reader(file)
           header = next(reader)  # Skip the header row
           for row in reader:
               if len(row) > 1:
                   vin_numbers.append(row[1])  # VIN is in the second column
               else:
                   print(f"Skipping row with insufficient columns: {row}")
       
       if not vin_numbers:
           print("No VIN numbers found in CSV. Using fallback sample VINs.")
           vin_numbers = ["1HGCM82633A123456", "JH4KA7532NC123456", "WAUAC48H86K123456"]
           
       # Assign weights using Pareto distribution for realistic frequency
       weights = np.random.pareto(a=1.5, size=len(vin_numbers))
       weights /= weights.sum()  # Normalize weights
       
       print(f"Loaded {len(vin_numbers)} VIN numbers from {csv_file}")
       return vin_numbers, weights.tolist()
   except FileNotFoundError:
       print(f"CSV file {csv_file} not found, using fallback data")
       fallback_vins = ["1HGCM82633A123456", "JH4KA7532NC123456", "WAUAC48H86K123456"]
       return fallback_vins, [0.33, 0.33, 0.34]

def create_topic_if_not_exists(admin_client, topic_name):
   """Create Kafka topic if it doesn't exist"""
   try:
       topics = admin_client.list_topics()
       if topic_name not in topics:
           print(f"Creating topic: {topic_name}")
           new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=1)
           admin_client.create_topics(new_topics=[new_topic], validate_only=False)
           print(f"Topic {topic_name} created.")
       else:
           print(f"Topic {topic_name} already exists.")
   except KafkaError as e:
       print(f"Failed to create topic {topic_name}: {e}")

def generate_parameter(param_config, timestamp):
   """Generate parameter with correct field mappings"""
   return {
       "Timestamp": str(timestamp),
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
       },
       {
           "name": "Vehicle.Acceleration.Lateral",
           "type": "Double",
           "value": random.uniform(-5.0, 5.0),
           "unit": "m/s²"
       },
       {
           "name": "Vehicle.Acceleration.Longitudinal",
           "type": "Double",
           "value": random.uniform(-10.0, 5.0),
           "unit": "m/s²"
       },
       {
           "name": "Vehicle.Acceleration.Vertical",
           "type": "Double",
           "value": random.uniform(-3.0, 3.0),
           "unit": "m/s²"
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

   # Define sev_messages for all rule IDs
   sev_messages = {
       "001": "Invalid Can ID Message",
       "002": "High CPU Usage Message",
       "003": "Bus Flood Attack Message",
       "004": "IdsM SEV_MSG",
       "005": "Negative Response Code",
       "007": "Unknown MAC address",
       "008": "Unknown IP address",
       "010": "Man in The Middle",
       "011": "Replay Attack",
       "012": "Spoofing ECU Comm",
       "32776": "Unexpected Periodicity SEv for CAN",
       "32777": "Access Control Monitor Violation SEv",
       "32778": "Falco Monitor SEV",
       "013": "REPLAY_SEV",
       "014": "MITM_SEV",
       "015": "SPOOF_SEV"
   }
   sev_msg = sev_messages.get(rule_id, "Unknown Security Event")

   # Handle IoC parameters based on rule_id
   if rule_id in ["005", "0006", "0007"]:
       # Always include exactly one standard parameter
       ioc_parameters = [generate_parameter(random.choice(standard_params), timestamp)]
   else:
       if random.choice([True, False]):
           ioc_parameters = []
       else:
           # Select up to 2 standard parameters
           selected_params = random.sample(standard_params, k=random.randint(1, min(2, len(standard_params))))
           ioc_parameters = [generate_parameter(param, timestamp) for param in selected_params]

           # Add random monitoring parameters (0 to 3)
           num_additional = random.randint(0, 3)
           for i in range(num_additional):
               monitor_param = generate_random_monitoring_parameter(i)
               ioc_parameters.append(generate_parameter(monitor_param, timestamp))

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
   # Setup Kafka clients
   admin_client = KafkaAdminClient(
       bootstrap_servers=bootstrap_servers,
       request_timeout_ms=60000
   )

   # Create topic if it doesn't exist
   create_topic_if_not_exists(admin_client, args.topic)

   # Create producer
   producer = KafkaProducer(
       bootstrap_servers=bootstrap_servers,
       value_serializer=lambda v: json.dumps(v).encode('utf-8'),
       request_timeout_ms=60000,
       reconnect_backoff_ms=1000,
       max_block_ms=120000,
       metadata_max_age_ms=300000,
       connections_max_idle_ms=60000
   )

   # Load VIN numbers
   csv_file = './car_models_vin.csv'
   vin_numbers, weights = load_vin_numbers(csv_file)

   # All rule IDs including the new ones
   predefined_rule_ids = [
       '001', '002', '003', '004', '005', '0006', '0007', '007', '008', 
       '010', '011', '012', '32776', '32777', '32778', '0x800B', '013', '014', '015'
   ]

   # Initialize counters
   vin_index = 0
   rule_index = 0
   total_vins = len(vin_numbers)
   total_security_events = 0
   total_ioc_parameters = 0
   total_messages_sent = 0
   vin_event_counts = {vin: 0 for vin in vin_numbers}

   # Parse start and end dates
   start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else datetime.now().replace(day=1)
   end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else start_date + timedelta(days=30)

   print(f"Simulating data from {start_date} to {end_date}")
   print(f"Events per day: {args.events_per_day}, Batch size: {args.batch_size}")

   current_date = start_date
   while current_date <= end_date:
       day_start_time = time.time()
       
       # Introduce high daily variation
       variation_factors = [0.2, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
       daily_factor = random.choice(variation_factors)
       daily_events = max(args.batch_size, int(args.events_per_day * daily_factor))  # Ensure at least one batch
       
       print(f"Simulating data for date: {current_date}")
       print(f"Events for today: {daily_events} (Factor: {daily_factor})")
       
       # Calculate the time interval between batches
       seconds_per_simulated_day = 10  # Compress a day into this many seconds
       batches_per_day = daily_events // args.batch_size
       interval = seconds_per_simulated_day / batches_per_day if batches_per_day > 0 else 1

       for batch in range(batches_per_day):
           batch_start_time = time.time()
           accumulated_events = []
           
           # Generate batch_size messages
           for _ in range(args.batch_size):
               # Generate a timestamp within the 24-hour period of the current date
               simulated_seconds = random.randint(0, 24 * 60 * 60 - 1)
               timestamp = current_date + timedelta(seconds=simulated_seconds)
               epoch_timestamp = int(timestamp.timestamp() * 1000)
               
               # Select a VIN based on weights
               vin_number = vin_numbers[np.random.choice(len(vin_numbers), p=weights)]
               rule_id = predefined_rule_ids[rule_index]
               
               security_event = create_security_event(vin_number, rule_id, epoch_timestamp)
               accumulated_events.append(security_event)
               
               # Count IoC parameters and track VIN event counts
               total_ioc_parameters += len(security_event["IoC"]["Parameters"])
               vin_event_counts[vin_number] += 1
               
               # Move to the next rule_id in a circular way
               rule_index = (rule_index + 1) % len(predefined_rule_ids)

           # Send accumulated events
           if accumulated_events:
               try:
                   message_timestamp = int(timestamp.timestamp() * 1000)  # Use the last timestamp for the message
                   future = producer.send(args.topic, accumulated_events, timestamp_ms=message_timestamp)
                   record_metadata = future.get(timeout=30)
                   total_messages_sent += 1
                   total_security_events += len(accumulated_events)
                   
                   # Print batch info every 10 batches
                   if batch % 10 == 0:
                       print(f"Batch {batch + 1}/{batches_per_day} complete. Total events: {total_security_events}")
                       print(f"Total IoC Parameters: {total_ioc_parameters}")
                       print(f"Total messages sent: {total_messages_sent}")
               except KafkaError as e:
                   print(f"Message delivery failed: {e}")

           # Calculate sleep time to maintain the desired rate
           elapsed_time = time.time() - batch_start_time
           sleep_time = max(0, interval - elapsed_time)
           time.sleep(sleep_time)

       day_end_time = time.time()
       day_duration = day_end_time - day_start_time
       print(f"Day {current_date} simulated in {day_duration:.2f} seconds")
       print("=================================================================================")

       current_date += timedelta(days=1)

   # Print summary
   print(f"Simulation complete. Total security events sent: {total_security_events}")
   
   # Print distribution of events per VIN (top 10)
   print("\nDistribution of events per VIN (top 10):")
   for vin, count in sorted(vin_event_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
       print(f"VIN: {vin}, Events: {count}")
   
   # Ensure resources are properly closed
   producer.close()
   admin_client.close()

if __name__ == '__main__':
   main()