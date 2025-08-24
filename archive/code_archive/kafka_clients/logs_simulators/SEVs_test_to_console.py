import json
import random
import time
import csv
from datetime import datetime
import uuid  # For generating unique IDs

# Convert current time to epoch format
def get_epoch_time():
    return int(datetime.utcnow().timestamp())

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

# Create a security event based on multiple conditions
def create_security_event(vin_number, cpu_data, temp_data, speed_data, network_type=None, network_id=None):
    # Generate a timestamp for this message in epoch format
    timestamp = get_epoch_time()

    # Randomly decide if IoC parameters should be empty
    if random.choice([True, False]):
        ioc_parameters = []
    else:
        # Randomly select parameters for IoC
        parameters = [cpu_data, temp_data, speed_data]
        selected_params = random.sample(parameters, k=min(len(parameters), 2))  # Select up to 2 parameters

        # Generate a random number of additional IoC parameters (0 to 10)
        additional_ioc_params = [generate_random_parameter(vin_number, f"Random_Param_{i}", "Type", random.randint(0, 100), "unit") for i in range(random.randint(0, 10))]

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

# CSV file path
csv_file = r"C:\Users\mark.girgis\OneDrive - VxLabs GmbH\DataPlatform\uraeus-dataplatform\kafka_clients\logs_simulators\car_models_vin.csv"
vin_numbers = load_vin_numbers(csv_file)

# Initialize indices
vin_index = 0
security_event_count = 0  # Initialize security event count

while True:
    if not vin_numbers:
        print("No VIN numbers loaded. Exiting.")
        break
    
    print(f"VIN index: {vin_index}, Total VINs: {len(vin_numbers)}")
    
    # Generate random parameters
    cpu_data = generate_random_parameter(vin_numbers[vin_index], "CPU", "Usage", random.randint(0, 100), "%")
    temp_data = generate_random_parameter(vin_numbers[vin_index], "Global_Temperature", "Temperature", random.randint(-10, 60), "celsius_degree")
    speed_data = generate_random_parameter(vin_numbers[vin_index], "Vehicle_Speed", "Speed", random.randint(0, 200), "km/h")

    # Print generated parameters to console
    print(f"Generated CPU Data: {json.dumps(cpu_data, indent=2)}")
    print(f"Generated Temperature Data: {json.dumps(temp_data, indent=2)}")
    print(f"Generated Speed Data: {json.dumps(speed_data, indent=2)}")

    # Randomly decide whether to include network info
    include_network_info = random.choice([True, False])
    if include_network_info:
        network_type = random.choice(["Ethernet", "CAN"])
        network_id = random.choice(["0001", "0002", "0003", "0004", "0005"])

        # Print network type and ID to console
        print(f"Selected Network Type: {network_type}")
        print(f"Selected Network ID: {network_id}")
    else:
        network_type = None
        network_id = None
        print("Network info is omitted for this event.")

    # Check for security events
    security_event = create_security_event(vin_numbers[vin_index], cpu_data, temp_data, speed_data, network_type, network_id)
    if security_event:
        security_event_count += 1  # Increment the security event count
        print("=================================SECURITY=EVENT============================")
        print("Security Event: ", json.dumps(security_event, indent=2))
        print("===========================================================================")

    # Print security event count
    print(f"Total Security Events Generated: {security_event_count}")

    # Increment index
    vin_index += 1
    if vin_index >= len(vin_numbers):
        vin_index = 0  # Reset index if we reach the end

    # Delay for a short period before next iteration
    time.sleep(2)
