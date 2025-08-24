import json
import random
import time
import csv
from datetime import datetime

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

# Function to generate a single data entry
def generate_data_entry():
    data_options = [
        ("CPU", "Unsigned Short Int", random.randint(0, 100)),
        ("Global_Temperature", "Double", random.randint(-10, 60)),
        ("Vehicle_Speed", "Unsigned Short Int", random.randint(0, 200))
    ]
    
    data_name, data_type, data_value = random.choice(data_options)
    
    return {
        "DataName": data_name,
        "DataType": data_type,
        "DataValue": str(data_value),
        "TimeStamp": str(int(time.time() * 1000))  # Milliseconds timestamp
    }

# Function to generate the full message
def generate_message(vin_number):
    num_entries = random.randint(50, 100)
    data_entries = [generate_data_entry() for _ in range(num_entries)]
    
    message = {
        "Campaign_ID": "Campaign_ID_1",
        "DataEntries": data_entries,
        "VinNumber": vin_number
    }
    
    return message

# Main function to generate and print messages
def main():
    # CSV file path
    csv_file = r'C:\Users\mark.girgis\OneDrive - VxLabs GmbH\DataPlatform\uraeus-dataplatform\kafka_clients\logs_simulators\car_models_vin.csv'
    vin_numbers = load_vin_numbers(csv_file)
    
    if not vin_numbers:
        print("No VIN numbers loaded. Exiting.")
        return

    vin_index = 0

    # Initialize total count of DataEntries
    total_data_entries_count = 0

    while True:
        vin_number = vin_numbers[vin_index]
        message = generate_message(vin_number)

        # Count the number of DataEntries in the current message
        data_entries_count = len(message["DataEntries"])
        total_data_entries_count += data_entries_count

        # Print the message and counts
        print(json.dumps(message, indent=2))
        print(f"Count of DataEntries: {data_entries_count}")
        print(f"Total DataEntries Count: {total_data_entries_count}")
        print("=================================================================================")

        vin_index = (vin_index + 1) % len(vin_numbers)  # Cycle through VIN numbers
        time.sleep(2)  # Adjust sleep time as needed

if __name__ == "__main__":
    main()
