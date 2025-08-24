import psycopg2
import csv

# Connect to the PostgreSQL database
connection = psycopg2.connect(
   host="k8s-stg-postgres-08c0537730-dbdb8651674d2bce.elb.eu-central-1.amazonaws.com",
   port="5432",
   database="fleetconnect_db",
   user="postgres",
   password="mysecretpassword"
)

try:
    cursor = connection.cursor()
    
    # Read the CSV file
    csv_file_path = r'C:\Users\mark.girgis\OneDrive - VxLabs GmbH\DataPlatform\archive\initial_build_scripts\car_modes_with_fleet.csv'
    
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        records = list(csv_reader)
    
    # First ensure all vehicle models exist and get their IDs
    for record in records:
        # Check if vehicle model exists
        cursor.execute("SELECT id FROM vehicle_model WHERE name = %s AND is_deleted = false", (record['Car Model'],))
        result = cursor.fetchone()
        
        if result is None:
            # Insert new vehicle model if it doesn't exist
            cursor.execute("""
                INSERT INTO vehicle_model (
                    name, 
                    description, 
                    is_deleted,
                    business_id,
                    manufacture,
                    model_year,
                    architecture_type,
                    region
                )
                VALUES (%s, %s, false, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                record['Car Model'], 
                f'Model description for {record["Car Model"]}',
                record.get('Business ID', 'DEFAULT_BIZ_ID'),
                record.get('Manufacture', 'DEFAULT_MANUFACTURE'),
                record.get('Model Year', '2023'),
                record.get('Architecture Type', 'DEFAULT_ARCHITECTURE'),
                record.get('Region', 'EU')
            ))
            record['vehicle_model_id'] = cursor.fetchone()[0]
        else:
            record['vehicle_model_id'] = result[0]

    # Now insert vehicles
    for record in records:
        # Check if vehicle exists
        cursor.execute("SELECT id FROM vehicle WHERE vin = %s AND is_deleted = false", (record['VIN Number'],))
        if cursor.fetchone() is None:
            cursor.execute("""
                INSERT INTO vehicle (
                    make_name,
                    vin,
                    is_deleted,
                    vehicle_model_id,
                    business_id,
                    manufacture,
                    model_year,
                    architecture_type,
                    status,
                    trim_level
                )
                VALUES (%s, %s, false, %s, %s, %s, %s, %s, 'Active', %s);
            """, (
                record['Car Model'],  # make_name
                record['VIN Number'], # vin
                record['vehicle_model_id'],  # vehicle_model_id
                record.get('Business ID', 'DEFAULT_BIZ_ID'),
                record.get('Manufacture', 'DEFAULT_MANUFACTURE'),
                record.get('Model Year', '2023'),
                record.get('Architecture Type', 'DEFAULT_ARCHITECTURE'),
                record.get('Trim Level', 'STANDARD')
            ))
            
            # Get the newly inserted vehicle ID
            cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (record['VIN Number'],))
            vehicle_id = cursor.fetchone()[0]
            
            # Link vehicle to its fleet in fleet_vehicles_vehicle table
            cursor.execute("SELECT id FROM fleet WHERE business_id = %s", (record['Fleet ID'],))
            fleet_result = cursor.fetchone()
            
            if fleet_result:
                fleet_id = fleet_result[0]
                cursor.execute("""
                    INSERT INTO fleet_vehicles_vehicle (fleet_id, vehicle_id)
                    VALUES (%s, %s);
                """, (fleet_id, vehicle_id))

    # Commit changes
    connection.commit()

except Exception as e:
    print(f"Error: {e}")
    connection.rollback()

finally:
    # Close cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()