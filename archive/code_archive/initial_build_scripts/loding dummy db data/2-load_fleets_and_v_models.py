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
    
    # Step 1: Insert vehicle models
    for record in records:
        # Check if vehicle model exists
        cursor.execute("SELECT id FROM vehicle_model WHERE name = %s AND is_deleted = false", (record['Car Model'],))
        result = cursor.fetchone()
        
        if result is None:
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

    # Step 2: Insert fleets
    for record in records:
        # Check if fleet exists
        cursor.execute("SELECT id FROM fleet WHERE business_id = %s AND is_deleted = false", (record['Fleet ID'],))
        result = cursor.fetchone()
        
        if result is None:
            cursor.execute("""
                INSERT INTO fleet (
                    name, 
                    description, 
                    status, 
                    is_deleted, 
                    business_id,
                    region
                )
                VALUES (%s, %s, 'Active', false, %s, %s)
                RETURNING id;
            """, (
                record['Fleet Name'], 
                f'Fleet description for {record["Fleet Name"]}', 
                record['Fleet ID'],
                record.get('Region', 'EU')
            ))
            record['fleet_id'] = cursor.fetchone()[0]
        else:
            record['fleet_id'] = result[0]
        
        # Step 3: Link fleet with vehicle model - using fleet_vehicle_models
        cursor.execute("""
            SELECT 1 FROM fleet_vehicle_models 
            WHERE fleet_id = %s AND vehicle_model_id = %s
        """, (record['fleet_id'], record['vehicle_model_id']))
        
        if cursor.fetchone() is None:
            cursor.execute("""
                INSERT INTO fleet_vehicle_models (fleet_id, vehicle_model_id)
                VALUES (%s, %s);
            """, (record['fleet_id'], record['vehicle_model_id']))

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