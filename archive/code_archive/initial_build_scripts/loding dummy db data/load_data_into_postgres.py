import psycopg2

connection = psycopg2.connect(
    host="127.0.0.1",
    port="9001",
    database="test_db",
    user="postgres",
    password="mysecretpassword"
)

try:
    cursor = connection.cursor()
    
    # Drop the cars table if it exists
    cursor.execute("DROP TABLE IF EXISTS fleet_cars;")
    
    # Create the fleet_cars table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS "fleet_cars" (
        "Car Model" VARCHAR(255),
        "VIN Number" VARCHAR(255),
        "Fleet ID" VARCHAR(255),
        "Fleet Name" VARCHAR(255)
    );
    """)
    
    # Load data from CSV
    csv_file_path = r'C:\Users\mark.girgis\OneDrive - VxLabs GmbH\DataPlatform\archive\initial_build_scripts\car_modes_with_fleet.csv'
    with open(csv_file_path, 'r') as file:
        cursor.copy_expert(
            sql='''COPY "fleet_cars" ("Car Model", "VIN Number", "Fleet ID", "Fleet Name")
                   FROM stdin WITH CSV HEADER''',
            file=file
        )
    
    # Insert data into vehicle table
    cursor.execute("""
        INSERT INTO vehicle (make_name, vin, status, is_deleted)
        SELECT 
            "Car Model",
            "VIN Number",
            'Active'::vehicle_status_enum,
            false
        FROM fleet_cars;
    """)
    
    connection.commit()
    print("Data loaded and inserted successfully")

except Exception as e:
    print(f"Error: {e}")
    connection.rollback()
    
finally:
    cursor.close()
    connection.close()