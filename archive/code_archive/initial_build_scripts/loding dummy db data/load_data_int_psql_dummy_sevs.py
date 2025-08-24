import psycopg2

# Connect to the PostgreSQL database
connection = psycopg2.connect(
    host="127.0.0.1",
    port="9001",
    database="test_db",
    user="postgres",
    password="mysecretpassword"
)

# Create a cursor object
cursor = connection.cursor()

# Drop the cars table if it exists to avoid conflicts
cursor.execute("DROP TABLE IF EXISTS security_event_lookup;")

# Create the table if it does not exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS security_event_lookup (
    SEV_ID VARCHAR(10),
    RULE_ID VARCHAR(10),  -- This will store the same ID as generated in the script
    SEV_NAME VARCHAR(100),
    DESCRIPTION TEXT                  -- Description of the security event
);
'''
cursor.execute(create_table_query)
print("Table 'security_event_lookup' created successfully.")

# Insert 20 predefined values into the table
insert_query = '''
INSERT INTO security_event_lookup (SEV_ID, RULE_ID, SEV_NAME, DESCRIPTION)
VALUES (%s, %s, %s, %s)
'''

# Define 20 security event entries
security_events = [
    ('001', '001', 'Invalid CAN ID', 'Invalid Can Message ID was sent on the CAN network'),
    ('002', '002', 'CPU Usage', 'Uraeus hosting ECU is facing a High CPU usage that might cause system issue.'),
    ('003', '003', 'Bus Flood Attack', 'Very simple denial-of-service attack: transmit CAN frames as fast as possible to soak up bus bandwidth, cause legitimate frames to be delayed and for parts of the system to fail when frames don’t turn up on time.'),
    ('004', '004', 'IdsM Sev', 'IdsM Security Event.'),
    ('005', '005', 'NRC', 'NRC Security Event.'),
    ('0006', '0006', 'AI Model Detected Fuzzing', 'Fuzzing Attack Detected'),
    ('0007', '0007', 'AI Model Detected Spoofing', 'Spoofing Attack Detected'),
    ('007', '007', 'Unknown MAC address', 'Unknown IP address event detected'),
    ('008', '008', 'Unknown IP address', 'Unknown MAC address event detected')
]

# Insert the data into the table
cursor.executemany(insert_query, security_events)
print(f"{len(security_events)} records inserted successfully.")

# Commit the transaction
connection.commit()

# Close the cursor and the connection
cursor.close()
connection.close()
