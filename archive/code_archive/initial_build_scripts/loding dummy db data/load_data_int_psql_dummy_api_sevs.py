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
cursor.execute("DROP TABLE IF EXISTS api_security_event_lookup;")

# Create the table if it does not exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS api_security_event_lookup (
    SEV_ID VARCHAR(10),
    SEV_NAME VARCHAR(100),
    DESCRIPTION TEXT,
    SEVERITY VARCHAR(20)
);
'''
cursor.execute(create_table_query)
print("Table 'api_security_event_lookup' created successfully.")

# Insert 20 predefined values into the table
insert_query = '''
INSERT INTO api_security_event_lookup (SEV_ID, SEV_NAME, DESCRIPTION, SEVERITY)
VALUES (%s, %s, %s, %s)
'''

# Define 20 security event entries
security_events = [
    ('1', 'Malicious IP', 'Malicious ip detected.', 'High'),
    ('2', 'XSS', 'Cross site scripting payload detected in the http request.', 'Medium'),
    ('3', 'SQLI', 'Sql injection payload detected in the HTTP request.', 'Critical'),
]

# Insert the data into the table
cursor.executemany(insert_query, security_events)
print(f"{len(security_events)} records inserted successfully.")

# Commit the transaction
connection.commit()

# Close the cursor and the connection
cursor.close()
connection.close()
