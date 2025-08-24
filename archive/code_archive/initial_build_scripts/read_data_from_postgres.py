import psycopg2

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        host="127.0.0.1",
        port="9001",
        database="test_db",
        user="postgres",
        password="postgres"
    )
    print("Connection successful")

    # Create a cursor object using the connection
    cursor = connection.cursor()

    # Define the query to select all columns from the users table
    query = "SELECT make_name, vin FROM public.vehicle;"

    # Execute the query
    cursor.execute(query)

    # Fetch all results from the executed query
    results = cursor.fetchall()

    # Print the results
    for row in results:
        print(row)

except Exception as e:
    print("Connection failed: {}".format(e))

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()
