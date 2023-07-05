import psycopg2
import pandas as pd
import re
from predictions_config import db_config

# Connect to PostgreSQL database
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Sample DataFrame with parsed data
data = {
    'Timestamp': [
        '2023-07-03 10:54:22',
        '2023-07-03 10:54:30',
        # ...
    ],
    'Location': [
        'Beverly',
        'Newburyport',
        # ...
    ],
    'Message': [
        'FASTER;ROUTE;USE MBTA;TRAINS;AT 11:20;AT 11:50',
        'FASTER;ROUTE;USE MBTA;TRAINS;AT 11:24;AT 12:54',
        # ...
    ]
}

df = pd.DataFrame(data)

# Define a function to parse the message and extract departure times
def parse_message(message):
    times = re.findall(r'AT (\d{2}:\d{2})', message)
    return times

# Create an empty DataFrame to store inconsistencies
inconsistencies = pd.DataFrame(columns=['Timestamp', 'Location', 'Message'])

# Buffer time in minutes to account for slight timestamp differences
time_buffer = 0

# Iterate over each row in the DataFrame
for index, row in df.iterrows():
    timestamp = row['Timestamp']
    location = row['Location']
    message = row['Message']

    # Parse the message to obtain expected departure times
    expected_times = parse_message(message)

    # Calculate the rounded down timestamp and subtract the buffer time
    buffered_timestamp = pd.to_datetime(timestamp) - pd.Timedelta(minutes=time_buffer)

    # Query the PostgreSQL database for matching records
    query = """
            SELECT depart_time 
            FROM response_data 
            WHERE date_trunc('minute', timestamp) = date_trunc('minute', %s)
            AND depart_station = %s;
            """
    cursor.execute(query, (buffered_timestamp, location))
    queried_records = cursor.fetchall()

    # Extract actual departure times from the queried records
    actual_times = [record[0].strftime('%H:%M') for record in queried_records]

    # Compare expected and actual departure times
    if set(expected_times) != set(actual_times):
        # Inconsistency found, store the details in the inconsistencies DataFrame
        inconsistencies = pd.concat([inconsistencies, pd.DataFrame({'Timestamp': [timestamp], 'Location': [location], 'Message': [message]})], ignore_index=True)

# Close the database connection
cursor.close()
conn.close()

# Display the inconsistencies DataFrame
if not inconsistencies.empty:
    print(inconsistencies)
else:
    print('No inconsistencies found.')