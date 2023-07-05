import psycopg2
import pandas as pd
import re
from predictions_config import db_config

# Connect to PostgreSQL database
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Allow the user to input a file path
file_path = input("Enter the file path: ")

# Pase the VMS Log .txt file
df = pd.read_csv(file_path, sep=';', header=None).iloc[: , :-1]

# Define columns
df.columns = names=['Timestamp','id','Location','Display Status','Message','Traffic Time','Transit Time','Traffic/Train Ratio']
df['Timestamp'] = pd.to_datetime(df.Timestamp)

# Define a function to parse messages and remove excess text
def parse_message(field):
    subfields = re.split(r'\[nl\]|\[np\]', field)
    return ' '.join(subfields)

# Apply the parsing function to the Message column
df['Message'] = df['Message'].apply(lambda x: parse_message(x))

# Dataframe with only Commuter Rail Stops
CR_signs = df[df.Location.isin(['Newburyport','Beverly'])]

# Define a function to parse the message and extract departure times
def extract_time(message):
    times = re.findall(r'AT (\d{2}:\d{2})', message)
    return times

# Create an empty DataFrame to store inconsistencies
inconsistencies = pd.DataFrame(columns=['Timestamp', 'Location', 'Message'])

# Buffer time in minutes to account for slight timestamp differences
time_buffer = 0

# Iterate over each row in the DataFrame
for index, row in CR_signs.iterrows():
    timestamp = row['Timestamp']
    location = row['Location']
    message = row['Message']
    id = row['id']

    # Parse the message to obtain expected departure times
    expected_times = extract_time(message)

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
    print(f'{len(inconsistencies)} inconsistencies were found.')
    print(inconsistencies)
else:
    print('No inconsistencies found.')