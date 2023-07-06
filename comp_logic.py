import psycopg2
import pandas as pd
import re
import tkinter as tk
from tkinter import filedialog
import matplotlib.pyplot as plt
from predictions_config import db_config
from visuals import plot_hourly_inconsistencies, plot_minute_inconsistencies

# Connect to PostgreSQL database
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Define a function to parse messages and remove excess text
def parse_message(field):
    subfields = re.split(r'\[nl\]|\[np\]', field)
    return ' '.join(subfields)

def extract_time(message):
    times = re.findall(r'AT (\d{2}:\d{2})', message)
    return times

# Define a function to process the data
def process_data(df):
    # Filter dataframe for desired locations and logic state
    filtered_df = df[(df['Location'].isin(['Newburyport', 'Beverly'])) & (df['Logic State'] == 'Normal')]

    # Create an empty DataFrame to store inconsistencies
    inconsistencies = pd.DataFrame(columns=['Timestamp', 'Location', 'Message'])

    # Iterate over each row in the DataFrame
    for index, row in filtered_df.iterrows():
        timestamp = row['Timestamp']
        location = row['Location']
        message = row['Message']
        #inrix_time = row['Inrix Time']

        # Parse the message to obtain reported departure times
        reported_times = extract_time(message)

        # Time buffer variable
        time_buffer = 15 if location == 'Newburyport' else 20
        #time_buffer = inrix_time + 5 if location == 'Newburyport' else inrix_time + 10

        # Calculate the rounded down timestamp and subtract the buffer time
        buffered_timestamp = pd.to_datetime(timestamp) + pd.Timedelta(minutes=time_buffer)

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

        # Check if any results were returned by the query
        if actual_times:
            # Compare expected and actual departure times
            if set(reported_times) != set(actual_times):
                # Inconsistency found, store the details in the inconsistencies DataFrame
                inconsistencies = pd.concat([inconsistencies, pd.DataFrame({'Timestamp': [timestamp], 'Location': [location], 'Message': [message], 'Actual Times' : [actual_times]})], ignore_index=True)
        else:
            inconsistencies = pd.concat([inconsistencies, pd.DataFrame({'Timestamp': [timestamp], 'Location': [location], 'Message': [message], 'Actual Times': 'No Data'})], ignore_index=True)

    # Close the database connection
    cursor.close()
    conn.close()

    # Display the inconsistencies DataFrame
    if not inconsistencies.empty:
        print(f"{len(inconsistencies)} inconsistencies were found.")
        for loc in inconsistencies["Location"].unique():
            print(f"{len(inconsistencies[inconsistencies['Location'] == loc])} inconsistencies for signs at {loc}.")
        print(inconsistencies)
        #plot_hourly_inconsistencies(inconsistencies)
        #plot_minute_inconsistencies(inconsistencies)
        #inconsistencies.to_csv('vmslog_inconsistencies', index=False)
    else:
        print('No inconsistencies found.')

# Define a function that allows the user to select a file
def select_file():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    if file_path:
        df = pd.read_csv(file_path, sep=';', header=None).iloc[:, :-1]
        df.columns = ['Timestamp', 'Sign ID', 'Location', 'Logic State', 'Message', 'Highway TT', 'Transit TT', 'Highway/Transit Ratio'] #ADD INRIX WHEN AVAILABLE
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df['Message'] = df['Message'].apply(parse_message)
        process_data(df)

select_file()