import psycopg2
import pandas as pd
import re
import os
import tkinter as tk
from tkinter import filedialog
import matplotlib.pyplot as plt
from predictions_config import db_config

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

    # Create an empty DataFrame to store inconsistencies and message errors
    inconsistencies = pd.DataFrame(columns=['Timestamp', 'Sign ID', 'Buffered Timestamp', 'Location', 'Message', 'Scheduled Times', 'Predicted Times', 'Mismatch Type'])
    message_errors = pd.DataFrame(columns=['Timestamp', 'Sign ID', 'Buffered Timestamp', 'Location', 'Message', 'Ratio'])

    # Iterate over each row in the DataFrame
    for index, row in filtered_df.iterrows():
        timestamp = row['Timestamp']
        sign_id = row['Sign ID']
        location = row['Location']
        message = row['Message']
        parking_TT = row['Transit Parking TT']
        ratio = row['Highway/Transit Ratio']

        # Check to make sure each relevant field is not null
        if pd.isnull(timestamp) or pd.isnull(location) or pd.isnull(message) or pd.isnull(parking_TT):
            inconsistencies = pd.concat(
                [inconsistencies, pd.DataFrame(
                    {'Timestamp': [timestamp], 'Sign ID': [sign_id], 'Buffered Timestamp': None, 'Location': [location], 'Message': [message],
                    'Scheduled Times': None, 'Predicted Times': None, 'Mismatch Type': 'Missing Field'})],
                ignore_index=True
            )
            continue

        # Parse the message to obtain reported departure times
        reported_times = extract_time(message)

        # Calculate the rounded down timestamp and add the buffer time
        buffered_timestamp = pd.to_datetime(timestamp) + pd.Timedelta(minutes=parking_TT)

        # Check to make sure the message is correct
        if (ratio >= 1.2 and 'FASTER ROUTE' not in message) or ('FASTER ROUTE' in message and ratio < 1.2):
            message_errors = pd.concat(
                [message_errors, pd.DataFrame(
                    {'Timestamp': [timestamp], 'Sign ID': [sign_id], 'Buffered Timestamp': [buffered_timestamp], 'Location': [location], 'Message': [message],
                    'Ratio': [ratio]})],
                ignore_index=True
            )

        # Query the PostgreSQL database for matching records
        query = """
                SELECT depart_station, scheduled_depart_time, predicted_depart_time 
                FROM schedules_and_pred 
                WHERE date_trunc('minute', timestamp) = date_trunc('minute', %s)
                AND depart_station = %s;
                """
        cursor.execute(query, (buffered_timestamp, location))
        queried_records = cursor.fetchall()

        # Extract actual departure times from the queried records
        scheduled_times = [record[1].strftime('%H:%M') if record[1] else None for record in queried_records]
        predicted_times = [record[2].strftime('%H:%M') if record[2] else None for record in queried_records]

        # Check if any results were returned by the query
        if scheduled_times:
            unmatched_reported_times = reported_times.copy()

            # Check for match between predicted and reported times
            matched_predicted_times = set()
            for predicted_time in predicted_times:
                if predicted_time in unmatched_reported_times:
                    unmatched_reported_times.remove(predicted_time)
                    matched_predicted_times.add(predicted_time)

            # Check for match between scheduled and unmatched reported times
            matched_scheduled_times = set()
            for scheduled_time in scheduled_times:
                if scheduled_time in unmatched_reported_times:
                    unmatched_reported_times.remove(scheduled_time)
                    matched_scheduled_times.add(scheduled_time)

            # Determine mismatch type based on unmatched reported times
            if any(predicted_times) and not matched_predicted_times and matched_scheduled_times and not unmatched_reported_times:
                mismatch_type = 'Predictions'
            elif not matched_predicted_times and not matched_scheduled_times:
                mismatch_type = 'Complete'
            elif unmatched_reported_times:
                mismatch_type = 'Partial'
            else:
                continue  # Skip row if all reported times are matched

            # Store the details in the inconsistencies DataFrame
            inconsistencies = pd.concat(
                [inconsistencies, pd.DataFrame(
                    {'Timestamp': [timestamp], 'Sign ID': [sign_id], 'Buffered Timestamp': [buffered_timestamp], 'Location': [location], 'Message': [message],
                     'Scheduled Times': [scheduled_times], 'Predicted Times': [predicted_times], 'Mismatch Type': [mismatch_type]})],
                ignore_index=True
            )
        else:
            # No data available in the database for the given timestamp and location
            mismatch_type = 'No Data'
            inconsistencies = pd.concat(
                [inconsistencies, pd.DataFrame(
                    {'Timestamp': [timestamp], 'Sign ID': [sign_id], 'Buffered Timestamp': [buffered_timestamp], 'Location': [location], 'Message': [message],
                     'Scheduled Times': None, 'Predicted Times': None, 'Mismatch Type': [mismatch_type]})],
                ignore_index=True
            )
            
    return inconsistencies, message_errors

# Define a function that allows the user to select a file
def select_file():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    file_name = os.path.basename(file_path)
    if file_path:
        df = pd.read_csv(file_path, sep=';', header=None).iloc[:, :-1]
        df.columns = ['Timestamp', 'Sign ID', 'Location', 'Logic State', 'Message', 'Transit Alert IDs', 'Transit Parking TT', 'Highway TT', 'Transit Departure Time', 'Transit Arrival Time', 'Total Transit TT', 'Highway/Transit Ratio']
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df['Message'] = df['Message'].apply(parse_message)
        # The final processed dataframe
        processed, message_errors = process_data(df)

    mbta_faster_percentage = len(df[df['Message'].str.contains('FASTER ROUTE') & (df['Highway/Transit Ratio'] >= 1.2)]) / len(df)
    print(f"Percentage of Faster MBTA Route Notifications: {mbta_faster_percentage}")

    if not processed.empty:
        # Determine percentage of inconsistencies that are mismatched on either predictions or schedules
        mismatch_percentage = len(processed)/len(df)
        predictions_percentage = len(processed[processed['Mismatch Type'] == 'Predictions']) / len(processed)
        partial_mismatch = len(processed[processed['Mismatch Type'] == 'Partial']) / len(processed)
        complete_mismatch = len(processed[processed['Mismatch Type'] == 'Complete']) / len(processed)
        no_data_percentage = len(processed[processed['Mismatch Type'] == 'No Data']) / len(processed)

        #processed.to_csv(f"{file_name}_validation_output.txt", index=False)
        processed.to_csv("mismatch_output.txt", index=False)
        
        print(f"Percentage of Mismatched Entries: {mismatch_percentage}")
        print(f"Predictions Mismatch Percentage: {predictions_percentage}")
        print(f"Partial Mismatch Percentage: {partial_mismatch}")
        print(f"Complete Mismatch Percentage: {complete_mismatch}")
        print(f"No Data percentage: {no_data_percentage}")
        print(processed)
    else:
        print('No inconsistencies found.')
        
    if not message_errors.empty:
        message_errors.to_csv("message_error_output.txt", index=False)
        print('Message Errors found.')

select_file()