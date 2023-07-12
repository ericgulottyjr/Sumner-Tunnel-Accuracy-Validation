import psycopg2
import requests
from datetime import datetime, timezone, timedelta
import time
from predictions_config import db_config, API_KEY

# API endpoints
PREDICTIONS_ENDPOINT = 'https://api-v3.mbta.com/predictions'
SCHEDULES_ENDPOINT = 'https://api-v3.mbta.com/schedules'

# CR Params
CR_departure_stops = {
    'place-ER-0362': 'Newburyport',
    'place-ER-0183': 'Beverly',
}

CR_arrival_stop_id = 'BNT-0000'
CR_arrival_stop_name = 'North Station'

CR_url_params = '&filter[direction_id]=1'

# BL Params
BL_departure_stops = {
    'place-wondl': 'Wonderland',
    'place-bmmnl': 'Beachmont',
}

BL_arrival_stop_id = 'place-state'
BL_arrival_stop_name = 'State'

BL_url_params = '&filter[direction_id]=0&filter[route]=Blue'

def make_api_call(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to make API call. Status code: {response.status_code}")

def create_database_table():
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    create_table_query = '''
        CREATE TABLE IF NOT EXISTS schedules_and_pred (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            trip_id VARCHAR(255),
            depart_station VARCHAR(255),
            scheduled_depart_time TIMESTAMP,
            predicted_depart_time TIMESTAMP,
            arrive_station VARCHAR(255),
            arrive_time TIMESTAMP,
            transit_time INTERVAL
        )
    '''

    cursor.execute(create_table_query)
    conn.commit()

    cursor.close()
    conn.close()

def get_scheduled_trips(departure_stop_id, url_params):
    # Make API call to schedules endpoint for departure stop
    url = f"{SCHEDULES_ENDPOINT}?filter[stop]={departure_stop_id}{url_params}&api_key={API_KEY}"
    data = make_api_call(url)

    trip_ids = []
    departure_times = []

    current_time = datetime.now(timezone(timedelta(hours=-4)))

    # Get the scheduled departure times and trip IDs from the response
    scheduled_departure_times = [
        {
            'departure_time': datetime.fromisoformat(schedule['attributes']['departure_time']),
            'trip_id': schedule['relationships']['trip']['data']['id']
        }
        for schedule in data['data']
    ]

    # Filter out departure times that have already passed
    scheduled_departure_times = [
        departure_info for departure_info in scheduled_departure_times
        if departure_info['departure_time'] >= current_time
    ]

    # Sort the departure times in ascending order
    scheduled_departure_times.sort(key=lambda x: x['departure_time'])

    # Find the remaining departure times closest to the current time
    for departure_info in scheduled_departure_times:
        departure_time = departure_info['departure_time']
        trip_ids.append(departure_info['trip_id'])
        departure_times.append(departure_time)
        if len(trip_ids) >= 3:
            break

    return trip_ids, departure_times


def get_predicted_trips(stop_id, url_params):
    # Make API call to predictions endpoint for specified trip ID
    url = f"{PREDICTIONS_ENDPOINT}?filter[stop]={stop_id}{url_params}&api_key={API_KEY}"
    data = make_api_call(url)

    trip_ids = []
    departure_times = []

    if len(data['data']) > 0:
        # Get the predicted departure times and trip IDs from the response
        predicted_departure_times = [
            {
                'departure_time': datetime.fromisoformat(prediction['attributes']['departure_time']),
                'trip_id': prediction['relationships']['trip']['data']['id']
            }
            for prediction in data['data']
        ]

        # Find the remaining departure times
        for departure_info in predicted_departure_times:
            departure_time = departure_info['departure_time']
            trip_ids.append(departure_info['trip_id'])
            departure_times.append(departure_time)
        
        return trip_ids, departure_times

    return None, None


def get_arrival_time(trip_id, stop_id):
    # Make API call to predictions endpoint for arrival stop and specified trip ID
    url = f"{PREDICTIONS_ENDPOINT}?filter[trip]={trip_id}&filter[stop]={stop_id}&api_key={API_KEY}"
    data = make_api_call(url)

    if len(data['data']) > 0:
        arrival_time = data['data'][0]['attributes'].get('arrival_time')
        return arrival_time

    # If no arrival time found, make additional API call to schedules endpoint
    url = f"{SCHEDULES_ENDPOINT}?filter[trip]={trip_id}&filter[stop]={stop_id}&api_key={API_KEY}"
    data = make_api_call(url)

    if len(data['data']) > 0:
        arrival_time = data['data'][0]['attributes'].get('arrival_time')
        return arrival_time

    return None


def insert_into_database(trip_id, departure_stop, scheduled_depart_time, predicted_depart_time, arrival_stop, arrival_time):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Define INSERT query
    query = "INSERT INTO schedules_and_pred (timestamp, trip_id, depart_station, scheduled_depart_time, predicted_depart_time, arrive_station, arrive_time, transit_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    transit_time = None

    if arrival_time:
        arrival_time = datetime.fromisoformat(arrival_time)
        if predicted_depart_time:
            predicted_depart_time = datetime.fromisoformat(str(predicted_depart_time))
            transit_time = arrival_time - predicted_depart_time
        elif scheduled_depart_time:
            scheduled_depart_time = datetime.fromisoformat(str(scheduled_depart_time))
            transit_time = arrival_time - scheduled_depart_time

    values = (datetime.now(), trip_id, departure_stop, scheduled_depart_time, predicted_depart_time, arrival_stop, arrival_time, transit_time)

    cursor.execute(query, values)
    conn.commit()

    cursor.close()
    conn.close()


def grab_arrival_times():
    # Start the process every 60 seconds
    target_interval = 60

    while True:
        start_time = time.time()

        # for loop for CR stops
        for departure_stop_id, departure_stop_name in CR_departure_stops.items():
            scheduled_trip_ids, scheduled_departure_times = get_scheduled_trips(departure_stop_id, CR_url_params)
            predicted_trip_ids, predicted_departure_times = get_predicted_trips(departure_stop_id, CR_url_params)

            # Check if predicted trip IDs and departure times are None
            if predicted_trip_ids is None or predicted_departure_times is None:
                # Handle the case where no predicted trips are available
                for i in range(len(scheduled_trip_ids)):
                    trip_id = scheduled_trip_ids[i]
                    scheduled_depart_time = scheduled_departure_times[i]
                    predicted_depart_time = None
                    arrival_time = get_arrival_time(trip_id, CR_arrival_stop_id)
                    insert_into_database(trip_id, departure_stop_name, scheduled_depart_time, predicted_depart_time, CR_arrival_stop_name, arrival_time)
            else:
                # Compare up to three trip IDs
                for i in range(3):
                    if i < len(scheduled_trip_ids) and i < len(predicted_trip_ids):
                        # If trip IDs match, assign predicted and scheduled times
                        if scheduled_trip_ids[i] == predicted_trip_ids[i]:
                            trip_id = scheduled_trip_ids[i]
                            scheduled_depart_time = scheduled_departure_times[i]
                            predicted_depart_time = predicted_departure_times[i]
                        else:
                            # If trip IDs don't match, insert each with the other field set as null
                            trip_id = scheduled_trip_ids[i]
                            scheduled_depart_time = scheduled_departure_times[i]
                            predicted_depart_time = None
                    else:
                        # If one of the lists is exhausted, insert remaining trip IDs with null fields
                        if i < len(scheduled_trip_ids):
                            trip_id = scheduled_trip_ids[i]
                            scheduled_depart_time = scheduled_departure_times[i]
                            predicted_depart_time = None
                        elif i < len(predicted_trip_ids):
                            trip_id = predicted_trip_ids[i]
                            scheduled_depart_time = None
                            predicted_depart_time = predicted_departure_times[i]
                        else:
                            break

                    arrival_time = get_arrival_time(trip_id, CR_arrival_stop_id)
                    insert_into_database(trip_id, departure_stop_name, scheduled_depart_time, predicted_depart_time, CR_arrival_stop_name, arrival_time)

        # for loop for BL stops
        for departure_stop_id, departure_stop_name in BL_departure_stops.items():
            scheduled_trip_ids, scheduled_departure_times = get_scheduled_trips(departure_stop_id, BL_url_params)
            predicted_trip_ids, predicted_departure_times = get_predicted_trips(departure_stop_id, BL_url_params)

            # Check if predicted trip IDs and departure times are None
            if predicted_trip_ids is None or predicted_departure_times is None:
                # Handle the case where no predicted trips are available
                for i in range(len(scheduled_trip_ids)):
                    trip_id = scheduled_trip_ids[i]
                    scheduled_depart_time = scheduled_departure_times[i]
                    predicted_depart_time = None
                    arrival_time = get_arrival_time(trip_id, BL_arrival_stop_id)
                    insert_into_database(trip_id, departure_stop_name, scheduled_depart_time, predicted_depart_time, BL_arrival_stop_name, arrival_time)
            else:
                # Compare up to three trip IDs
                for i in range(3):
                    if i < len(scheduled_trip_ids) and i < len(predicted_trip_ids):
                        # If trip IDs match, assign predicted and scheduled times
                        if scheduled_trip_ids[i] == predicted_trip_ids[i]:
                            trip_id = scheduled_trip_ids[i]
                            scheduled_depart_time = scheduled_departure_times[i]
                            predicted_depart_time = predicted_departure_times[i]
                        else:
                            # If trip IDs don't match, insert each with the other field set as null
                            trip_id = scheduled_trip_ids[i]
                            scheduled_depart_time = scheduled_departure_times[i]
                            predicted_depart_time = None
                    else:
                        # If one of the lists is exhausted, insert remaining trip IDs with null fields
                        if i < len(scheduled_trip_ids):
                            trip_id = scheduled_trip_ids[i]
                            scheduled_depart_time = scheduled_departure_times[i]
                            predicted_depart_time = None
                        elif i < len(predicted_trip_ids):
                            trip_id = predicted_trip_ids[i]
                            scheduled_depart_time = None
                            predicted_depart_time = predicted_departure_times[i]
                        else:
                            break

                    arrival_time = get_arrival_time(trip_id, BL_arrival_stop_id)
                    insert_into_database(trip_id, departure_stop_name, scheduled_depart_time, predicted_depart_time, BL_arrival_stop_name, arrival_time)

        end_time = time.time()
        run_time = end_time - start_time

        # Calculate the remaining time until 60 seconds has passed
        remaining_time = target_interval - run_time
        
        time.sleep(remaining_time)

# Generate database schema
create_database_table()

# Start process
grab_arrival_times()