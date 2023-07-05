import matplotlib.pyplot as plt

def plot_hourly_inconsistencies(df):
    # Extract the hour from the Timestamp column
    df['Hour'] = df['Timestamp'].dt.hour

    # Create a histogram of inconsistencies by hour
    plt.figure(figsize=(10, 6))
    plt.hist(df['Hour'], bins=24, range=(0, 24), align='left', edgecolor='black')
    plt.xlabel('Hour of the Day')
    plt.ylabel('Number of Inconsistencies')
    plt.title('Hourly Distribution of Inconsistencies')
    plt.xticks(range(0, 24))
    plt.grid(True)
    plt.show()

def plot_minute_inconsistencies(df):
    # Extract the minute from the Timestamp column
    df['Minute'] = df['Timestamp'].dt.minute

    # Create a histogram of inconsistencies by minute
    plt.figure(figsize=(10, 6))
    plt.hist(df['Minute'], bins=60, range=(0, 60), align='left', edgecolor='black')
    plt.xlabel('Minute of the Hour')
    plt.ylabel('Number of Inconsistencies')
    plt.title('Distribution of Inconsistencies by Minute')
    plt.xticks(range(0, 61, 5))
    plt.grid(True)
    plt.show()
