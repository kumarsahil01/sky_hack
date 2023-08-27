import csv
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# MySQL database connection parameters
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sahil@123',
    'database': 'sky_hack'
}

try:
    # Establish a connection to the database
    connection = mysql.connector.connect(**db_config)
    print("Connection created successfully")

    # Create a cursor
    cursor = connection.cursor()

    flight_number = '23'
    query = f"""
        SELECT *
        FROM InflightServiceInventoryJ
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Process and print the results
    # for row in rows:
    #    print(row)
    columns = ['flight_number', 'departure_station_code', 'arrival_station_code', 'scheduled_departure_dtl', 'entree_product_id', 'entree_description', 'entree_code', 'planned_entree_count', 'consumed_entree_count']
    
    # Create DataFrame
    df = pd.DataFrame(rows, columns=columns)
    
    grouped = df.groupby(['flight_number', 'entree_description']).agg({
        'planned_entree_count': 'sum',
        'consumed_entree_count': 'sum'
    }).reset_index()

    # Group by flight_number and entree_description and sum planned and consumed counts


    # Get a list of unique flight numbers
    unique_flights = grouped['flight_number'].unique()

    # Create a bar graph for each flight_number and entree_description combination
    for flight in unique_flights:
        flight_data = grouped[grouped['flight_number'] == flight]
        entrees = flight_data['entree_description']
        planned_counts = flight_data['planned_entree_count']
        consumed_counts = flight_data['consumed_entree_count']
        x = np.arange(len(entrees))
        width = 0.35
        fig, ax = plt.subplots()
        ax.bar(x - width/2, planned_counts, width, label='Planned')
        ax.bar(x + width/2, consumed_counts, width, label='Consumed')
        ax.set_xticks(x)
        ax.set_xticklabels(entrees, rotation=45, ha="right")
        ax.set_ylabel('Counts')
        ax.set_title(f'Planned and Consumed Counts of Entrees - Flight {flight}')
        ax.legend()
        plt.tight_layout()
        plt.show()

        
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Access denied error")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(f"Error: {err}")
finally:
    # Close the cursor and connection
    if 'cursor' in locals() and cursor is not None:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Connection closed")
        