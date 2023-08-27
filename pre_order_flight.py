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


def plot_top_routes(df, top_n=50):
    # Group by departure and arrival station codes
    grouped = df.groupby(['departure_station_code', 'arrival_station_code']).agg({
        'meal_preorder_quantity': 'sum'
    }).reset_index()

    # Get top N routes based on meal preorder quantity
    top_routes = grouped.nlargest(top_n, 'meal_preorder_quantity')

    # Create a bar graph for the top routes
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Increase the gap between bars by adjusting x-values
    x = np.arange(0, 4 * len(top_routes), step=4)
    
    width = 0.8  # Width of the bars
    bars = ax.bar(x, top_routes['meal_preorder_quantity'], width, align='center', label='Preorder Quantity')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{src} to {dest}' for src, dest in zip(top_routes['departure_station_code'], top_routes['arrival_station_code'])], rotation=45, ha="right")
    ax.set_ylabel('Total Meal Preorder Quantity')
    ax.set_title(f'Top {top_n} Routes by Meal Preorder Quantity')
    ax.legend()

    # Adding preorder quantity labels on the bars
    for bar, label in zip(bars, top_routes['meal_preorder_quantity']):
        height = bar.get_height()
        ax.annotate(f'{label}', xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords='offset points',
                    ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.show()

def display_meal_group_pie_chart(df):
    # Group by meal_group and calculate the total preorder quantity for each group
    group_demand = df.groupby('meal_group')['meal_preorder_quantity'].sum()

    # Calculate the total pre-order quantity for all groups
    total_demand = group_demand.sum()

    # Calculate the percentage of each meal group
    group_percentages = (group_demand / total_demand) * 100

    # Create a pie chart for meal group percentages
    plt.figure(figsize=(8, 8))
    pie = plt.pie(group_percentages, labels=group_percentages.index, autopct='%1.1f%%', startangle=140, labeldistance=1)
    
    plt.title('Percentage of Preorder Quantity for Each Meal Group')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.show()
#  i have to check the  label distance 

def top_meal_groups_for_top_routes(top_n_routes=10, top_n_groups=3):
    # Group by flight route and calculate total preorder quantity for each route
    route_demand = df.groupby('flight_number')['meal_preorder_quantity'].sum()

    # Get the top N flight routes based on total preorder quantity
    top_routes = route_demand.nlargest(top_n_routes).index

    for route in top_routes:
        # Filter the dataframe for the specific flight route
        route_data = df[df['flight_number'] == route]
        
        # Get departure and arrival stations for the route
        departure_station = route_data['departure_station_code'].iloc[0]
        arrival_station = route_data['arrival_station_code'].iloc[0]

        # Group by meal_group and calculate the total preorder quantity for each group
        group_demand = route_data.groupby('meal_group')['meal_preorder_quantity'].sum()

        # Get the top N meal groups
        top_groups = group_demand.nlargest(top_n_groups)

        # Plot a bar graph for each route
        plt.figure(figsize=(10, 6))
        top_groups.plot(kind='bar', color='skyblue')
        plt.xlabel('Meal Group')
        plt.ylabel('Total Preorder Quantity')
        plt.title(f'Top {top_n_groups} Meal Groups for Flight Route {route} ({departure_station} to {arrival_station})')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()




    
try:
    # Establish a connection to the database
    connection = mysql.connector.connect(**db_config)
    print("Connection created successfully")

    # Create a cursor
    cursor = connection.cursor()

    query = f"""
        SELECT *
        FROM InflightServicePreOrder
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Process and print the results
    # for row in rows:
    #    print(row)
 
    
    # Create DataFrame
    columns = ['flight_number', 'departure_station_code', 'arrival_station_code', 'record_locator', 'carrier_code',
               'scheduled_departure_dtl', 'cabin_code', 'meal_group', 'meal_category', 'meal_short_description',
               'meal_description', 'is_entree', 'meal_preorder_quantity']
    df = pd.DataFrame(rows, columns=columns)
    
    plot_top_routes(df, top_n=50)
    display_meal_group_pie_chart(df)
    top_meal_groups_for_top_routes(top_n_routes=50, top_n_groups=4)
    
    
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
        