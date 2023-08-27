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

#  all the function goes here 
def plot_score_distribution(data_frame):
    # Count the occurrences of each score
    score_counts = data_frame['score'].value_counts()

    # Calculate the percentage of each score
    total_scores = score_counts.sum()
    score_percentages = (score_counts / total_scores) * 100

    # Create a pie chart
    plt.figure(figsize=(8, 6))
    plt.pie(score_percentages, labels=score_percentages.index, autopct='%1.1f%%', startangle=140)
    plt.title('Score Distribution')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.show()


def plot_cabin_satisfaction(data_frame):
    # Group data by cabin_code_desc and satisfaction_type
    grouped = data_frame.groupby(['cabin_code_desc', 'satisfaction_type']).size().unstack(fill_value=0)
    
    # Calculate total counts for each cabin
    grouped['total'] = grouped['Dissatisfied'] + grouped['Satisfied']
    
    # Calculate percentages
    grouped['dissatisfaction_percent'] = (grouped['Dissatisfied'] / grouped['total']) * 100
    grouped['satisfaction_percent'] = (grouped['Satisfied'] / grouped['total']) * 100
    
    # Iterate through each cabin and plot a pie chart
    for cabin in grouped.index:
        plt.figure(figsize=(6, 6))
        plt.pie([grouped.loc[cabin, 'dissatisfaction_percent'], grouped.loc[cabin, 'satisfaction_percent']],
                labels=['Dissatisfied', 'Satisfied'],
                autopct='%1.1f%%',
                startangle=140)
        plt.title(f"Satisfaction Distribution for Cabin {cabin}")
        plt.axis('equal')
        plt.show()

def plot_satisfied_cabin_distribution(data_frame):
    # Filter data for satisfied scores
    satisfied_data = data_frame[data_frame['satisfaction_type'] == 'Satisfied']
    
    # Group data by cabin_code_desc
    grouped = satisfied_data.groupby('cabin_code_desc').size()
    
    # Calculate percentages
    total_satisfied = grouped.sum()
    percentages = (grouped / total_satisfied) * 100
    
    # Create pie chart for each cabin
    for cabin, percentage in percentages.items():
        plt.figure(figsize=(6, 6))
        plt.pie([percentage, 100 - percentage],
                labels=[f"Satisfied ({percentage:.1f}%)", f"Not Satisfied ({100 - percentage:.1f}%)"],
                autopct='%1.1f%%',
                startangle=140)
        plt.title(f"Satisfaction Distribution for Cabin {cabin}")
        plt.axis('equal')
        plt.show()

def plot_satisfaction_by_media(data_frame):
    # Group data by media_provider and satisfaction_type
    grouped = data_frame.groupby(['media_provider', 'satisfaction_type']).size().unstack(fill_value=0)
    
    # Calculate total counts for each media partner
    grouped['Total'] = grouped['Dissatisfied'] + grouped['Satisfied']
    
    # Calculate percentages
    grouped['Satisfied (%)'] = (grouped['Satisfied'] / grouped['Total']) * 100
    grouped['Dissatisfied (%)'] = (grouped['Dissatisfied'] / grouped['Total']) * 100
    
    # Iterate through each media partner and plot a pie chart
    for media in grouped.index:
        plt.figure(figsize=(6, 6))
        plt.pie([grouped.loc[media, 'Satisfied (%)'], grouped.loc[media, 'Dissatisfied (%)']],
                labels=['Satisfied', 'Dissatisfied'],
                autopct='%1.1f%%',
                startangle=140)
        plt.title(f"Satisfaction Distribution for Media Partner {media}")
        plt.axis('equal')
        plt.show()

def plot_satisfied_media_distribution(data_frame):
    # Filter data for satisfied scores
    satisfied_data = data_frame[data_frame['satisfaction_type'] == 'Satisfied']
    
    # Group data by media_provider
    grouped = satisfied_data.groupby('media_provider').size()
    
    # Create a pie chart
    plt.figure(figsize=(8, 6))
    plt.pie(grouped, labels=grouped.index, autopct='%1.1f%%', startangle=140)
    plt.title('Satisfied Customer Distribution by Media Partner')
    plt.axis('equal')
    plt.show()


def plot_satisfaction_by_loyalty(data_frame):
    # Group data by loyalty_program_level and satisfaction_type
    grouped = data_frame.groupby(['loyalty_program_level', 'satisfaction_type']).size().unstack(fill_value=0)
    
    # Calculate total counts for each loyalty program
    grouped['Total'] = grouped['Dissatisfied'] + grouped['Satisfied']
    
    # Calculate percentages
    grouped['Satisfied (%)'] = (grouped['Satisfied'] / grouped['Total']) * 100
    grouped['Dissatisfied (%)'] = (grouped['Dissatisfied'] / grouped['Total']) * 100
    
    # Iterate through each loyalty program and plot a pie chart
    for loyalty in grouped.index:
        plt.figure(figsize=(6, 6))
        plt.pie([grouped.loc[loyalty, 'Satisfied (%)'], grouped.loc[loyalty, 'Dissatisfied (%)']],
                labels=['Satisfied', 'Dissatisfied'],
                autopct='%1.1f%%',
                startangle=140)
        plt.title(f"Satisfaction Distribution for Loyalty Program {loyalty}")
        plt.axis('equal')
        plt.show()
# Example DataFrame (replace this with your actual DataFrame)

def plot_satisfaction_by_generation(data_frame):
    # Group data by generation and satisfaction_type
    grouped = data_frame.groupby(['generation', 'satisfaction_type']).size().unstack(fill_value=0)
    
    # Calculate total counts for each generation
    grouped['Total'] = grouped['Dissatisfied'] + grouped['Satisfied']
    
    # Calculate percentages
    grouped['Satisfied (%)'] = (grouped['Satisfied'] / grouped['Total']) * 100
    grouped['Dissatisfied (%)'] = (grouped['Dissatisfied'] / grouped['Total']) * 100
    
    # Iterate through each generation and plot a pie chart
    for generation in grouped.index:
        plt.figure(figsize=(6, 6))
        plt.pie([grouped.loc[generation, 'Satisfied (%)'], grouped.loc[generation, 'Dissatisfied (%)']],
                labels=['Satisfied', 'Dissatisfied'],
                autopct='%1.1f%%',
                startangle=140)
        plt.title(f"Satisfaction Distribution for Generation {generation}")
        plt.axis('equal')
        plt.show()
import pandas as pd

def plot_satisfied_media_by_generation(data_frame):
    # Filter data for satisfied scores
    satisfied_data = data_frame[data_frame['satisfaction_type'] == 'Satisfied']
    
    # Group data by generation and media_provider
    grouped = satisfied_data.groupby(['generation', 'media_provider']).size().unstack(fill_value=0)
    
    # Calculate percentages and plot
    for generation in grouped.index:
        percentages = (grouped.loc[generation] / grouped.loc[generation].sum()) * 100
        plt.figure(figsize=(6, 6))
        plt.pie(percentages, labels=grouped.columns, autopct='%1.1f%%', startangle=140)
        plt.title(f"Media Usage Distribution for Satisfied Generation {generation}")
        plt.axis('equal')
        plt.show()

def plot_satisfied_cabin_by_generation(data_frame):
    # Filter data for satisfied scores
    satisfied_data = data_frame[data_frame['satisfaction_type'] == 'Satisfied']
    
    # Group data by generation and cabin_code_desc
    grouped = satisfied_data.groupby(['generation', 'cabin_code_desc']).size().unstack(fill_value=0)
    
    # Calculate percentages and plot
    for generation in grouped.index:
        percentages = (grouped.loc[generation] / grouped.loc[generation].sum()) * 100
        plt.figure(figsize=(6, 6))
        plt.pie(percentages, labels=grouped.columns, autopct='%1.1f%%', startangle=140)
        plt.title(f"Cabin Usage Distribution for Satisfied Generation {generation}")
        plt.axis('equal')
        plt.show()



    # Filter data for highly satisfied scores
    highly_satisfied_data = data_frame[data_frame['score'] == 5]
    
    # Group data by flight routes and calculate average satisfaction
    grouped = highly_satisfied_data.groupby(['origin_station_code', 'destination_station_code']).size().reset_index(name='count')
    
    # Calculate total counts for each route
    total_counts = grouped.groupby('origin_station_code')['count'].sum()
    
    # Calculate percentages
    grouped['Satisfaction (%)'] = (grouped['count'] / total_counts[grouped['origin_station_code']]) * 100
    
    # Sort routes by satisfaction percentage
    sorted_routes = grouped.sort_values('Satisfaction (%)', ascending=False).head(top_n)
    
    # Plot pie charts for each route
    for index, row in sorted_routes.iterrows():
        route_name = f"{row['origin_station_code']} to {row['destination_station_code']}"
        plt.figure(figsize=(6, 6))
        plt.pie([row['Satisfaction (%)'], 100 - row['Satisfaction (%)']],
                labels=[f"Satisfied ({row['Satisfaction (%)']:.1f}%)", f"Not Satisfied ({100 - row['Satisfaction (%)']:.1f}%)"],
                autopct='%1.1f%%',
                startangle=140)
        plt.title(f"Satisfaction Distribution for Route {route_name}")
        plt.axis('equal')
        plt.show()

    return sorted_routes

def plot_satisfaction_pie_chart(data_frame):
    # Group data by satisfaction_type
    grouped = data_frame.groupby('satisfaction_type').size()
    
    # Calculate total counts
    total_counts = grouped.sum()
    
    # Calculate percentages
    percentages = (grouped / total_counts) * 100
    
    # Plot pie chart
    plt.figure(figsize=(6, 6))
    plt.pie(percentages, labels=grouped.index, autopct='%1.1f%%', startangle=140)
    plt.title('Satisfaction Distribution')
    plt.axis('equal')
    plt.show()


def plot_satisfaction_by_delay(data_frame):
    # Group data by arrival_delay_group and satisfaction_type
    grouped = data_frame.groupby(['arrival_delay_group', 'satisfaction_type']).size().unstack(fill_value=0)
    
    # Calculate total counts for each delay group
    total_counts = grouped.sum()
    
    # Calculate percentages
    grouped['Satisfied (%)'] = (grouped['Satisfied'] / total_counts['Satisfied']) * 100
    grouped['Dissatisfied (%)'] = (grouped['Dissatisfied'] / total_counts['Dissatisfied']) * 100
    
    # Plot pie charts for each delay group
    for group, row in grouped.iterrows():
        plt.figure(figsize=(6, 6))
        plt.pie([row['Satisfied (%)'], row['Dissatisfied (%)']],
                labels=[f"Satisfied ({row['Satisfied (%)']:.1f}%)", f"Dissatisfied ({row['Dissatisfied (%)']:.1f}%)"],
                autopct='%1.1f%%',
                startangle=140)
        plt.title(f"Satisfaction Distribution for Delay Group: {group}")
        plt.axis('equal')
        plt.show()

# after arrival on time the mdeia source used by the stisfied an the disatisfied customer
def plot_cabin_satisfaction(data_frame):
    # Filter data for "Arrival On Time"
    # print(  "this is the data frame",data_frame
    arrival_on_time_data = data_frame[data_frame['arrival_delay_group'] == 'Early & Ontime']
    # Group data by cabin_code_desc and satisfaction_type
    grouped = arrival_on_time_data.groupby(['cabin_code_desc', 'satisfaction_type']).size().unstack(fill_value=0)
    # Calculate total counts for each cabin code description
    total_counts = grouped.sum(axis=1)

    # Calculate percentages
    for satisfaction_type in ['Satisfied', 'Dissatisfied']:
        if satisfaction_type in grouped.columns:
            grouped[f'{satisfaction_type} (%)'] = (grouped[satisfaction_type] / total_counts) * 100
    
    # Plot pie charts for cabin satisfaction by satisfaction type
    for satisfaction_type in ['Satisfied (%)', 'Dissatisfied (%)']:
        if satisfaction_type in grouped.columns:
            plt.figure(figsize=(6, 6))
            plt.pie(grouped[satisfaction_type], labels=grouped.index, autopct='%1.1f%%', startangle=140)
            plt.title(f'Cabin Satisfaction Distribution for {satisfaction_type} Customers (Arrival On Time)')
            plt.axis('equal')
            plt.show()
# after this we check the 
def plot_media_usage_by_satisfaction_and_arrival(data_frame):
    # Filter data for "Arrival On Time"
    arrival_on_time_data = data_frame[data_frame['arrival_delay_group'] == 'Early & Ontime']
    
    # Group data by media_provider and satisfaction_type
    grouped = arrival_on_time_data.groupby(['media_provider', 'satisfaction_type']).size().unstack(fill_value=0)
    
    # Calculate total counts for each media provider
    total_counts = grouped.sum(axis=1)
    
    # Calculate percentages
    for satisfaction_type in ['Satisfied', 'Dissatisfied']:
        if satisfaction_type in grouped.columns:
            grouped[f'{satisfaction_type} (%)'] = (grouped[satisfaction_type] / total_counts) * 100
    
    # Plot pie charts for media provider usage by satisfaction type
    for satisfaction_type in ['Satisfied (%)', 'Dissatisfied (%)']:
        if satisfaction_type in grouped.columns:
            plt.figure(figsize=(6, 6))
            plt.pie(grouped[satisfaction_type], labels=grouped.index, autopct='%1.1f%%', startangle=140)
            plt.title(f'Media Provider Usage Distribution for {satisfaction_type} Customers (Arrival On Time)')
            plt.axis('equal')
            plt.show()


try:
    # Establish a connection to the database
    connection = mysql.connector.connect(**db_config)
    print("Connection created successfully")

    # Create a cursor
    cursor = connection.cursor()

    flight_number = '23'
    query = f"""
        SELECT *
        FROM InflightSatisfactionScore
        WHERE  driver_sub_group2 ='food and beverage satisfaction'
        ;
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Process and print the results
    # for row in rows:
    #    print(row)
    columns = [
    'flight_number', 'origin_station_code', 'destination_station_code', 
    'record_locator', 'scheduled_departure_date', 'question_text', 
    'score', 'satisfaction_type', 'driver_sub_group1', 'driver_sub_group2', 
    'arrival_delay_minutes', 'arrival_delay_group', 'cabin_code_desc', 
    'cabin_name', 'entity', 'number_of_legs', 'seat_factor_band', 
    'loyalty_program_level', 'generation', 'fleet_type_description', 
    'fleet_usage', 'equipment_type_code', 'ua_uax', 'actual_flown_miles', 
    'haul_type', 'departure_gate', 'arrival_gate', 'international_domestic_indicator', 
    'response_group', 'media_provider', 'hub_spoke'
    ]
    # Create DataFrame
    df = pd.DataFrame(rows, columns=columns)
    # plot_satisfaction_pie_chart(df)
    # plot_score_distribution(df)
    # plot_cabin_satisfaction(df)
    # plot_satisfied_cabin_distribution(df)
    # plot_satisfied_media_distribution(df)
    # plot_satisfaction_by_media(df)
    # plot_satisfaction_by_loyalty(df)
    # plot_satisfaction_by_generation(df)
    # plot_satisfied_media_by_generation(df)
    # plot_satisfied_cabin_by_generation(df)
    
    #  satisfaction on the basis of  delay
    # plot_satisfaction_by_delay(df)
    # plot_cabin_satisfaction(df)
    # plot_media_usage_by_satisfaction_and_arrival(df)
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
        