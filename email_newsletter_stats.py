# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 10:41:45 2023

Script to fetch and process newsletter statistics from an API and merge them with existing data.
Runs daily, appending the results to a cumulative DataFrame.
"""

import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
# comment for change
# Function to fetch and process newsletter data
def fetch_newsletter_data(api_key):
    # Prepare the headers with the bearer token
    headers = {'X-Auth-Token': f'api-key {api_key}'}

    # Fetch the list of all newsletters
    response1 = requests.get('https://api.getresponse.com/v3/newsletters', headers=headers)
    all_newsletters = response1.json()
    df_all = pd.DataFrame(all_newsletters)
    df_all = df_all[['newsletterId', 'name', 'subject', 'sendOn']]
    newsletter_ids = list(df_all['newsletterId'])

    # Base URL for the API endpoint
    base_url = 'https://api.getresponse.com/v3/newsletters/'

    # List to store DataFrame for each newsletter
    dfs = []

    # Loop over each newsletter ID and fetch statistics
    for newsletter_id in newsletter_ids:
        # Construct the full URL for each specific newsletter
        url = f"{base_url}{newsletter_id}/statistics"

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Process the response
        if response.status_code == 200:
            data_df = pd.DataFrame(response.json())
            data_df['newsletterId'] = newsletter_id  # Add newsletter ID as a new column
            dfs.append(data_df)

    # Concatenate all DataFrames in the list
    final_df = pd.concat(dfs, ignore_index=True)

    # Perform a full outer join with the original data
    merged_df = pd.merge(df_all, final_df, on='newsletterId', how='outer')

    # Drop the 'timeInterval' column
    merged_df = merged_df.drop('timeInterval', axis=1)

    # Add the current date to the DataFrame
    merged_df['dateFetched'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return merged_df

# Function to insert data into the MySQL table
def insert_into_table(data, db_config):
    try:
        # Establish a connection to the MySQL database
        connection = mysql.connector.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'])

        if connection.is_connected():
            cursor = connection.cursor()

              # SQL insert statement
            insert_stmt = (
                "INSERT INTO NewsletterStats (newsletterId, name, subject, sendOn, sent, totalOpened, uniqueOpened, totalClicked, uniqueClicked, goals, uniqueGoals, forwarded, unsubscribed, bounced, complaints, dateFetched)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )

            print(insert_stmt)
            # Executing the SQL command
            cursor.executemany(insert_stmt, data)

            # Commit your changes in the database
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into table")

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        # Closing the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Database configuration
db_config = {
    'host': 'localhost',
    'database': 'newsletter',
    'user': 'root',
    'password': 'SerVer619$'
}

# API key for authentication
api_key = 'm81vap1w0hwq95u3gkqqaxdkq3fachw5'

# Initialize an empty DataFrame to store cumulative data
cumulative_df = pd.DataFrame()

# Fetch data and prepare for insertion
new_data = fetch_newsletter_data(api_key)
cumulative_df = pd.concat([cumulative_df, new_data], ignore_index=True)
cumulative_df['sendOn'] = pd.to_datetime(cumulative_df['sendOn']).dt.strftime('%Y-%m-%d %H:%M:%S')
# cumulative_df['id'] = range(1, 1 + len(cumulative_df))
# Preparing data tuple from the DataFrame for insertion
# Ensure the order and format of these columns match the structure of your SQL table
data_for_insertion = [tuple(x) for x in cumulative_df[['newsletterId', 'name', 'subject', 'sendOn', 'sent', 'totalOpened',
       'uniqueOpened', 'totalClicked', 'uniqueClicked', 'goals', 'uniqueGoals',
       'forwarded', 'unsubscribed', 'bounced', 'complaints', 'dateFetched']].values]
# Insert data into the table
insert_into_table(data_for_insertion, db_config)

# # Define the path to save the CSV file
# csv_file_path = 'cumulative_newsletter_data.csv'

# # Main loop to run the process daily
# while True:
#     new_data = fetch_newsletter_data(api_key)
#     cumulative_df = pd.concat([cumulative_df, new_data], ignore_index=True)
    
#     # Save to CSV file
#     cumulative_df.to_csv(csv_file_path, index=False)
    
#     # Wait for 1 day (86400 seconds) before the next run
#     time.sleep(86400)