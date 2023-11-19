"""
Pickem ETL
Author: Gabe Baduqui

Load college football schedule data from various web sources into desired destinations.
"""
import pandas as pd
import etl.common.get_timestamp as ts

timestamp = ts.get_timestamp()
cfb_load_logfile_path = f'./logs/cfb_load_{timestamp}.log'
cfb_load_logfile = open(cfb_load_logfile_path, 'a')

def load_csv(df, table_name):
    """Function that loads data from a given Pandas DataFrame into a CSV file
       Accepts `df`: Pandas DataFrame
       Returns: n/a"""
    print('\n~~~~ Writing DataFrame to CSV File ~~')
    cfb_load_logfile.write('\n~~~~ Writing DataFrame to CSV File ~~\n')
    
    csv_path = f'./data/{table_name}.csv'
    df.to_csv(csv_path, index=False)

def load_json(df, table_name):
    """Function that loads data from a given Pandas DataFrame into a JSON object
       Accepts `df`: Pandas DataFrame
       Returns: n/a"""
    print('\n~~~~ Writing DataFrame to JSON Object ~~')
    cfb_load_logfile.write('\n~~~~ Writing DataFrame to JSON Object ~~\n')
    
    json_path = f'./data/{table_name}.json'
    df.to_json(json_path, orient='records')

def full_load(games_df, schools_df, locations_df):
    """Function that calls all necessary functions to load all CFB pickem data, stored in Pandas DataFrames, into the desired desinations
       Accepts `games_df`: Pandas DataFrame, `schools_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
       Returns: n/a"""
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')
    cfb_load_logfile.write('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n')

