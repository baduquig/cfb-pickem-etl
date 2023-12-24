"""
Pickem ETL
Author: Gabe Baduqui

Load pickem data from various web sources into desired destinations.
"""
import etl.utils.get_timestamp as ts

def instantiate_logfile(league: str):
    timestamp = ts.get_timestamp()
    load_logfile_path = f'./logs/{league}_load_{timestamp}.log'
    load_logfile = open(load_logfile_path, 'a')
    return load_logfile

def load_csv(df, table_name, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a CSV file
      Accepts `df`: Pandas DataFrame, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to CSV File ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to CSV File ~~\n')
    
   csv_path = f'./data/{table_name}.csv'
   df.to_csv(csv_path, index=False)

def load_json(df, table_name, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a JSON object
      Accepts `df`: Pandas DataFrame, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~\n')
    
   json_path = f'./data/{table_name}.json'
   df.to_json(json_path, orient='records')

def full_load(league: str, games_df: dict, schools_df: dict, locations_df: dict):
   """Function that calls all necessary functions to load all CFB pickem data, stored in Pandas DataFrames, into the desired desinations
      Accepts `league`: String, `games_df`: Pandas DataFrame, `schools_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
      Returns: n/a"""
   load_logfile = instantiate_logfile(league)
   print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

   load_csv(games_df, f'{league.lower()}_games', load_logfile)
   load_csv(schools_df, f'{league.lower()}_schools', load_logfile)
   load_csv(locations_df, f'{league.lower()}_locations', load_logfile)
   
   load_json(games_df, f'{league.lower()}_games', load_logfile)
   load_json(schools_df, f'{league.lower()}_schools', load_logfile)
   load_json(locations_df, f'{league.lower()}_locations', load_logfile)

   print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')