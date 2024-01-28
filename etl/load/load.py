"""
Pickem ETL
Author: Gabe Baduqui

Load pickem data from various web sources into desired destinations.
"""
import etl.utils.get_timestamp as ts

def instantiate_logfile():
    timestamp = ts.get_timestamp()
    load_logfile_path = f'./pickem_logs/load_all_{timestamp}.log'
    load_logfile = open(load_logfile_path, 'a')
    return load_logfile

def load_csv(df, table_name, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a CSV file
      Accepts `df`: Pandas DataFrame, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to CSV File ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to CSV File ~~\n')
    
   csv_path = f'./pickem_data/{table_name}.csv'
   df.to_csv(csv_path, index=False)

def load_json(df, table_name, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a JSON object
      Accepts `df`: Pandas DataFrame, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~\n')
    
   json_path = f'./pickem_data/{table_name}.json'
   df.to_json(json_path, orient='records')

def full_load(games_df: dict, teams_df: dict, locations_df: dict):
   """Function that calls all necessary functions to load all consolidated pickem data, stored in Pandas DataFrames, into the desired desinations
      Accepts `league`: String, `games_df`: Pandas DataFrame, `teams_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
      Returns: n/a"""
   load_logfile = instantiate_logfile()
   print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

   load_csv(games_df, 'games', load_logfile)
   load_csv(teams_df, 'teams', load_logfile)
   load_csv(locations_df, 'locations', load_logfile)
   
   load_json(games_df, 'games', load_logfile)
   load_json(teams_df, 'teams', load_logfile)
   load_json(locations_df, 'locations', load_logfile)

   print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')