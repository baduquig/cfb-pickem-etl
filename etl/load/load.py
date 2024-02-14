"""
Pickem ETL
Author: Gabe Baduqui

Load pickem data from various web sources into desired destinations.
"""
from flask import jsonify, request
import etl.utils.get_timestamp as ts

def instantiate_logfile(league):
    timestamp = ts.get_timestamp()
    load_logfile_path = f'./pickem_logs/load_{league.upper()}_data_{timestamp}.log'
    load_logfile = open(load_logfile_path, 'a')
    return load_logfile

def load_csv(prod: bool, df: object, table_name: str, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a CSV file
      Accepts `prod`: Boolean, `df`: Pandas DataFrame, `table_name`: String, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to CSV File ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to CSV File ~~\n')
   
   if not prod:
      csv_path = f'./pickem_data/{table_name}.csv'
   
   df.to_csv(csv_path, index=False)

def load_json(app, prod: bool, df: object, table_name: str, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a JSON object
      Accepts `prod`: Boolean, `df`: Pandas DataFrame, `table_name`: String, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~\n')
   
   if prod:
      @app.route(f'/{table_name}')
      def get_schedule_data():
         # todo
         pass
   else:
      json_path = f'./pickem_data/{table_name}.json'
   df.to_json(json_path, orient='records')

def full_load(app, prod: bool, league: str, games_df: dict, teams_df: dict, locations_df: dict):
   """Function that calls all necessary functions to load all consolidated pickem data, stored in Pandas DataFrames, into the desired desinations
      Accepts `league`: String, `games_df`: Pandas DataFrame, `teams_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
      Returns: n/a"""
   load_logfile = instantiate_logfile(league)
   print(f'\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write(f'\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

   load_csv(prod, games_df, f'{league.lower()}_games', load_logfile)
   load_csv(prod, teams_df, f'{league.lower()}_teams', load_logfile)
   load_csv(prod, locations_df, f'{league.lower()}_locations', load_logfile)
   
   load_json(app, prod, games_df, f'{league.lower()}_games', load_logfile)
   load_json(app, prod, teams_df, f'{league.lower()}_teams', load_logfile)
   load_json(app, prod, locations_df, f'{league.lower()}_locations', load_logfile)

   print(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')