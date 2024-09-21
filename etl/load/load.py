"""
Pickem ETL
Author: Gabe Baduqui

Load pickem data from various web sources into desired destinations.
"""
from math import nan
import etl.load.db as db
import etl.utils.get_timestamp as ts

def instantiate_logfile(league):
    timestamp = ts.get_timestamp()
    load_logfile_path = f'./pickem_logs/{league.upper()}_load_{timestamp}.log'
    load_logfile = open(load_logfile_path, 'a')
    return load_logfile

def load_csv(df: dict, table_name: str, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a CSV file
      Accepts `df`: Pandas DataFrame, `table_name`: String, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to CSV File ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to CSV File ~~\n')
   
   csv_path = f'./pickem_data/{table_name}.csv'
   df.to_csv(csv_path, index=False)

def load_json(df: dict, table_name: str, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into a JSON object
      Accepts `df`: Pandas DataFrame, `table_name`: String, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~')
   load_logfile.write(f'~~~~ Writing {table_name} DataFrame to JSON Object ~~\n')
   
   json_path = f'./pickem_data/{table_name}.json'
   df.to_json(json_path, orient='records')
   
def load_db(league: str, df: dict, table_name: str, load_logfile: object):
   """Function that loads data from a given Pandas DataFrame into the MySQL Database
      Accepts `league`: String, `df`: Pandas DataFrame, `table_name`: String, `load_logfile`: File Object
      Returns: n/a"""
   print(f'~~~~ Loading {league} {table_name} data into MySQL Database ~~')
   load_logfile.write(f'~~~~ Loading {league} {table_name} data into MySQL Database ~~\n')
   
   for i in range(len(df)):
      record = df.iloc[i]
      if record.league == 'CFB':
         try:
            if table_name.lower() == 'games':
               if record.game_id is not nan:
                  if db.record_exists_in_table(table_name, record, load_logfile):
                     db.update_record(table_name, record, load_logfile)
                  else:
                     db.insert_record(table_name, record, load_logfile)
            elif table_name.lower() == 'teams':
               if record.team_id is not nan:
                  if db.record_exists_in_table(table_name, record, load_logfile):
                     db.update_record(table_name, record, load_logfile)
                  else:
                     db.insert_record(table_name, record, load_logfile)
            else:
               if db.record_exists_in_table(table_name, record, load_logfile):
                  db.update_record(table_name, record, load_logfile)
               else:
                  db.insert_record(table_name, record, load_logfile)
         except Exception as e:
            print(f'~~~~ Error occurred loading record {record} into database:\n{e}')
            load_logfile(f'~~~~ Error occurred loading record {record} into database:\n{e}\n')


def full_load(prod: bool, league: str, games_df: dict, teams_df: dict, locations_df: dict):
   """Function that calls all necessary functions to load all consolidated pickem data, stored in Pandas DataFrames, into the desired desinations
      Accepts `league`: String, `games_df`: Pandas DataFrame, `teams_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
      Returns: n/a"""
   load_logfile = instantiate_logfile(league)
   print(f'\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write(f'\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

   load_csv(games_df, f'{league.lower()}_games', load_logfile)
   load_csv(teams_df, f'{league.lower()}_teams', load_logfile)
   load_csv(locations_df, f'{league.lower()}_locations', load_logfile)
   
   load_json(games_df, f'{league.lower()}_games', load_logfile)
   load_json(teams_df, f'{league.lower()}_teams', load_logfile)
   load_json(locations_df, f'{league.lower()}_locations', load_logfile)

   load_db(league, games_df, 'games', load_logfile)
   load_db(league, teams_df, 'teams', load_logfile)
   load_db(league, locations_df, 'locations', load_logfile)
   print(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
   load_logfile.write(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished {league.upper()} Load Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')