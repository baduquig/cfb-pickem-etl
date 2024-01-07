"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare extracted pickem data for loading.
"""
import etl.utils.get_timestamp as ts
import etl.transform.common.transform_games_data as games

def instantiate_logfile(league: str):
    timestamp = ts.get_timestamp()
    transfrom_logfile_path = f'./pickem_logs/{league}_transform_{timestamp}.log'
    transform_logfile = open(transfrom_logfile_path, 'a')
    return transform_logfile

def transform_games(games_df: dict):
    """Function that applies all necessary transformations to Games related data elements
       Accepts: `games_df`: Pandas DataFrame
       Returns: `games_df: Pandas DataFrame"""
    for idx in range(len(games_df)):
        away_team_box_score = games_df.loc[idx, 'away_team_box_score']
        print(away_team_box_score)
        away_quarter1, away_quarter2, away_quarter3, away_quarter4, overtime, away_total = games.transform_box_score(away_team_box_score)
        games_df.loc[idx, 'away_quarter1'] = away_quarter1
        games_df.loc[idx, 'away_quarter2'] = away_quarter2
        games_df.loc[idx, 'away_quarter3'] = away_quarter3
        games_df.loc[idx, 'away_quarter4'] = away_quarter4
        games_df.loc[idx, 'away_overtime'] = overtime
        games_df.loc[idx, 'away_total'] = away_total
    
    games_df.drop(['away_team_box_score'], axis=1)
    return games_df

def full_transform(league: str, games_raw: dict, teams_raw: dict, locations_raw: dict):
    """Function that calls all necessary functions to apply necessary data transformations to pickem data frames
      Accepts `league`: String, `games_df`: Pandas DataFrame, `teams_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
      Returns: `games_df`: Pandas DataFrame, `schools_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame"""
    transform_logfile = instantiate_logfile(league)
    print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Transform Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    games_df = transform_games(games_raw)

    transform_logfile.write('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Transform Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    return games_df, teams_raw, locations_raw