"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare extracted pickem data for loading.
"""
import etl.utils.get_timestamp as ts
import etl.transform.common.transform_games_data as tf_games

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
        print(f'~~ Cleansing and formatting Data for Game ID {games_df.loc[idx, "game_id"]}')
        # Current row colum variables
        away_team_box_score = games_df.loc[idx, 'away_team_box_score']
        home_team_box_score = games_df.loc[idx, 'home_team_box_score']
        location = games_df.loc[idx, 'location']
        game_timestamp = games_df.loc[idx, 'game_timestamp']
        stadium_capacity = games_df.loc[idx, 'stadium_capacity']
        attendance = games_df.loc[idx, 'attendance']

        # Away Box Score
        away_quarter1, away_quarter2, away_quarter3, away_quarter4, overtime, away_total = tf_games.transform_box_score(away_team_box_score)
        games_df.loc[idx, 'away_quarter1'] = away_quarter1
        games_df.loc[idx, 'away_quarter2'] = away_quarter2
        games_df.loc[idx, 'away_quarter3'] = away_quarter3
        games_df.loc[idx, 'away_quarter4'] = away_quarter4
        games_df.loc[idx, 'away_overtime'] = overtime
        games_df.loc[idx, 'away_total'] = away_total

        # Home Box Score
        home_quarter1, home_quarter2, home_quarter3, home_quarter4, overtime, home_total = tf_games.transform_box_score(home_team_box_score)
        games_df.loc[idx, 'home_quarter1'] = home_quarter1
        games_df.loc[idx, 'home_quarter2'] = home_quarter2
        games_df.loc[idx, 'home_quarter3'] = home_quarter3
        games_df.loc[idx, 'home_quarter4'] = home_quarter4
        games_df.loc[idx, 'home_overtime'] = overtime
        games_df.loc[idx, 'home_total'] = away_total

        # Location
        games_df.loc[idx, 'location'] = tf_games.transform_location(location)

        # Game Timestamp
        games_df.loc[idx, 'game_time'] = tf_games.transform_game_time(game_timestamp)
        games_df.loc[idx, 'game_date'] = tf_games.transform_game_date(game_timestamp)
        print(str(games_df.loc[idx, 'game_date']))
        print(str(games_df.loc[idx, 'game_time']))

        # Stadium and Attendance
        if stadium_capacity is not None:
            games_df.loc[idx, 'stadium_capacity'] = tf_games.transform_stadium_capacity(stadium_capacity)
        if attendance is not None:
            games_df.loc[idx, 'attendance'] = tf_games.transform_stadium_attendance(attendance)
        
    
    games_df.drop(['away_team_box_score'], axis=1)
    games_df.drop(['home_team_box_score'], axis=1)
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