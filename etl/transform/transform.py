"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare extracted pickem data for loading.
"""
import etl.utils.get_timestamp as ts
import etl.transform.common.transform_games_data as tf_games
import etl.transform.common.transform_teams_data as tf_teams

def instantiate_logfile(league: str):
    """Function that instantiates logfile for current transform job
       Accepts `league`: String
       Returns `extract_logfile`: File Object"""
    timestamp = ts.get_timestamp()
    transfrom_logfile_path = f'./pickem_logs/{league}_transform_{timestamp}.log'
    transform_logfile = open(transfrom_logfile_path, 'a')
    return transform_logfile

def transform_games(league:str, games_df: dict, transform_logfile: object):
    """Function that applies all necessary transformations to Games related data elements
       Accepts `games_df`: Pandas DataFrame, `transform_logfile`: File Object
       Returns `games_df`: Pandas DataFrame"""
    for idx in range(len(games_df)):
        print(f'~~ Cleansing and formatting Data for Game ID {games_df.loc[idx, "game_id"]}')
        transform_logfile.write(f'\n~~ Cleansing and formatting Data for Game ID {games_df.loc[idx, "game_id"]}\n')

        # Current row colum variables
        location = games_df.loc[idx, 'location']
        game_timestamp = games_df.loc[idx, 'game_timestamp']
        stadium_capacity = games_df.loc[idx, 'stadium_capacity']
        attendance = games_df.loc[idx, 'attendance']

        # Away Box Score
        if 'away_team_box_score' in games_df.columns:
            away_team_box_score = games_df.loc[idx, 'away_team_box_score']
            away_quarter1, away_quarter2, away_quarter3, away_quarter4, overtime, away_total = tf_games.transform_box_score(away_team_box_score, transform_logfile)
            games_df.loc[idx, 'away_quarter1'] = away_quarter1
            games_df.loc[idx, 'away_quarter2'] = away_quarter2
            games_df.loc[idx, 'away_quarter3'] = away_quarter3
            games_df.loc[idx, 'away_quarter4'] = away_quarter4
            games_df.loc[idx, 'away_overtime'] = overtime
            games_df.loc[idx, 'away_total'] = away_total

        # Home Box Score
        if 'home_team_box_score' in games_df.columns:
            home_team_box_score = games_df.loc[idx, 'home_team_box_score']
            home_quarter1, home_quarter2, home_quarter3, home_quarter4, overtime, home_total = tf_games.transform_box_score(home_team_box_score, transform_logfile)
            games_df.loc[idx, 'home_quarter1'] = home_quarter1
            games_df.loc[idx, 'home_quarter2'] = home_quarter2
            games_df.loc[idx, 'home_quarter3'] = home_quarter3
            games_df.loc[idx, 'home_quarter4'] = home_quarter4
            games_df.loc[idx, 'home_overtime'] = overtime
            games_df.loc[idx, 'home_total'] = home_total

        # Location
        games_df.loc[idx, 'location'] = tf_games.transform_location(location, transform_logfile)

        # Game Timestamp
        games_df.loc[idx, 'game_time'] = tf_games.transform_game_time(game_timestamp, transform_logfile)
        games_df.loc[idx, 'game_date'] = tf_games.transform_game_date(game_timestamp, transform_logfile)

        # Stadium and Attendance
        if stadium_capacity is not None:
            games_df.loc[idx, 'stadium_capacity'] = tf_games.transform_stadium_capacity(stadium_capacity, transform_logfile)
        if attendance is not None:
            games_df.loc[idx, 'attendance'] = tf_games.transform_stadium_attendance(attendance, transform_logfile)
    
    if league.upper() in ['CFB', 'NFL']:
        print(f'Dropping columns [\'away_team_box_score\', \'home_team_box_score\', \'game_timestamp\'] from games_df\n')
        transform_logfile.write(f'Dropping columns [\'away_team_box_score\', \'home_team_box_score\', \'game_timestamp\'] from games_df\n\n')
        games_df.drop(['away_team_box_score', 'home_team_box_score', 'game_timestamp'], axis=1, inplace=True)
    else:
        print(f'Dropping column [\'game_timestamp\'] from games_df\n')
        transform_logfile.write(f'Dropping column [\'game_timestamp\'] from games_df\n\n')
        games_df = games_df.drop(['game_timestamp'], axis=1)
    return games_df


def transform_teams(league: str, teams_df: dict, transform_logfile: object):
    """Function that applies all necessary transformations to Teams related data elements
       Accepts `teams_df`: Pandas DataFrame
       Returns `teams_df`: Pandas DataFrame"""
    for idx in range(len(teams_df)):
        print(f'~~ Cleansing and formatting Data for Team ID {teams_df.loc[idx, "team_id"]}')
        transform_logfile.write(f'\n~~ Cleansing and formatting Data for Team ID {teams_df.loc[idx, "team_id"]}\n')

        # Current row colum variables
        conference_name = teams_df.loc[idx, 'conference_name']
        overall_record = teams_df.loc[idx, 'overall_record']

        # Conference Name
        teams_df.loc[idx, 'conference_name'] = tf_teams.transform_conference_name(conference_name, transform_logfile)

        # Conference Record
        if 'conference_record' in teams_df.columns:
            conference_record = teams_df.loc[idx, 'conference_record']
            if conference_record is not None:
                conference_wins, conference_losses, conference_ties = tf_teams.transform_record(conference_record, transform_logfile)
                teams_df.loc[idx, 'conference_wins'] = conference_wins
                teams_df.loc[idx, 'conference_losses'] = conference_losses
                teams_df.loc[idx, 'conference_ties'] = conference_ties
            else:
                teams_df.loc[idx, 'conference_wins'] = 0
                teams_df.loc[idx, 'conference_losses'] = 0
                teams_df.loc[idx, 'conference_ties'] = 0
        
        # Overall Record
        if 'overall_record' in teams_df.columns:
            if overall_record is not None:
                overall_wins, overall_losses, overall_ties = tf_teams.transform_record(overall_record, transform_logfile)
                teams_df.loc[idx, 'overall_wins'] = overall_wins
                teams_df.loc[idx, 'overall_losses'] = overall_losses
                teams_df.loc[idx, 'overall_ties'] = overall_ties
            else:
                teams_df.loc[idx, 'overall_wins'] = 0
                teams_df.loc[idx, 'overall_losses'] = 0
                teams_df.loc[idx, 'overall_ties'] = 0
    
    if league.upper() in ['CFB', 'NFL']:
        print(f'Dropping columns [\'conference_record\', \'overall_record\'] from teams_df\n')
        transform_logfile.write(f'Dropping columns [\'conference_record\', \'overall_record\'] from teams_df\n\n')
        teams_df.drop(['conference_record', 'overall_record'], axis=1, inplace=True)
    else:
        print(f'Dropping columns [\'conference_record\', \'overall_record\', \'conference_wins\', \'conference_losses\', \'conference_ties\'] from teams_df\n')
        transform_logfile.write(f'Dropping columns [\'conference_record\', \'overall_record\', \'conference_wins\', \'conference_losses\', \'conference_ties\'] from teams_df\n\n')
        teams_df = teams_df.drop(['conference_record', 'overall_record', 'conference_wins', 'conference_losses', 'conference_ties'], axis=1)
    return teams_df


def full_transform(league: str, games_raw: dict, teams_raw: dict, locations_raw: dict):
    """Function that calls all necessary functions to apply necessary data transformations to pickem data frames
      Accepts `league`: String, `games_df`: Pandas DataFrame, `teams_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
      Returns `games_df`: Pandas DataFrame, `schools_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame"""
    transform_logfile = instantiate_logfile(league)
    print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Transform Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

    print('~~ Transforming games data...')
    transform_logfile.write('\n~~ Transforming games data...')
    games_df = transform_games(league, games_raw, transform_logfile)

    print('~~ Transforming teams data...')
    transform_logfile.write('\n~~ Transforming teams data...')
    teams_df = transform_teams(league, teams_raw, transform_logfile)

    transform_logfile.write('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Transform Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

    return games_df, teams_df, locations_raw