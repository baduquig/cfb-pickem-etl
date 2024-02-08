"""
Pickem ETL
Author: Gabe Baduqui

Scrape pickem data from various web sources.
"""
import pandas as pd
import time
import etl.utils.get_timestamp as ts
import etl.extract.common.scrape_schedule_page as schedule
import etl.extract.common.scrape_game_page as game
import etl.extract.common.scrape_team_page as team
import etl.extract.common.get_geocode_data as geo
from datetime import date

def instantiate_logfile(league: str):
    """Function that instantiates logfile for current extract job
       Accepts `league`: String
       Returns `extract_logfile`: File Object"""
    timestamp = ts.get_timestamp()
    extract_logfile_path = f'./pickem_logs/{league}_extract_{timestamp}.log'
    extract_logfile = open(extract_logfile_path, 'a')
    return extract_logfile

def extract_games(league: str, game_ids: list, extract_logfile: object):
    """Function that instantiates a Pandas DataFrame storing Game Data scraped from ESPN Game web pages
       Accepts `league`: String, game_ids`: List, `extract_logfile`: File Object
       Returns `games_df`: Pandas DataFrame"""
    games_df = pd.DataFrame([], columns=['game_id', 'league', 'away_team_id', 'home_team_id', 'away_team_box_score', 
                                            'home_team_box_score', 'stadium', 'location', 'game_timestamp', 'tv_coverage', 'betting_line', 
                                            'betting_over_under', 'stadium_capacity', 'attendance', 'away_win_pct', 'home_win_pct'])

    for game_id in game_ids:
        game_data = game.get_game_data(league, game_id, extract_logfile)
        new_game_row = pd.DataFrame([game_data])
        games_df = pd.concat([games_df, new_game_row], ignore_index=True)
        time.sleep(.2)
    return games_df

def extract_teams(league: str, team_ids: list, extract_logfile: object):
    """Function that instantiates a Pandas DataFrame storing School Data scraped from ESPN Team web pages
       Accepts `league`: String, team_ids`: List, `extract_logfile`: File Object
       Returns `teams_df`: Pandas DataFrame"""
    teams_df = pd.DataFrame([], columns=['team_id', 'league', 'name', 'mascot', 'logo_url', 'conference_name', 'conference_record', 'overall_record'])

    for team_id in team_ids:
        team_data = team.get_team_data(league, team_id, extract_logfile)
        new_team_row = pd.DataFrame([team_data])
        teams_df = pd.concat([teams_df, new_team_row], ignore_index=True)
        time.sleep(.2)
    return teams_df

def extract_locations(league: str, stadiums: list, location_names: list, extract_logfile: object):
    """Function that instantiates a Pandas DataFrame storing Geocode Data retrieved from Geocode.maps REST API
       Accepts `stadiums`: List, `location_names`: List, `extract_logfile`: File Object
       Returns `games_raw`: Pandas DataFrame, `teams_raw`: Pandas DataFrame, `locations_raw`: Pandas DataFrame"""
    locations_df = pd.DataFrame([], columns=['league', 'location_id', 'stadium', 'city', 'state', 'latitude', 'longitude'])
    unique_locations = []
    location_id = 1

    for i in range(len(stadiums)):
        stadium = stadiums[i]
        location_name = location_names[i]
        concatenated_location = f'{stadium}, {location_name}'
        
        if ((stadium is not None) and (location_name is not None)) and (concatenated_location not in unique_locations):
            unique_locations.append(concatenated_location)
            location_data = geo.get_location_data(league, location_id, stadium, location_name, extract_logfile)
            new_location_row = pd.DataFrame([location_data])
            locations_df = pd.concat([locations_df, new_location_row], ignore_index=True)
            location_id += 1
        time.sleep(1)
    return locations_df


def full_extract(league: str, year=2024, weeks=15, schedule_window_begin=date(2024, 8, 21), schedule_window_end=date(2024, 9, 29)):
    """Function that calls all necessary functions to extract all pickem data from required sources and return in Pandas DataFrames
       Accepts `league`: String, `year`: Number, `weeks`: Number, `schedule_window_begin`: Date, `schedule_window_end`: Date
       Returns `locations_df`: Pandas DataFrame"""
    extract_logfile = instantiate_logfile(league)
    print(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full {league.upper()} Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')
    extract_logfile.write(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full {league.upper()} Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n')
    
    print(f'\n~~ Retrieving {league.upper()} Game IDs for {year} schedule ~~')
    extract_logfile.write(f'\n~~ Retrieving {league.upper()} Game IDs for {year} schedule ~~\n')
    if league in ['CFB', 'NFL']: 
        game_ids = schedule.get_football_game_ids(league, year, weeks, extract_logfile)
    elif league in ['MLB', 'NBA']:
        game_ids = schedule.get_non_football_game_ids(league, schedule_window_begin, schedule_window_end, extract_logfile)
    else:
        print(f'\n~~ Invalid League: {league.upper()}')
        extract_logfile.write(f'\n~~ Invalid League: {league.upper()}\n')

    print(f'\n~~ Retrieving {league.upper()} Game Data ~~')
    extract_logfile.write(f'\n~~ Retrieving {league.upper()} Game Data ~~\n')
    games_raw = extract_games(league, game_ids, extract_logfile)
    print(games_raw)

    print(f'\n\n~~ Retrieving {league.upper()} Teams Data ~~')
    extract_logfile.write(f'\n\n~~ Retrieving {league.upper()} Teams Data ~~\n')
    teams_raw = extract_teams(league, games_raw['away_team_id'].unique(), extract_logfile)

    print(f'\n\n~~ Retrieving {league.upper()} Locations Data ~~')
    extract_logfile.write(f'\n\n~~ Retrieving {league.upper()} Locations Data ~~\n')
    locations_raw = extract_locations(league, games_raw['stadium'], games_raw['location'], extract_logfile)

    print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n')
    extract_logfile.write('\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n\n')

    return games_raw, teams_raw, locations_raw