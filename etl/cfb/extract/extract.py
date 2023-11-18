"""
Pickem ETL
Author: Gabe Baduqui

Scrape college football schedule data from various web sources.
"""
import pandas as pd
import etl.common.get_timestamp as ts
import etl.cfb.extract.scrape_schedule_page as cfb_schedule
import etl.cfb.extract.scrape_game_page as cfb_game
import etl.cfb.extract.scrape_school_page as cfb_school
import etl.common.get_geocode_data as geo

timestamp = ts.get_timestamp()
cfb_extract_logfile = f'./logs/cfb_extract_{timestamp}.log'
cfb_extract_logfile = open(cfb_extract_logfile, 'a')

def create_games_df(game_ids: list):
    """Function that instantiates a Pandas DataFrame storing Game Data scraped from ESPN Game web pages
       Accepts `game_ids`: List
       Returns `games_df`: Pandas DataFrame"""
    games_df = pd.DataFrame([], columns=['away_school_id', 'home_school_id', 'away_school_box_score', 'home_school_box_score', 
                                         'stadium', 'location', 'game_timestamp', 'tv_coverage', 'betting_line', 
                                         'betting_over_under', 'stadium_capacity', 'attendance', 'away_win_pct', 'home_win_pct'])
    for game_id in game_ids:
        game_data = cfb_game.get_game_data(game_id, logfile=cfb_extract_logfile)
        new_game_row = pd.DataFrame([game_data])
        games_df = pd.concat([games_df, new_game_row], ignore_index=True)
    return games_df

def create_schools_df(school_ids: list):
    """Function that instantiates a Pandas DataFrame storing School Data scraped from ESPN Team web pages
       Accepts `school_ids`: List
       Returns `schools_df`: Pandas DataFrame"""
    schools_df = pd.DataFrame([], columns=['name', 'mascot', 'logo_url', 'conference_name', 'conference_record', 'overall_record'])
    for school_id in school_ids:
        if school_id is not None:
            school_data = cfb_school.get_school_data(school_id, logfile=cfb_extract_logfile)
            new_school_row = pd.DataFrame([school_data])
            schools_df = pd.concat([schools_df, new_school_row], ignore_index=True)

def create_locations_df(stadiums: list, location_names: list):
    """Function that instantiates a Pandas DataFrame storing Geocode Data retrieved from Geocode.maps REST API
       Accepts `stadiums`: List, `location_names`: List
       Returns `locations_df`: Pandas DataFrame"""
    locations_df = pd.DataFrame([], columns=['location_id', 'stadium', 'city', 'state', 'latitude', 'longitude'])
    unique_locations = []
    location_id = 1

    for i in range(len(stadiums)):
        stadium = stadiums[i]
        location_name = location_names[i]
        concatenated_location = f'{stadium}, {location_name}'
        
        if concatenated_location not in unique_locations:
            unique_locations.append(concatenated_location)
            location_data = geo.get_location_data(location_id, stadium, location_name, logfile=cfb_extract_logfile)
            new_location_row = pd.DataFrame([location_data])
            locations_df = pd.concat([locations_df, new_location_row], ignore_index=True)
            location_id += 1
    return locations_df


def full_extract(year=2023, weeks=15):
    """Function that calls all necessary functions to extract all CFB pickem data from required sources and return in Pandas DataFrames
       Accepts `stadiums`: List, `location_names`: List
       Returns `locations_df`: Pandas DataFrame"""
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')
    cfb_extract_logfile.write('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nBeginning Full Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n')
    
    print(f'\n~~ Retrieving Game IDs for {year} schedule ~~')
    cfb_extract_logfile.write(f'\n~~ Retrieving Game IDs for {year} schedule ~~\n')
    game_ids = cfb_schedule.get_all_game_ids(year=year, weeks=weeks, logfile=cfb_extract_logfile)

    print('\n~~ Retrieving Game Data ~~')
    cfb_extract_logfile.write('\n~~ Retrieving Game Data ~~\n')
    games_raw = create_games_df(game_ids)

    print('\n~~ Retrieving Schools Data ~~')
    cfb_extract_logfile.write('\n~~ Retrieving Schools Data ~~\n')
    schools_raw = create_schools_df(games_raw['away_school_id'].unique())

    print('\n~~ Retrieving Locations Data ~~')
    cfb_extract_logfile.write('\n~~ Retrieving Locations Data ~~\n')
    locations_raw = create_locations_df(games_raw['stadium'], games_raw['location'])

    print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n')
    cfb_extract_logfile.write('\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinished Full Extract Jobs\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n\n')

    return games_raw, schools_raw, locations_raw