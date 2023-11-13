"""
Pickem ETL
Author: Gabe Baduqui

Scrape college football schedule data from various web sources.
"""
import pandas as pd
from etl.common.get_timestamp import get_timestamp
from etl.cfb.extract.scrape_schedule_page import get_all_game_ids
from etl.cfb.extract.scrape_game_page import get_game_data
from etl.cfb.extract.scrape_school_page import get_school_data
from etl.cfb.extract.get_geocode_data import get_location_data

timestamp = get_timestamp()
cfb_extract_logfile = f'./logs/cfb_extract_{timestamp}.log'
cfb_extract_logfile = open(cfb_extract_logfile, 'a')

def full_extract():
    games_df = pd.DataFrame([], columns=['away_school_id', 'home_school_id', 
                                         'away_school_box_score', 'home_school_box_score', 
                                         'stadium', 'location', 'game_timestamp', 
                                         'tv_coverage', 'betting_line', 'betting_over_under', 
                                         'stadium_capacity', 'attendance', 'away_win_pct', 'home_win_pct'])    
    schools_df = pd.DataFrame([], columns=['name', 'mascot', 'logo_url', 'conference_name', 
                                           'conference_record', 'overall_record'])    
    locations_df = pd.DataFrame([], columns=['location_id', 'stadium', 'city', 
                                             'state', 'latitude', 'longitude'])
    
    game_ids = get_all_game_ids(logfile=cfb_extract_logfile)
    stadium_locations = []
    location_id = 1

    for game_id in game_ids:
        game_data = get_game_data(game_id, logfile=cfb_extract_logfile)
        new_game_row = pd.DataFrame([game_data])
        games_df = pd.concat([games_df, new_game_row], ignore_index=True)
        stadium_locations.append((game_data['stadium'], game_data['location']))

    for school_id in games_df['home_school_id'].unique():
        school_data = get_school_data(school_id, logfile=cfb_extract_logfile)
        new_school_row = pd.DataFrame([school_data])
        schools_df = pd.concat([schools_df, new_school_row], ignore_index=True)

    for stadium_location in stadium_locations:
        stadium = stadium_location[0]
        location_name = stadium_location[1]
        location_data = get_location_data(location_id, stadium, location_name, logfile=cfb_extract_logfile)
        new_location_row = pd.DataFrame([location_data])
        locations_df = pd.concat([locations_df, new_location_row], ignore_index=True)
        

    return games_df, schools_df, locations_df