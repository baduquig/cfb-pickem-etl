"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape college football schedule data from various web sources.
"""
import pandas as pd
from cfb_etl.cfb_extract.scrape_schedule_page import get_all_game_ids
from cfb_etl.cfb_extract.scrape_game_page import get_game_data
from cfb_etl.cfb_extract.scrape_school_page import get_school_data

etl_log_path = './logs/cfb_etl.log'
logfile = open(etl_log_path, 'a')

def full_extract():
    games_df = pd.DataFrame([], columns=['away_school_id', 'home_school_id', 
                                         'away_school_box_score', 'home_school_box_score', 
                                         'stadium', 'location', 'game_timestamp', 
                                         'tv_coverage', 'betting_line', 'betting_over_under', 
                                         'stadium_capacity', 'attendance', 'away_win_pct', 'home_win_pct'])
    
    schools_df = pd.DataFrame([], columns=['name', 'mascot', 'logo_url', 'conference_name', 
                                           'conference_record', 'overall_record'])
    

    game_ids = get_all_game_ids(logfile)

    for game_id in game_ids:
        game_data = get_game_data(game_id, logfile)
        new_game_row = pd.DataFrame([game_data])
        games_df = pd.concat([games_df, new_game_row], ignore_index=True)

    for school_id in games_df['home_school_id'].unique:
        school_data = get_school_data(school_id, logfile)
        new_school_row = pd.DataFrame([school_data])
        schools_df = pd.concat([schools_df, new_school_row], ignore_index=True)

    return games_df, schools_df