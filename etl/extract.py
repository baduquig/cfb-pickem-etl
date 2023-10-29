"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape college football schedule data from various web sources.
"""
import pandas as pd
from .scrape_schedule_pages import get_all_game_ids as gag
from .scrape_game_pages import get_game_data as gd

etl_log_path = './logs/cfb_etl.log'
logfile = open(etl_log_path, 'a')

def full_extract():
    games_df = pd.DataFrame([], columns=['away_school_id', 'home_school_id', 
                                         'away_school_box_score', 'home_school_box_score', 
                                         'stadium', 'location', 'game_timestamp', 
                                         'tv_coverage', 'betting_line', 'betting_over_under', 
                                         'stadium_capacity', 'attendance', 'away_win_pct', 'home_win_pct'])
    game_ids = gag(logfile)

    for game_id in game_ids:
        game_data = gd(game_id, logfile)
        new_game_row = pd.DataFrame([game_data])
        games_df = pd.concat([games_df, new_game_row], ignore_index=True)

    print(len(game_ids))
    print(games_df)