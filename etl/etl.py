"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load fall sports schedule data from various web pages
"""
import etl.extract.extract as ext
import etl.transform.transform as trf
import etl.load.load as load
from datetime import date

def full_etl(prod: bool, league: str):
    league = league.upper()
    
    # Extract
    if league == 'CFB':
        games_raw, teams_raw, locations_raw = ext.full_extract(league, year=2024, weeks=1)
    elif league == 'NFL':
        games_raw, teams_raw, locations_raw = ext.full_extract(league, year=2024, weeks=18)
    elif league == 'MLB':
        games_raw, teams_raw, locations_raw = ext.full_extract(league, schedule_window_begin=date(2024, 8, 22), schedule_window_end=date(2024, 9, 29))
    elif league == 'NBA':
        games_raw, teams_raw, locations_raw = ext.full_extract(league, schedule_window_begin=date(2024, 10, 1), schedule_window_end=date(2024, 12, 1))
    else:
        print('Invalid League!!!')
        quit()
    
    # Transform
    games, teams, locations = trf.full_transform(league, games_raw, teams_raw, locations_raw)

    # Load
    load.full_load(prod, league, games, teams, locations)