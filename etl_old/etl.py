"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load fall sports schedule data from various web pages
"""
import pandas as pd
import etl_old.extract.extract as ext
import etl_old.extract.common.scrape_schedule_page as schedule
import etl_old.transform.transform as trf
import etl_old.load.load as load
from datetime import date

def full_etl(prod: bool, league: str):
    league = league.upper()
    
    # Extract
    if league == 'CFB':
        games_raw, teams_raw, locations_raw = ext.full_extract(league, year=2024, weeks=15)
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

    return games, teams, locations


def incremental_etl(league: str):
    league = league.upper()

    # Extract
    extract_logfile = ext.instantiate_logfile(league)
    game_ids = schedule.get_football_game_ids(league, 2024, 15, extract_logfile)
    games_raw = ext.extract_games(league, game_ids, extract_logfile)

    unique_teams = games_raw['away_team'].unique()
    teams_raw = ext.extract_teams(league, unique_teams, extract_logfile)

    locations = pd.read_csv('./pickem_data/cfb_locations.csv')

    # Transfrom
    transform_logfile = trf.instantiate_logfile(league)
    games = trf.transform_games(league, games_raw, locations, transform_logfile)
    teams = trf.transform_teams(league, teams_raw, transform_logfile)

    # Load
    load_logfile = load.instantiate_logfile(league)
    load.load_csv(games, 'cfb_games', load_logfile)
    load.load_csv(teams, 'cfb_teams', load_logfile)
    