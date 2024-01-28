"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football and NFL schedule data from various web pages
"""
import etl.extract.extract as ext
import etl.transform.transform as trf
import etl.load.load as load
from datetime import date

def main():
    cfb_games_raw, cfb_teams_raw, cfb_locations_raw = ext.full_extract('CFB', 2024, 15)
    nfl_games_raw, nfl_teams_raw, nfl_locations_raw = ext.full_extract('NFL', 2024, 18)
    mlb_games_raw, mlb_teams_raw, mlb_locations_raw = ext.full_extract('MLB', schedule_window_begin=date(2024, 8, 22), schedule_window_end=date(2024, 9, 29))
    nba_games_raw, nba_teams_raw, nba_locations_raw = ext.full_extract('NBA', schedule_window_begin=date(2024, 10, 1), schedule_window_end=date(2024, 12, 1))

    cfb_games, cfb_teams, cfb_locations = trf.full_transform('CFB', cfb_games_raw, cfb_teams_raw, cfb_locations_raw)
    nfl_games, nfl_teams, nfl_locations = trf.full_transform('NFL', nfl_games_raw, nfl_teams_raw, nfl_locations_raw)
    mlb_games, mlb_teams, mlb_locations = trf.full_transform('MLB', mlb_games_raw, mlb_teams_raw, mlb_locations_raw)
    nba_games, nba_teams, nba_locations = trf.full_transform('NBA', nba_games_raw, nba_teams_raw, nba_locations_raw)

    games = trf.consolidate_data('games', cfb_df=cfb_games, nfl_df=nfl_games, mlb_df=mlb_games, nba_df=nba_games)
    teams = trf.consolidate_data('teams', cfb_df=cfb_teams, nfl_df=nfl_teams, mlb_df=mlb_teams, nba_df=nba_teams)
    locations = trf.consolidate_data('locations', cfb_df=cfb_locations, nfl_df=nfl_locations, mlb_df=mlb_locations, nba_df=nba_locations)

    load.full_load(games, teams, locations)

if __name__ == '__main__':
    main()
