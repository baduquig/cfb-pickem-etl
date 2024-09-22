"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load fall sports schedule data from various web pages
"""
import pandas as pd
import etl_old.etl as x

prod = False

def full():
    global all_games
    global all_teams
    global all_locations
    global all_schedule

    cfb_games, cfb_teams, cfb_locations = x.full_etl(prod, 'CFB')
    nfl_games, nfl_teams, nfl_locations = x.full_etl(prod, 'NFL')
    mlb_games, mlb_teams, mlb_locations = x.full_etl(prod, 'MLB')
    nba_games, nba_teams, nba_locations = x.full_etl(prod, 'NBA')

    all_games = pd.concat([cfb_games, nfl_games, mlb_games, nba_games], axis=0, ignore_index=True)
    all_teams = pd.concat([cfb_teams, nfl_teams, mlb_teams, nba_teams], axis=0, ignore_index=True)
    all_locations = pd.concat([cfb_locations, nfl_locations, mlb_locations, nba_locations], axis=0, ignore_index=True)

    all_schedule = pd.merge(all_games, all_teams, left_on=['league', 'away_team'], right_on=['league', 'team_id'], how='left')
    all_schedule = all_schedule.rename(columns={'name': 'away_team_name', 'mascot': 'away_team_mascot', 'logo_url': 'away_team_logo', 'conference_name': 'away_team_conference', 
                                                'conference_wins': 'away_team_conference_wins', 'conference_losses': 'away_team_conference_losses', 'conference_ties': 'away_team_conference_ties', 
                                                'overall_wins': 'away_team_overall_wins', 'overall_losses': 'away_team_overall_losses', 'overall_ties': 'away_team_overall_ties'})
    all_schedule = pd.merge(all_schedule, all_teams, left_on=['league', 'home_team'], right_on=['league', 'team_id'], how='left')
    all_schedule = all_schedule.rename(columns={'name': 'home_team_name', 'mascot': 'home_team_mascot', 'logo_url': 'home_team_logo', 'conference_name': 'home_team_conference', 
                                                'conference_wins': 'home_team_conference_wins', 'conference_losses': 'home_team_conference_losses', 'conference_ties': 'home_team_conference_ties', 
                                                'overall_wins': 'home_team_overall_wins', 'overall_losses': 'home_team_overall_losses', 'overall_ties': 'home_team_overall_ties'})
    all_schedule = pd.merge(all_schedule, all_locations, left_on=['league', 'location'], right_on=['league', 'location_id'], how='left')
    all_schedule.drop(['team_id_x', 'team_id_y'], axis=1, inplace=True)


    all_schedule.to_csv('./pickem_data/all_schedule.csv', index=False)
    all_schedule.to_json('./pickem_data/all_schedule.json', orient='records')

def full_cfb():
    cfb_games, cfb_teams, cfb_locations = x.full_etl(prod, 'CFB')

def incremental():
    x.incremental_etl('CFB')


#full()
#full_cfb()
incremental()