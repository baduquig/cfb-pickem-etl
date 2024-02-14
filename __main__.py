"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load fall sports schedule data from various web pages
"""
from flask import Flask
import etl.etl as x
import schedule, time

app = Flask(__name__)

prod = False

cfb_games = None 
cfb_teams = None
cfb_locations = None
nfl_games = None
nfl_teams = None
nfl_locations = None
mlb_games = None
mlb_teams = None
mlb_locations = None
nba_games = None
nba_teams = None
nba_locations = None

def main():
    x.full_etl(prod, 'CFB')
    #x.full_etl(prod, 'NFL')
    #x.full_etl(prod, 'MLB')
    #x.full_etl(prod, 'NBA')

if __name__ == '__main__':
    if prod:
        schedule.every().day.at("02:00").do(main)
        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        main()