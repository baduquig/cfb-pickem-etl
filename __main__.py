"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load fall sports schedule data from various web pages
"""
from flask import Flask, jsonify, request
import etl.etl as x
import schedule, time

app = Flask(__name__)

prod = False

cfb_games_endpoint = None
cfb_teams_endpoint = None
cfb_locations_endpoint = None
nfl_games_endpoint = None
nfl_teams_endpoint = None
nfl_locations_endpoint = None
mlb_games_endpoint = None
mlb_teams_endpoint = None
mlb_locations_endpoint = None
nba_games_endpoint = None
nba_teams_endpoint = None
nba_locations_endpoint = None

@app.route('/cfb_games')
def return_cfb_games():
    response = jsonify(cfb_games_endpoint.to_dict(orient='records'))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

def main():
    global cfb_games_endpoint
    global cfb_teams_endpoint
    global cfb_locations_endpoint
    global nfl_games_endpoint
    global nfl_teams_endpoint
    global nfl_locations_endpoint
    global mlb_games_endpoint
    global mlb_teams_endpoint
    global mlb_locations_endpoint
    global nba_games_endpoint
    global nba_teams_endpoint
    global nba_locations_endpoint
    
    cfb_games_endpoint, cfb_teams_endpoint, cfb_locations_endpoint = x.full_etl(prod, 'CFB')
    nfl_games_endpoint, nfl_teams_endpoint, nfl_locations_endpoint = x.full_etl(prod, 'NFL')
    mlb_games_endpoint, mlb_teams_endpoint, mlb_locations_endpoint = x.full_etl(prod, 'MLB')
    nba_games_endpoint, nba_teams_endpoint, nba_locations_endpoint = x.full_etl(prod, 'NBA')

if __name__ == '__main__':
    if prod:
        schedule.every().day.at("02:00").do(main)
        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        main()