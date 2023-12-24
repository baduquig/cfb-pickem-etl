"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football schedule data from various web pages
"""
import etl.extract.extract as ext
import etl.load.load as load

def main():
    cfb_games, cfb_teams, cfb_locations = ext.full_extract('CFB', 2023, 1)
    #nfl_games, nfl_teams, nfl_locations = ext.full_extract('NFL', 2023, 18)

    load.full_load('CFB', cfb_games, cfb_teams, cfb_locations)
    #load.full_load('NFL', nfl_games, nfl_teams, nfl_locations)

if __name__ == '__main__':
    main()
