import pandas as pd
from etl_classes.scrape_all import ScrapeAll
from etl_classes.scrape_games import ScrapeGames
from etl_classes.scrape_schools import ScrapeSchools
from etl_classes.scrape_conferences import ScrapeConferences
from etl_classes.get_game_locations import GetGameLocations
from etl_classes.load_data import LoadData


def full_extract(year):
    scrape = ScrapeAll()
    games = ScrapeGames()
    schools = ScrapeSchools()
    conferences = ScrapeConferences()
    locations = GetGameLocations()

    games_df = games.scrape_games(year)

    unique_schools = games_df[games_df['home_school'] != '0']['home_school'].unique()
    schools_df = schools.scrape_schools(unique_schools)

    conferences_df = conferences.scrape_conferences()
    
    unique_locations = games_df['location'].unique()
    locations_df = locations.get_game_locations(unique_locations)

    scrape.logfile.close()
    return games_df, schools_df, conferences_df, locations_df

def full_transform(games_df, schools_df, old_conf_df, locations_df):
    #games_df['location_id'] = locations_df[locations_df['location_name'] == games_df['location']]
    games_df = pd.merge(games_df, locations_df[['location_id', 'location_name']], left_on='location', right_on='location_id')
    games_df = games_df.drop(['location'])

    schools_df = pd.merge(schools_df, old_conf_df, on='school_id', how='left')
    schools_df = schools_df.drop(['conference_name', 'division_name'], axis=1)    
    
    conferences_df = old_conf_df.drop(['school_id'], axis=1).drop_duplicates()
    return games_df, schools_df, conferences_df

def full_load(games_df, schools_df, conferences_df, locations_df):
    load_obj = LoadData(games_df, schools_df, conferences_df, locations_df)
    load_obj.load_csv()
    load_obj.load_json()
    load_obj.logfile.close()

def full_etl(year):
    games_df, schools_df, conferences_df, locations_df = full_extract(year)
    schools_df, conferences_df = full_transform(games_df, schools_df, conferences_df, locations_df)
    full_load(games_df, schools_df, conferences_df, locations_df)



full_etl(2023)