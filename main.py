from etl_classes.extract_all import ExtractAll
from etl_classes.scrape_games import ScrapeGames
from etl_classes.scrape_schools import ScrapeSchools
from etl_classes.scrape_conferences import ScrapeConferences
from etl_classes.get_game_locations import GetGameLocations
from etl_classes.transform_all import TransformAll
from etl_classes.load_data import LoadData


def full_extract(year):
    scrape = ExtractAll()
    games = ScrapeGames()
    schools = ScrapeSchools()
    conferences = ScrapeConferences()
    locations = GetGameLocations()

    games_df = games.scrape_games(year)

    unique_schools = games_df[games_df['awaySchool'] != '0']['awaySchool'].unique()
    schools_df = schools.scrape_schools(unique_schools)

    conferences_df = conferences.scrape_conferences()
    
    unique_locations = games_df['location'].unique()
    locations_df = locations.get_game_locations(unique_locations)

    scrape.logfile.close()
    return games_df, schools_df, conferences_df, locations_df

def full_transform(games_df, schools_df, conferences_df, locations_df):
    transform = TransformAll(games_df, schools_df, conferences_df, locations_df)
    games_df = transform.transform_games_data()
    schools_df = transform.transform_schools_data()
    conferences_df = transform.transform_conferences_data()
    all_data = transform.consolidate_data()

    transform.logfile.close()
    return games_df, schools_df, conferences_df, locations_df, all_data

def full_load(games_df, schools_df, conferences_df, locations_df, all_data):
    load_obj = LoadData(games_df, schools_df, conferences_df, locations_df, all_data)
    load_obj.load_csv()
    load_obj.load_json()
    load_obj.load_mysql_db()
    load_obj.logfile.close()

def full_etl(year):
    games_df, schools_df, conferences_df, locations_df = full_extract(year)
    games_df, schools_df, conferences_df, locations_df, all_data = full_transform(games_df, schools_df, conferences_df, locations_df)
    full_load(games_df, schools_df, conferences_df, locations_df, all_data)



full_etl(2023)