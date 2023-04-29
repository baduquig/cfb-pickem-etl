import pandas as pd
from web_scraping_classes.scrape_games import ScrapeGames
from web_scraping_classes.scrape_schools import ScrapeSchools
from web_scraping_classes.scrape_conferences import ScrapeConferences

#scrape = ScrapeAll()
games = ScrapeGames()
schools = ScrapeSchools()
conferences = ScrapeConferences()

def full_extract(year):
    games_df = games.scrape_games(year)
    unique_schools = games_df[games_df['home_school'] != '0']['home_school'].unique()
    schools_df = schools.scrape_schools(unique_schools)
    conferences_df = conferences.scrape_conferences()
    return games_df, schools_df, conferences_df

def full_transform(schools_df, old_conf_df):
    schools_df = pd.merge(schools_df, old_conf_df, on='school_id', how='left')
    schools_df = schools_df.drop(['conference_name', 'division_name'], axis=1)    
    conferences_df = old_conf_df.drop(['school_id'], axis=1)
    return schools_df, conferences_df

def full_etl(year):
    games_df, schools_df, conferences_df = full_extract(year)
    schools_df, conferences_df = full_transform(schools_df, conferences_df)



full_etl(2023)