#from web_scraping_classes.scrape_all import ScrapeAll
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

full_extract(2023)