import pandas as pd
from datetime import datetime
from web_scraping_classes.scrape_games import ScrapeGames
from web_scraping_classes.scrape_schools import ScrapeSchools
from web_scraping_classes.scrape_conferences import ScrapeConferences

class ScrapeAll:
    """This class contains methods needed to scrape data from more than one source/webpage."""
    def __init__(self, year=2023):
        self.year = year
        self.games_df = pd.DataFrame(columns=['game_date', 'away_school', 'home_school', 'game_id', 'time', 'location'])
        self.schools_df = pd.DataFrame(columns=['school_id', 'logo_url', 'name', 'mascot', 'record', 'wins', 'losses', 'ties'])
        self.school_conferences_df = pd.DataFrame(columns=['school_id', 'conference_id', 'division_id', 'conference_name', 'division_name'])
        
    def get_cell_text(self, td_html_str):
        """Method to extract the innerHTML text of the child tag of a given table cell."""
        try:
            cell_text = td_html_str.contents[0].text
        except:
            cell_text = ''
        return cell_text
    
    def get_school_id(self, school_span):
        """Method to extract SchoolID from the URL in the underlying href attribute."""
        try:
            href_str = school_span.find('a', href=True)['href']
            begin_index = href_str.index('/id/') + 4
            end_index = href_str.rfind('/')
            school_id = href_str[begin_index:end_index]
        except:
            school_id = '0'
        return school_id
    
    def scrape_all(self):
        logfile = open('../logs/scrape_all_' + str(datetime.now()), 'a')

        games = ScrapeGames(logfile)
        schools = ScrapeSchools(logfile)
        conferences = ScrapeConferences(logfile)

        games_df = games.scrape_games(self.year)
        #non_0_schools = self.games_df[self.games_df['home_school'] != '0']
        #unique_schools = non_0_schools['home_school'].unique()
        unique_schools = self.games_df[games_df['home_school'] != '0']['home_school'].unique()

        schools_df = schools.scrape_schools(unique_schools)
        school_conferences_df = conferences.scrape_conferences()

        self.games_df = games_df
        self.schools_df = schools_df
        self.school_conferences_df = school_conferences_df
        logfile.close()
