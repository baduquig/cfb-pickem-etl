import pandas as pd
import requests
from bs4 import BeautifulSoup
from etl_classes.extract_all import ExtractAll


class ScrapeConferences(ExtractAll):
    """This class contains the methods needed to scrape college football conferences and
    division data from ESPN at (https://www.espn.com/college-football/team/_/id/WXYZ/)."""
    def __init__(self):
        super().__init__()

    def scrape_conferences(self):
        """Method to scrape conference/division data from https://www.espn.com/college-football/standings for a given list of schools."""
        self.cfb_etl_log('\nBeginning scraping conference data.')

        conferences_df = pd.DataFrame(columns=['schoolID', 'conferenceID', 'divisionID', 'conferenceName', 'divisionName'])
        standings_url = 'https://www.espn.com/college-football/standings'

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        response = requests.get(standings_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Instantiate parent element variables
        conference_divs = soup.find('div', class_='tabs__content').find('section').find_all('div', class_='standings__table InnerLayout__child--dividers')
        
        # Instantiate surrogate IDs 
        conference_id = 1
        division_id = 10

        # Iterate through each overarching conference DIV
        for conf_div in conference_divs:
            self.cfb_etl_log(f'  ~ Scraping data for Conference ID: {conference_id}')

            conference_title_div = conf_div.find('div', class_='Table__Title')
            conference_table = conf_div.find('div', class_='flex').find('table')

            # Instantiate conference field for all teams in this div
            conference_name = self.get_cell_text(conference_title_div)
            division_name = ''
            conference_table_rows = conference_table.find('tbody', class_='Table__TBODY').find_all('tr')

            # Iterate through divisions/teams in Table Body

            for conference_row in conference_table_rows:
                if 'Table__sub-header' in conference_row.get('class'):
                    division_span = conference_row.find('td', class_='Table__TD').find('span')
                    division_name = self.get_cell_text(division_span)
                    division_id += 1
                else:
                    school_span = conference_row.find('td').find('div').find_all('span')[2]
                    school_id = self.get_school_id(school_span)

                    new_school_conference = pd.DataFrame({
                        'schoolID': [school_id], 
                        'conferenceID': [conference_id],
                        'divisionID': [division_id],
                        'conferenceName': [conference_name], 
                        'divisionName': [division_name]
                    })                
                    conferences_df = pd.concat([conferences_df, new_school_conference], ignore_index=True)
                
            conference_id += 1
            division_id += 1

        self.cfb_etl_log('Completed scraping conference data.\n')
        return conferences_df
