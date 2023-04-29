import pandas as pd
import requests
from bs4 import BeautifulSoup
from etl_classes.scrape_all import ScrapeAll


class ScrapeConferences(ScrapeAll):
    """This class contains the methods needed to scrape college football conferences and
    division data from ESPN at (https://www.espn.com/college-football/team/_/id/WXYZ/)."""
    def __init__(self):
        super().__init__()
        self.conferences_df = pd.DataFrame(columns=['school_id', 'conference_id', 'division_id', 'conference_name', 'division_name'])
        self.standings_url = 'https://www.espn.com/college-football/standings'

    def scrape_conferences(self):
        """Method to scrape conference/division data from https://www.espn.com/college-football/standings for a given list of schools."""
        print('\nBeginning scraping conference data.')
        self.logfile.write('\nBeginning scraping conference data.\n')

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        response = requests.get(self.standings_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Instantiate parent element variables
        conference_divs = soup.find('div', class_='tabs__content').find('section').find_all('div', class_='standings__table InnerLayout__child--dividers')
        #conf_container = soup.find('div', class_='tabs__content').find('section')
        #conference_divs = conf_container.find_all('div', class_='standings__table InnerLayout__child--dividers')

        # Instantiate surrogate IDs 
        conference_id = 1
        division_id = 10

        # Iterate through each overarching conference DIV
        for conf_div in conference_divs:
            print(f'  ~ Scraping data for Conference ID: {conference_id}...')
            self.logfile.write(f'  ~ Scraping data for Conference ID: {conference_id}...\n')

            conference_title_div = conf_div.find('div', class_='Table__Title')
            conference_table = conf_div.find('div', class_='flex').find('table')

            # Instantiate conference field for all teams in this div
            conference_name = self.get_cell_text(conference_title_div)
            division_name = ''
            conference_table_rows = conference_table.find('tbody', class_='Table__TBODY').find_all('tr')
            #conf_tbody = conference_table.find('tbody', class_='Table__TBODY')
            #conf_trows = conf_tbody.find_all('tr')

            # Iterate through divisions/teams in Table Body
            for conference_row in conference_table_rows:
                #subgroup_header_class = 'subgroup-headers'
                #tr_class = conference_row.get('class')

                if 'subgroup-headers' in conference_row.get('class'):
                    division_span = conference_row.find('td', class_='Table__TD').find('span')
                    division_name = self.get_cell_text(division_span)
                    division_id += 1
                else:
                    school_span = conference_row.find('td').find('div').find_all('span')[2]
                    school_id = self.get_school_id(school_span)

                # Assign new DataFrame rows
                # self.school_confs_df = pd.DataFrame(columns=['school_id', 'division_id'])
                # self.conferences_df = pd.DataFrame(columns=['division_id', 'conference_id', 'division_name', 'conference_name'])
                new_school_conference = pd.DataFrame({
                    'school_id': [school_id], 
                    'conference_id': [conference_id],
                    'division_id': [division_id],
                    'conference_name': [conference_name], 
                    'division_name': [division_name]
                })                
                self.conferences_df = pd.concat([self.conferences_df, new_school_conference], ignore_index=True)
                
            conference_id += 1
        print('Completed scraping conference data.\n')
        self.logfile.write('Completed scraping conference data.\n')
        return self.conferences_df
