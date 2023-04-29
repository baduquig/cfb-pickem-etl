import pandas as pd
import requests
from bs4 import BeautifulSoup
from web_scraping_classes.scrape_all import ScrapeAll


class ScrapeSchools(ScrapeAll):
    """This class contains the methods needed to scrape college football schools
    data from ESPN at (https://www.espn.com/college-football/team/_/id/WXYZ/)."""
    def __init__(self):
        super().__init__()
        self.schools_df = pd.DataFrame(columns=['school_id', 'logo_url', 'name', 'mascot', 'record', 'wins', 'losses', 'ties'])
    
    def scrape_schools(self, schools_list):
        """ Method to scrape school data from https://www.espn.com/college-football/team/_/id/WXYZ/ for a given list of schools."""
        print('\nBeginning scraping schools data.')
        self.logfile.write('\nBeginning scraping schools data.\n')
        
        # Iterate through distinct schools in list of away schools
        for school_id in schools_list:
            espn_url = 'https://www.espn.com/college-football/team/_/id/' + str(school_id) + '/'
            print(f'  ~ Scraping data for School ID: {school_id}...')
            self.logfile.write(f'  ~ Scraping data for School ID: {school_id}...\n')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Instantiate variable for Parent DIV of main school ("clubhouse") banner
            school_banner = soup.find('h1', class_='ClubhouseHeader__Name')
            name_span, mascot_span = school_banner.find_all('span', class_='db')
            # TODO: Scrape record as this is available in the HTML
            # record_li = school_banner.find('ul', class_='ClubhouseHeader__Record').find_all('li')[0]

            # Instantiate school data elements
            logo = self.get_logo_url(school_id)
            name = self.get_cell_text(name_span)
            mascot = self.get_cell_text(mascot_span)
            #record = self.get_cell_text(record_li)
            record = '0-0'
            try:
                wins, losses, ties = record.split('-')
            except:
                wins, losses = record.split('-')
                ties = '0'
            wins = wins.strip()
            losses = losses.strip()

            # Assign new DataFrame row
            new_school = pd.DataFrame({
                'school_id': [school_id], 'logo_url': [logo], 'name': [name], 'mascot': [mascot], 
                'record': [record], 'wins': [wins], 'losses': [losses], 'ties': [ties]
            })
            self.schools_df = pd.concat([self.schools_df, new_school], ignore_index=True)
        print('Completed scraping schools data.\n')
        self.logfile.write('Completed scraping schools data.\n\n')
        return self.schools_df