import pandas as pd
import requests
from bs4 import BeautifulSoup
from etl_classes.extract_all import ExtractAll


class ScrapeSchools(ExtractAll):
    """This class contains the methods needed to scrape college football schools
    data from ESPN at (https://www.espn.com/college-football/team/_/id/WXYZ/)."""
    def __init__(self):
        super().__init__()
    
    def scrape_schools(self, schools_list):
        """ Method to scrape school data from https://www.espn.com/college-football/team/_/id/WXYZ/ for a given list of schools."""
        self.cfb_etl_log('\nBeginning scraping schools data.')
        
        schools_df = pd.DataFrame(columns=['schoolID', 'logoUrl', 'name', 'mascot', 'record', 'wins', 'losses', 'ties'])
        
        # Iterate through distinct schools in list of away schools
        for school_id in schools_list:
            espn_url = 'https://www.espn.com/college-football/team/_/id/' + str(school_id) + '/'
            self.cfb_etl_log(f'  ~ Scraping data for School ID: {school_id}')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            custom_header = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
            }
            response = requests.get(espn_url, headers=custom_header)
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
                'schoolID': [school_id], 'logoUrl': [logo], 'name': [name], 'mascot': [mascot], 
                'record': [record], 'wins': [wins], 'losses': [losses], 'ties': [ties]
            })
            schools_df = pd.concat([schools_df, new_school], ignore_index=True)
            
        self.cfb_etl_log('Completed scraping schools data.\n')
        return schools_df