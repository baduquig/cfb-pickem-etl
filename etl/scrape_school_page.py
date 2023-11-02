"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape all School-specific data elements for a given School ID
"""
import requests
from bs4 import BeautifulSoup


def get_school_data(school_id, logfile):
    """Function that scrapes the webpage of a given School ID and extracts the needed data fields.
    Accepts `school_id`: Number
    Returns school_data: Dictionary"""
    print(f'~~ Scraping SchoolID {school_id} data')
    logfile.write(f'~~ Scraping SchoolID {school_id} data\n')
    espn_school_url = f'https://www.espn.com/college-football/team/_/id/{school_id}'

    # Scrape HTML from HTTP request to the URL above and store in variable `page_soup`
    custom_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    school_resp = requests.get(espn_school_url, headers=custom_header)
    school_soup = BeautifulSoup(school_resp.content, 'html.parser')

    # Instantiate`school_data` dictionary
    school_data = {
        'school_id': school_id
    }

    # Instantiate Clubhouse Container and scrape data fields
    clubhouse_div = school_soup.find('div', class_='ClubhouseHeader').find('div', class_='ClubhouseHeader__Main')


    # Instantiate Team Standings Section and scrape data fields
    standings_sectino = school_soup.find('section', class_='TeamStandings')