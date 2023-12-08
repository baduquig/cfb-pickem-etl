"""
Pickem ETL
Author: Gabe Baduqui

Scrape all Team-specific data elements for a given Team ID
"""
import requests
from bs4 import BeautifulSoup

def get_logo_url(team_id: str):
    """Function that extracts the ESPN url to a given team's PNG image logo
       Accepts `team_id`: String
       Returns `logo_url`: String"""
    try:
        # College team logo
        int(team_id)
        logo_url = f'https://www.espn.com/college-football/team/_/id/{team_id}'
    except:
        # NFL team logo
        logo_url = f'https://www.espn.com/nfl/team/_/name/{team_id}'
    return logo_url

def get_clubhouse_header_span(clubhouse_div: str):
    """Function that scrapes and returns the <H1> tag storing the team name and mascot
       Accepts `clubhose_div`: <div> HTML Element String
       Returns `header_span`: <span> HTML Element String"""
    # Example `clubhouse_div` string: '<div class="ClubhouseHeader__Main">...</div>'
    try:
        header_h1 = clubhouse_div.find('h1', class_='ClubhouseHeader__Name')
        header_span = header_h1.find('span', class_='flex')
    except:
        header_span = None
    return header_span

def get_team_name(clubhouse_div: str):
    """Function that scrapes the team name from a given ClubhouseHeader DIV tag
       Accepts `clubhouse_div`: <div> HTML Element String
       Returns `team_name`: String"""
    # Example `clubhouse_div` string: '<div class="ClubhouseHeader__Main">...</div>'
    header_span = get_clubhouse_header_span(clubhouse_div)
    try:
        team_name_span = header_span.find_all('span')[0]
        team_name = team_name_span.text
    except:
        team_name = None
    return team_name

def get_team_mascot(clubhouse_div: str):
    """Function that scrapes the team name from a given ClubhouseHeader DIV tag
       Accepts `clubhouse_div`: <div> HTML Element String
       Returns `team_name`: String"""
    # Example `clubhouse_div` string: '<div class="ClubhouseHeader__Main">...</div>'
    header_span = get_clubhouse_header_span(clubhouse_div)
    try:
        team_mascot_span = header_span.find_all('span')[1]
        team_mascot = team_mascot_span.text
    except:
        team_mascot = None
    return team_mascot


def get_conference_name(standings_section: str):
    """Function that scrapes the header of the a given TeamStandings SECTION tag and extracts the conference name
       Accepts `standings_section`: <section> HTML Element String
       Returns `conference_name`: String"""
    # Example `standings_section String: '<section class="Card TeamStandings">...</section>'`
    try:
        section_header = standings_section.find('div', class_='Card__Header__Title__Wrapper')
        h3 = section_header.find('h3').text
        conference_name_elements = h3.split(' ')
        conference_name = conference_name_elements[1]
    except:
        conference_name = None
    return conference_name

def get_team_standing_row(conference_standing_rows: str, team_name=None):
    """Function that extracts the the <tr> tag storing the conference and overall records for the current team being scraped
       Accepts `conference_standing_rows`: List of <tr> HTML Element Strings
       Returns `team_standing_row`: <tr> HTML Element String"""
    # Example `conference_standing_rows` list: ['<tr class="Table__TR Table__TR--sm Table__even" data-idx="n">...</tr>', ....]
    team_standing_row = ''
    try:
        for row in conference_standing_rows:
            row_anchor = row.find('td', class_='Table__TD').find('a')
            anchor_text = row_anchor.text
            class_name = row_anchor['class']
            bolded_class = 'AnchorLink fw-bold'

            if ((anchor_text == team_name) or (class_name == bolded_class)):
                team_standing_row = row
    except:
        team_standing_row = None
    return team_standing_row

def get_record_text(record_td: str):
    """Function that extracts the record text from a given TD tag
       Accepts `record_td`: <td> HTML Element String
       Returns `record`: String"""
    # Exampe `record_td` string: '<div class="fw-bold clr-gray-01 Table__TD">...</div>'
    try:
        record_text = record_td.find('span').text
    except:
        record_text = None
    return record_text

def get_conference_record(team_standing_row: str):
    """Function that extracts the conference record from a given TR tag
       Accepts `team_standing_row`: <tr> HTML Element String
       Returns `conference_record`: String"""
    # Example `team_standing_row` string: '<div class="Table__TR Table__TR--sm Table__even">...</div>'
    try:
        conf_record_td = team_standing_row.find_all('td')[1]
        conf_record = get_record_text(conf_record_td)
    except:
        conf_record = None
    return conf_record

def get_overall_record(team_standing_row: str):
    """Function that extracts the overall record from a given TR tag
       Accepts `team_standing_row`: <tr> HTML Element String
       Returns `overall_record`: String"""
    # Example `team_standing_row` string: '<div class="Table__TR Table__TR--sm Table__even">...</div>'
    try:
        overall_record_td = team_standing_row.find_all('td')[2]
        overall_record = get_record_text(overall_record_td)
    except:
        overall_record = None
    return overall_record


def get_team_data(team_id: str, espn_team_url: str, year: int, logfile: object):
    """Function that scrapes the webpage of a given Team and extracts the needed data fields.
       Accepts `team_id`: String, `espn_team_url`: String, `year`: Number, `logfile`: File Object
       Returns team_data: Dictionary"""
    print(f'~~ Scraping TeamID {team_id} data')
    logfile.write(f'~~ Scraping TeamID {team_id} data\n')
    espn_team_url = espn_team_url + team_id

    # Scrape HTML from HTTP request to the URL above and store in variable `page_soup`
    custom_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    team_resp = requests.get(espn_team_url, headers=custom_header)
    team_soup = BeautifulSoup(team_resp.content, 'html.parser')

    # Instantiate `team_data` dictionary
    team_data = {
        'team_id': team_id
    }

    # Instantiate Clubhouse Container and scrape data fields
    try:
        clubhouse_div = team_soup.find('div', class_='ClubhouseHeader').find('div', class_='ClubhouseHeader__Main')
        team_data['name'] = get_team_name(clubhouse_div)
        team_data['mascot'] = get_team_mascot(clubhouse_div)
        team_data['logo_url'] = get_logo_url(team_id)
    except:
        print(f'~~~~ Could not find ClubhouseHeader Container for Team ID: {team_id}')
        logfile.write(f'~~~~ Could not find ClubhouseHeader Container for Team ID: {team_id}\n')

    # Instantiate Team Standings Section and scrape data fields
    try:
        standings_section = team_soup.find('section', class_='TeamStandings')
        team_data['conference_name'] = get_conference_name(standings_section, year)

        standings_tables = standings_section.find('div', class_='Wrapper Card__Content').find_all('div', class_='ResponsiveTable') 
        for standings_table in standings_tables:
            standings_table.find('div', class_='Table__ScrollerWrapper').find('div', class_='Table__Scroller').find('table')
            standings_rows = standings_table.find('tbody').find_all('tr')
            if standings_rows != '' and standings_rows is not None:
                break
        
        team_standing_row = get_team_standing_row(standings_rows, team_data['name'])
        team_data['conference_record'] = get_conference_record(team_standing_row)
        team_data['overall_record'] = get_overall_record(team_standing_row)
    except:
        print(f'~~~~ Could not find Team Standings Section for TeamID: {team_id}')
        logfile.write(f'~~~~ Could not find Team Standings Section for TeamID: {team_id}\n')
    
    return team_data


#team_df = get_team_data('228', 'https://www.espn.com/college-football/team/_/id/', 2023, open('../../../logs/cfb_extract.log', 'a'))
team_df = get_team_data('gb/green-bay-packers', 'https://www.espn.com/nfl/team/_/name/', 2023, open('../../../logs/nfl_extract.log', 'a'))
print(team_df)