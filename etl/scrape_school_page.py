"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape all School-specific data elements for a given School ID
"""
import requests
from bs4 import BeautifulSoup

def get_logo_url(clubhouse_div):
    """Function that extracts the ESPN url to a given school's PNG image logo
       Accepts `clubhouse_div`: <div> HTML Element String
       Returns `logo_url`: String"""
    # Example `clubhouse_div` string: '<div class="ClubhouseHeader__Main_Aside pl4 relative">...</div>'


def get_clubhouse_header_span(clubhouse_div):
    """Function that scrapes and returns the <H1> tag storing the school name and mascot
       Accepts `clubhose_div`: <div> HTML Element String
       Returns `header_span`: <span> HTML Element String"""
    # Example `clubhouse_div` string: '<div class="ClubhouseHeader__Main_Aside pl4 relative">...</div>'
    try:
        header_h1 = clubhouse_div.find('h1', class_='ClubhouseHeader__Name')
        header_span = header_h1.find('span')
    except:
        header_span = None
    return header_span

def get_school_name(clubhouse_div):
    """Function that scrapes the school name from a given ClubhouseHeader DIV tag
       Accepts `clubhouse_div`: <div> HTML Element String
       Returns `school_name`: String"""
    # Example `clubhouse_div` string: '<div class="ClubhouseHeader__Main_Aside pl4 relative">...</div>'
    header_span = get_clubhouse_header_span(clubhouse_div)
    try:
        school_name = header_span.find_all('span')[0]
    except:
        school_name = None
    return school_name

def get_school_mascot(clubhouse_div):
    """Function that scrapes the school name from a given ClubhouseHeader DIV tag
       Accepts `clubhouse_div`: <div> HTML Element String
       Returns `school_name`: String"""
    # Example `clubhouse_div` string: '<div class="ClubhouseHeader__Main_Aside pl4 relative">...</div>'
    header_span = get_clubhouse_header_span(clubhouse_div)
    try:
        school_mascot = header_span.find_all('span')[1]
    except:
        school_mascot = None
    return school_mascot



def get_conference_name(standings_section):
    """Function that scrapes the header of the a given TeamStandings SECTION tag and extracts the conference name
       Accepts `standings_section`: <section> HTML Element String
       Returns `conference_name`: String"""
    # Example `standings_section String: '<section class="Card TeamStandings">...</section>'`
    try:
        section_header = standings_section.find('div', class_='Card__Header__Title__Wrapper')
        h3 = section_header.find('h3').text
        conference_name = h3.replace('2023 ', '')
        conference_name = h3.replace(' Standings', '')
    except:
        conference_name = None
    return conference_name

def get_school_standing_row(conference_standing_rows, school_name=None):
    """Function that extracts the the <tr> tag storing the conference and overall records for the current school being scraped
       Accepts `conference_standing_rows`: List of <tr> HTML Element Strings
       Returns `school_standing_row`: <tr> HTML Element String"""
    # Example `conference_standing_rows` list: ['<tr class="Table__TR Table__TR--sm Table__even" data-idx="n">...</tr>', ....]
    try:
        for row in conference_standing_rows:
            row_anchor = row.find('div', class_='Table__TD').find('a')
            anchor_text = row_anchor.text
            class_name = row_anchor['class']
            bolded_class = 'AnchorLink fw-bold'

            if ((anchor_text == school_name) or (class_name == bolded_class)):
                school_standing_row = row
    except:
        school_standing_row = None
    return school_standing_row

def get_record_text(record_td):
    """Function that extracts the record text from a given TD tag
       Accepts `record_td`: <td> HTML Element String
       Returns `record`: String"""
    # Exampe `record_td` string: '<div class="fw-bold clr-gray-01 Table__TD">...</div>'
    try:
        record_text = record_td.find('span').text
    except:
        record_text = None
    return record_text

def get_conference_record(school_standing_row):
    """Function that extracts the conference record from a given TR tag
       Accepts `school_standing_row`: <tr> HTML Element String
       Returns `conference_record`: String"""
    # Example `school_standing_row` string: '<div class="Table__TR Table__TR--sm Table__even">...</div>'
    try:
        conf_record_td = school_standing_row.find_all('td')[1]
        conf_record = get_record_text(conf_record_td)
    except:
        conf_record = None
    return conf_record

def get_overall_record(school_standing_row):
    """Function that extracts the overall record from a given TR tag
       Accepts `school_standing_row`: <tr> HTML Element String
       Returns `overall_record`: String"""
    # Example `school_standing_row` string: '<div class="Table__TR Table__TR--sm Table__even">...</div>'
    try:
        overall_record_td = school_standing_row.find_all('td')[2]
        overall_record = get_record_text(overall_record_td)
    except:
        overall_record = None
    return overall_record


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
    try:
        clubhouse_div = school_soup.find('div', class_='ClubhouseHeader').find('div', class_='ClubhouseHeader__Main')
        school_data['name'] = get_school_name(clubhouse_div)
        school_data['mascot'] = get_school_mascot(clubhouse_div)
        school_data['logo_url'] = get_logo_url(clubhouse_div)
    except:
        print(f'~~~~ Could not find ClubhouseHeader Container for School ID: {school_id}')
        logfile.write(f'~~~~ Could not find ClubhouseHeader Container for School ID: {school_id}\n')

    # Instantiate Team Standings Section and scrape data fields
    try:
        standings_section = school_soup.find('section', class_='TeamStandings')
        school_data['conference_name'] = get_conference_name(standings_section)

        standings_table = standings_section.find('div', class_='Wrapper Card__Content').find('div', class_='Table__ScrollerWrapper').find('div', class_='Table__Scroller').find('table')
        standings_rows = standings_table.find('tbody').find_all('tr')
        
        school_standing_row = get_school_standing_row(standings_rows)
        school_data['conference_record'] = get_conference_record(school_standing_row)
        school_data['overall_record'] = get_overall_record(school_standing_row)
    except:
        print(f'~~~~ Could not find Team Standings Section for School ID: {school_id}')
        logfile.write(f'~~~~ Could not find Team Standings Section for School ID: {school_id}\n')

    return school_data