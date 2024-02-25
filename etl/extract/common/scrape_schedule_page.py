"""
Pickem ETL
Author: Gabe Baduqui

Scrape all Game IDs for a given season/week(s).
"""
import requests
import etl.extract.extract as ex
import etl.utils.get_all_dates_in_range as all_dates
from bs4 import BeautifulSoup
from datetime import datetime

def get_game_id(game_row_html: str, logfile: object):
    """Function that extracts the Game ID from the <TR> HTML Element for a given game.
       Accepts `game_row_html`: <tr> HTML Element String, `logfile`: File Object
       Returns `game_id`: String"""
    # Example `game_row_html` string: '<tr class="Table__TR Table__TR--sm Table__even" data-idx="0">...</tr>'
    try:
        td_elem = game_row_html.find_all('td')[2]
        href_str = td_elem.find('a', href=True)['href']
        begin_index = href_str.index('gameId/') + 7
        end_index = href_str.rfind('/')
        game_id = href_str[begin_index:end_index]
    except:
        game_id = None
        print(f'Error occurred while extracting Game ID from\n{game_row_html}\n')
        logfile.write(f'Error occurred while extracting Game ID from\n{game_row_html}\n\n')
    return game_id

def get_non_football_game_ids(league: str, schedule_window_begin: datetime, schedule_window_end: datetime, logfile: object):
    """Function that scrapes the Game ID from each game row for a given period of a non-football season
       Accepts `league`: String, `schedule_window_begin`: Date, `schedule_window_end`: Date, logfile: File Object"""
    espn_url = f'https://www.espn.com/{league.lower()}/schedule/_/date'
    dates = all_dates.date_range(schedule_window_begin, schedule_window_end)
    game_ids = []
    for distinct_date in dates:
        print(f'~~~~ Scraping {league.upper()} Games for {distinct_date}')
        logfile.write(f'~~~~ Scraping {league.upper()} Games for {distinct_date}\n')

        date_yyyymmdd = str(distinct_date).replace('-', '')
        schedule_page_url = f'{espn_url}/{date_yyyymmdd}'

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        page = requests.get(schedule_page_url, headers=custom_header)
        page_soup = BeautifulSoup(page.content, 'html.parser')

        # Instantiate variable for current day DIV
        try:
            game_table = page_soup.find_all('div', class_='mt3')[1].find_all('div', class_='ScheduleTables')[0].find('div', class_='Table__ScrollerWrapper').find('div', 'Table__Scroller').find('table').find('tbody')
            table_rows = game_table.find_all('tr')
            for game_row in table_rows:
                game_id = get_game_id(game_row, logfile)
                if game_id is not None:
                    game_ids.append(game_id)
        except Exception as e:
            print(f'Error occurred scraping schedule for {distinct_date}\n{e}\n')
            logfile.write(f'Error occurred scraping schedule for {distinct_date}\n{e}\n\n')

    logfile.write('\n')
    return game_ids


def get_football_game_ids(league: str, year: any, weeks: any, logfile: object):
    """Function that scrapes the Game ID from each game row for a given season.
       Accepts: `espn_schedule_url`: String, `year`: Number, `weeks`: Number, `logfile`: File Object
       Returns: game_ids: List of Strings"""
    game_ids = []
    espn_url = 'https://www.espn.com'
    if league.upper() == 'CFB':
        schedule_url = f'{espn_url}/college-football/schedule/_/'
    elif league.upper() == 'NFL':
        schedule_url = f'{espn_url}/nfl/schedule/_/'
    else:
        print(f'Incorrect league `{league.upper()}` inputted!!!')
        logfile.write(f'Incorrect league `{league.upper()}` inputted!!!\n')
        return
    
    for week in range(weeks):
        week += 1
        print(f'~~~~ Scraping {league.upper()} Week {week} Games')
        logfile.write(f'~~~~ Scraping {league.upper()} Week {week} Games\n')

        espn_current_week_url = f'{schedule_url}week/{week}/year/{year}/'

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        page = requests.get(espn_current_week_url, headers=ex.custom_header)
        page_soup = BeautifulSoup(page.content, 'html.parser')

        # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
        try:
            schedule_tables = page_soup.find_all('div', class_='mt3')[1]
            #schedule_tables = page_soup.find_all('div', class_='ScheduleTables')
            
            # Iterate through each distinct day with games on this particular week
            for gameday in schedule_tables:
                try:
                    games_table_rows = gameday.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')
                    
                    for game_row in games_table_rows:
                        game_id = get_game_id(game_row, logfile)
                        if game_id is not None:
                            game_ids.append(game_id)
                except Exception as e:
                    print(f'Error occurred while extracting Games from\n{gameday}\n{e}\n\n')
                    logfile.write(f'Error occurred while extracting Games from\n{gameday}\n{e}\n\n')
        except Exception as e:
            print(f'Error occurred scraping schedule for week {week}\n{e}\n\n')
            logfile.write(f'Error occurred scraping schedule for week {week}\n{e}\n\n')
        
    logfile.write('\n')
    return game_ids