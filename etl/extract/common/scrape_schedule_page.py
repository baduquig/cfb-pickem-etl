"""
Pickem ETL
Author: Gabe Baduqui

Scrape all Game IDs for a given season/week(s).
"""
import requests
from bs4 import BeautifulSoup

def get_game_id(game_row_html: str):
    """Function that extracts the Game ID from the <TR> HTML Element for a given game.
       Accepts `game_row_html`: <tr> HTML Element String
       Returns `game_id`: String"""
    # Example `game_row_html` string: '<tr class="Table__TR Table__TR--sm Table__even" data-idx="0">...</tr>'
    try:
        td_elem = game_row_html.find_all('td')[2]
        href_str = td_elem.find('a', href=True)['href']
        begin_index = href_str.index('gameId=') + 7
        end_index = href_str.index('&_slug_=')
        game_id = href_str[begin_index:end_index]
    except:
        game_id = None
    return game_id

def get_all_game_ids(league: str, year: any, weeks: any, logfile: object):
    """Function that scrapes the Game ID from each game row for a given season.
       Accepts: `espn_schedule_url`: String, `year`: Number, `weeks`: Number, `logfile`: File Object
       Returns: game_ids: List of Strings"""
    if league.upper() == 'CFB':
        espn_schedule_url = 'https://www.espn.com/college-football/schedule/_/'
    else:
        espn_schedule_url = 'https://www.espn.com/nfl/schedule/_/'

    game_ids = []
    custom_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    
    for week in range(weeks):
        week += 1
        print(f'~~~~ Scraping Week {week} Games')
        logfile.write(f'~~~~ Scraping Week {week} Games\n')

        espn_current_week_url = espn_schedule_url + f'week/{week}/year/{year}/'

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        page = requests.get(espn_current_week_url, headers=custom_header)
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
                        game_id = get_game_id(game_row)
                        if game_id is not None:
                            game_ids.append(game_id)
                        else:
                            print(f'Error occurred while extracting Game IDs from\n{game_row}\n')
                            logfile.write(f'Error occurred while extracting Game IDs from\n{game_row}\n\n')
                except:
                    print(f'Error occurred while extracting Games from\n{gameday}\n')
                    logfile.write(f'Error occurred while extracting Games from\n{gameday}\n\n')
        except Exception:
            print(f'Error occurred scraping schedule for week {week}')
            logfile.write(f'Error occurred scraping schedule for week {week}\n\n')
        
    print('')
    logfile.write('\n')
    return game_ids