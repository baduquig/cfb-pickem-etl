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
        game_id_index = href_str.index('gameId=') + 7
        game_id = href_str[game_id_index:]
    except:
        game_id = None
    return game_id

def get_all_game_ids(espn_schedule_url: str, year: any, weeks: any, logfile: str):
    """Function that scrapes the Game ID from each game row for a given season.
       Accepts: `espn_schedule_url`: String, `year`: Number, `weeks`: Number, `logfile`: String
       Returns: game_ids: List"""    
    game_ids = []
    # espn_schedule_url = 'https://www.espn.com/college-football/schedule/_/'
    custom_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    
    for week in range(weeks):
        week += 1
        print(f'~~~~ Scraping Week {week} Games')
        logfile.write(f'~~~~ Scraping Week {week} Games\n')

        espn_current_week_url = espn_schedule_url + f'week/{week}/year/{year}/'

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        resp = requests.get(espn_current_week_url, headers=custom_header)
        week_soup = BeautifulSoup(resp.content, 'html.parser')

        # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
        try:
            schedule_div = week_soup.find_all('div', class_='mt3')[1]
        
            # Iterate through each distinct day with games on this particular week
            for day in schedule_div.children:
                try:
                    games_table_rows = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')
                    
                    for game_row in games_table_rows:
                        game_id = get_game_id(game_row)
                        if game_id is not None:
                            game_ids.append(game_id)
                        else:
                            print(f'Error occurred while extracting Game IDs from\n{game_row}\n')
                            logfile.write(f'Error occurred while extracting Game IDs from\n{game_row}\n\n')
                except:
                    print(f'Error occurred while extracting Games from\n{day}\n')
                    logfile.write(f'Error occurred while extracting Games from\n{day}\n\n')
        except:
            print(f'Error occurred scraping schedule for week {week}\n')
            logfile.write(f'Error occurred scraping schedule for week {week}\n\n')
        
    print('')
    logfile.write('\n')
    return game_ids


cfb_games = get_all_game_ids('https://www.espn.com/college-football/schedule/_/', 2023, 15, open('../../../logs/cfb_extract.log', 'a'))
nfl_games = get_all_game_ids('https://www.espn.com/nfl/schedule/_/', 2023, 18, open('../../../logs/nfl_extract.log', 'a'))

print(cfb_games)
print()
print(nfl_games)