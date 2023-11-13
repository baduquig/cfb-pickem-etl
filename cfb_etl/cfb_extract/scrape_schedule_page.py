"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape all Game IDs for a given season/week(s).
"""
import requests
from bs4 import BeautifulSoup


def get_game_id(game_row_html, logfile):
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
        print(f'\nError occurred while extracting Game ID from the provided TR tag\n')
        logfile(f'\nError occurred while extracting Game ID from the provided TR tag\n')
        game_id = None
    return game_id

def get_all_game_ids(logfile, year=2023, season_weeks=15):
    """Function that scrapes the Game ID from each game row for a given season.
       Accepts: `year`: Number, `season_weeks`: Number
       Returns List: game_ids"""
    
    game_ids = []
    espn_schedule_url = 'https://www.espn.com/college-football/schedule/_/'
    print(f'\n~~ Retrieving Game IDs for {year} schedule ~~')
    logfile.write(f'\n~~ Retrieving Game IDs for {year} schedule ~~\n')
    
    for week in range(season_weeks):
        week += 1
        espn_current_week_url = espn_schedule_url + f'week/{week}/year/{year}/'
        print(f'~~~~ Scraping Week {week} Games')
        logfile.write(f'~~~~ Scraping Week {week} Games\n')

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        custom_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
        }
        resp = requests.get(espn_current_week_url, headers=custom_header)
        week_soup = BeautifulSoup(resp.content, 'html.parser')

        # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
        schedule_div = week_soup.find_all('div', class_='mt3')[1]
        
        # Iterate through each distinct day with games on this particular week
        #try:
        for day in schedule_div.children:
            games_table_rows = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')
            
            for game_row in games_table_rows:
                game_id = get_game_id(game_row, logfile)
                if game_id is not None:
                    game_ids.append(game_id)
    print('')
    logfile.write('\n')
    return game_ids