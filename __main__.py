"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football schedule data from various web pages
"""
import requests
from bs4 import BeautifulSoup

etl_log_path = './logs/cfb_etl.log'
logfile = open(etl_log_path, 'a')


def get_game_data(game_id):
    """Function that scrapes the webpage of a given Game ID and extracts needed data fields.
       Accepts parameter: game_id
       Returns DataFrame: games_df"""
    espn_game_url = 'https://www.espn.com/college-football/game?gameId='


def get_game_id(game_row_html):
    """Function that extracts the Game ID from the <TR> HTML for a given game.
       Accepts parameter: game_row_html
       Returns Number: game_id"""
    try:
        td_elem = game_row_html[2]
        href_str = td_elem.find('a', href=True)['href']
        game_id_index = href_str.index('gameId=') + 7
        game_id = href_str[game_id_index:]
    except:
        print(f'\n\nError occurred while extracting Game ID from the TR tag: {game_row_html}\n\n')
        logfile(f'\n\nError occurred while extracting Game ID from the TR tag: {game_row_html}\n\n')
        game_id = 0
    return game_id

def get_all_game_ids(year=2023, season_weeks=15):
    """Function that scrapes the Game ID from each game row for a given season.
       Accepts arguments: year, season_weeks
       Returns List: game_ids"""
    
    game_ids = []
    espn_schedule_url = 'https://www.espn.com/college-football/schedule/_/'
    print('\n~~ Retrieving Game IDs ~~')
    logfile.write('\n~~ Retrieving Game IDs ~~')
    
    for week in range(season_weeks):
        week += 1
        espn_current_week_url = espn_schedule_url + f'week/{week}/year/2023/'
        print(f'~~~~ Retrieving Week {week} Games')
        logfile.write(f'~~~~ Retrieving Week {week} Games')

        # Scrape HTML from HTTP request to the URL above and store in variable `page_soup`
        custom_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
        }
        game_resp = requests.get(espn_current_week_url, headers=custom_header)
        soup = BeautifulSoup(game_resp.content, 'html.parser')

        # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
        schedule_div = soup.find_all('div', class_='mt3')[1]
        
        # Iterate through each distinct day with games on this particular week
        try:
            for day in schedule_div.children:
                games_table = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')
                
                for game_row in games_table:
                    game_id = get_game_id(game_row)
                    if game_id != 0:
                        game_ids.append(game_id)
        except:
            print(f'\n\nError occurred while scraping week {week} webpage!!!\n\n')
            logfile.write(f'\n\nError occurred while scraping week {week} webpage!!!\n\n')
    return game_ids


def main():
    # Begin ETL Process
    game_ids = get_all_game_ids()

if __name__ == '__main__':
    main()