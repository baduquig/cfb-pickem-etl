"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football schedule data from various web pages
"""
import requests
from bs4 import BeautifulSoup

etl_log_path = './logs/cfb_etl.log'
logfile = open(etl_log_path, 'a')


def get_school_id(school_anchor_tag):
    """Function that extracts the School ID from the HREF attribute from a given Anchor Tag.
       Accepts `team_anchor_tag`: <a> HTML Element String
       Returns `school_id`: String"""
    try:
        school_hyperlink = school_anchor_tag['href']
        # Example `school_hyperlink` string: 'https://www.espn.com/college-football/team/_/id/0000/schoolName-schoolMascot'
        begin_idx = school_anchor_tag.index('/id/') + 4
        end_idx = school_anchor_tag.rfind('/')
        school_id = school_hyperlink[begin_idx:end_idx]
    except:
        print(f'\n\nError occurred while extracting School ID from Anchor Tag: \n\n{school_anchor_tag}\n\n')
        logfile(f'\n\nError occurred while extracting School ID from Anchor Tag: \n\n{school_anchor_tag}\n\n')
        school_id = None
    return school_id

def get_away_school_id(gamestrip):
    """Function that scrapes the Away School ID from a given 'Gamestrip' DIV tag.
       Accepts `gamestrip`: <div> HTML Element String
       Returns `school_id`: String"""
    try:
        away_school_container = gamestrip.find('div', class_='Gamestrip__Team--away').find('div', class_='Gamestrip__TeamContainer')
        away_school_a_tag = away_school_container.find('a')
        school_id = get_school_id(away_school_a_tag)
    except:
        print(f'\n\nError occurred while extracting Team ID from DIV: \n\n{gamestrip}\n\n')
        logfile(f'\n\nError occurred while extracting Team ID from the DIV: \n\n{gamestrip}\n\n')
        school_id = None
    return school_id

def get_home_school_id(gamestrip):
    """Function that scrapes the Home School ID from a given 'Gamestrip' DIV tag.
       Accepts `gamestrip`: <div> HTML Element String
       Returns `school_id`: String"""
    try:
        home_school_container = gamestrip.find('div', class_='Gamestrip__Team--home').find('div', class_='Gamestrip__TeamContainer')
        home_school_a_tag = home_school_container.find('a')
        school_id = get_school_id(home_school_a_tag)
    except:
        print(f'\n\nError occurred while extracting Team ID from DIV: \n\n{gamestrip}\n\n')
        logfile(f'\n\nError occurred while extracting Team ID from the DIV: \n\n{gamestrip}\n\n')
        school_id = None
    return school_id

def get_box_score_table(gamestrip):
    """Function that scrapes the Box Score from a given 'Gamestrip' DIV tag.
       Accepts `gamestrip`: <div> HTML Element String
       Returns `box_score_table`: <tbody> HTML Element String"""
    try:
        box_score_container = gamestrip.find('div', class_='Gamestrip__Overview').find('div', class_='Gamestrip__Table').find('div', class_='Table__Scroller').find('table')
        box_score_table = box_score_container.find('tbody', class_='Table__TBODY')
    except:
        print(f'\n\nError occurred while extracting Box Score from DIV: \n\n{gamestrip}\n\n')
        logfile(f'\n\nError occurred while extracting Box Score from the DIV: \n\n{gamestrip}\n\n')
        box_score_table = None
    return box_score_table

def get_away_box_score(box_score_tbody):
    """Function that scrapes the Away school Box Score from a given 'Gamestrip' DIV tag.
       Accepts `box_score_tbody`: <tbody> HTML Element String
       Returns `away_box_score`: Dictionary"""
    away_box_score = {}
    try:
        away_box_score_quarters = get_box_score_table.find_all('tr')[0].find_all('td')
        away_box_score['1'] = away_box_score_quarters[1]
        away_box_score['2'] = away_box_score_quarters[2]
        away_box_score['3'] = away_box_score_quarters[3]
        away_box_score['4'] = away_box_score_quarters[4]
        away_box_score['total'] = away_box_score_quarters[5]
    except:
        away_box_score['1'] = None
        away_box_score['2'] = None
        away_box_score['3'] = None
        away_box_score['4'] = None
        away_box_score['total'] = None
        print(f'\n\nError occurred while extracting Away Box Score from DIV: \n\n{box_score_tbody}\n\n')
        logfile(f'\n\nError occurred while extracting Away Box Score from the DIV: \n\n{box_score_tbody}\n\n')
    return away_box_score

def get_home_box_score(box_score_tbody):
    """Function that scrapes the Home School Box Score from a given 'Gamestrip' DIV tag.
       Accepts `box_score_tbody`: <tbody> HTML Element String
       Returns `home_box_score`: Dictionary"""
    home_box_score = {}
    try:
        home_box_score_quarters = get_box_score_table.find_all('tr')[1].find_all('td')
        home_box_score['1'] = home_box_score_quarters[1]
        home_box_score['2'] = home_box_score_quarters[2]
        home_box_score['3'] = home_box_score_quarters[3]
        home_box_score['4'] = home_box_score_quarters[4]
        home_box_score['total'] = home_box_score_quarters[5]
    except:
        home_box_score['1'] = None
        home_box_score['2'] = None
        home_box_score['3'] = None
        home_box_score['4'] = None
        home_box_score['total'] = None
        print(f'\n\nError occurred while extracting Home Box Score from DIV: \n\n{box_score_tbody}\n\n')
        logfile(f'\n\nError occurred while extracting Home Box Score from the DIV: \n\n{box_score_tbody}\n\n')
    return home_box_score

def get_stadium(information):
    """Function that scrapes the Stadium Name from a given 'GameInfo' Section tag.
       Accepts `information`: <section> HTML Element String
       Returns `stadium_name: String`"""
    # Example `information` string: '<section class="Card GameInfo">...</section>
    try:
        stadium_name = information.find('div', class_='GameInfo__Location__Name').text
    except:
        print(f'\n\nError occurred while extracting Stadium name from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting Stadium name from the DIV: \n\n{information}\n\n')
        stadium_name = None
    return stadium_name

def get_location(information):
    """Function that scrapes the City and State from a given 'GameInfo' Section tag.
       Accepts `information`: <section> HTML Element String
       Returns `location`: String"""
    # Example `information` string: '<section class="Card GameInfo">...</section>
    try:
        location = information.find('span', class_='Location__Text').text
    except:
        print(f'\n\nError occurred while extracting Game Location from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting Game Location from the DIV: \n\n{information}\n\n')
        location = None
    return location

def get_timestamp(information):
    """Function that scrapes the Game Date and Time from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `game_timestamp`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        game_timestamp = information.find('div', class_='GameInfo__Meta').find('span')[0].text
    except:
        print(f'\n\nError occurred while extracting Game Date and Time from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting Game Date and Time from the DIV: \n\n{information}\n\n')
        game_timestamp = None
    return game_timestamp

def get_tv_coverage(information):
    """Function that scrapes the TV Coverage Channel from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `tv_coverage`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        tv_coverage = information.find('div', class_='GameInfo__Meta').find('span')[1].text
    except:
        print(f'\n\nError occurred while extracting TV Coverage Channel from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting TV Coverage Channel from the DIV: \n\n{information}\n\n')
        tv_coverage = None
    return tv_coverage

def get_betting_line(information):
    """Function that scrapes the Betting Line from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `betting_line`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        betting_line = information.find('div', class_='GameInfo__BettingItem line')
    except:
        print(f'\n\nError occurred while extracting Betting Line from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting Betting Line from the DIV: \n\n{information}\n\n')
        betting_line = None
    return betting_line

def get_betting_over_under(information):
    """Function that scrapes the Betting Over/Under from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `betting_over_under`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        betting_over_under = information.find('div', class_='GameInfo__BettingItem ou')
    except:
        print(f'\n\nError occurred while extracting Betting Over/Under from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting Betting Over/Under from the DIV: \n\n{information}\n\n')
        betting_over_under = None
    return betting_over_under

def get_stadium_capacity(information):
    """Function that scrapes the Stadium Capacity from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `stadium_capacity`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        stadium_capacity = information.find('div', class_='Attendance__Capacity')
    except:
        print(f'\n\nError occurred while extracting Stadium Capacity from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting Stadium Capacity from the DIV: \n\n{information}\n\n')
        stadium_capacity = None
    return stadium_capacity

def get_attendance(information):
    """Function that scrapes the Game Attendance from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `attendance`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        attendance = information.find('div', class_='Attendance__Capacity')
    except:
        print(f'\n\nError occurred while extracting Game Attendance from DIV: \n\n{information}\n\n')
        logfile(f'\n\nError occurred while extracting Game Attendance from the DIV: \n\n{information}\n\n')
        attendance = None
    return attendance

def get_away_winning_probability(matchup):
    """Function that scrapes the winning probability percentage of the Away School for a given 'matchup' DIV tag.
       Accepts `matchup`: <div> HTML Element String
       Returns `win_pct`: String"""
    # Example `matchup` string: '<div class="matchupPredictor">...</div>'
    try:
        win_pct = matchup.find('div', class_='matchupPredictor__teamValue')[0].text
    except:
        print(f'\n\nError occurred while extracting Away School Winning Percentage from DIV: \n\n{matchup}\n\n')
        logfile(f'\n\nError occurred while extracting Away School Winning Percentage from the DIV: \n\n{matchup}\n\n')
        win_pct = None
    return win_pct

def get_home_winning_probability(matchup):
    """Function that scrapes the winning probability percentage of the Away School for a given 'matchup' DIV tag.
       Accepts `matchup`: <div> HTML Element String
       Returns `win_pct`: String"""
    # Example `matchup` string: '<div class="matchupPredictor">...</div>'
    try:
        win_pct = matchup.find('div', class_='matchupPredictor__teamValue')[1].text
    except:
        print(f'\n\nError occurred while extracting Home School Winning Percentage from DIV: \n\n{matchup}\n\n')
        logfile(f'\n\nError occurred while extracting Home School Winning Percentage from the DIV: \n\n{matchup}\n\n')
        win_pct = None
    return win_pct

def get_game_data(game_id):
    """Function that scrapes the webpage of a given Game ID and extracts needed data fields.
       Accepts `game_id`: Number
       Returns `game_data`: Dictionary"""
    print(f'~~ Scraping GameID {game_id} information')
    logfile.write(f'~~ Scraping GameID {game_id} information')
    espn_game_url = f'https://www.espn.com/college-football/game?gameId={game_id}'

    # Scrape HTML from HTTP request to the URL above and store in variable `page_soup`
    custom_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    game_resp = requests.get(espn_game_url, headers=custom_header)
    game_soup = BeautifulSoup(game_resp.content, 'html.parser')

    game_data = {}

    try:
        # Instantiate Gamestring Container data fields
        gamestrip_div = game_soup.find('div', class_='Gamestrip')
        away_school_id = get_away_school_id(gamestrip_div)
        home_school_id = get_home_school_id(gamestrip_div)
        try:
            away_school_box_score = get_away_box_score(gamestrip_div)
            home_school_box_score = get_home_box_score(gamestrip_div)
        except:
            away_school_box_score = None
            home_school_box_score = None
            print(f'\nBox Score Table not present on webpage.\n')
            logfile(f'\nBox Score Table not present on webpage.\n')
    except:
        print(f'\n\nError occurred while scraping `Gamestrip` DIV from Game webpage: \n\n{game_soup}\n\n')
        logfile(f'\n\nError occurred while scraping `Gamestrip` DIV from Game webpage: \n\n{game_soup}\n\n')
        away_school_id = None
        home_school_id = None
    
    try:
        # Instantiate Information Section data fields
        info_section = game_soup.find('section', class_='GameInfo')
        stadium = get_stadium(info_section)
        location = get_location(info_section)
        game_timestamp = get_timestamp(info_section)
        tv_coverage = get_tv_coverage(info_section)
        betting_line = get_betting_line(info_section)
        betting_over_under = get_betting_over_under(info_section)
        stadium_capacity = get_stadium_capacity(info_section)
        attendance = get_attendance(info_section)
    except:
        print(f'\n\nError occurred while scraping `GameInfo` SECTION from Game webpage: \n\n{game_soup}\n\n')
        logfile(f'\n\nError occurred while scraping `GameInfo` SECTION from Game webpage: \n\n{game_soup}\n\n')
        stadium = None
        location = None
        game_timestamp = None
        tv_coverage = None
        betting_line = None
        betting_over_under = None
        stadium_capacity = None
        attendance = None

    try: 
        matchup_div = game_soup.find('div', class_='matchupPredictor')
        away_win_pct = get_away_winning_probability(matchup_div)
        home_win_pct = get_home_winning_probability(matchup_div)
    except:
        away_win_pct = None
        home_win_pct = None



def get_game_id(game_row_html):
    """Function that extracts the Game ID from the <TR> HTML Element for a given game.
       Accepts `game_row_html`: <tr> HTML Element String
       Returns `game_id`: String"""
    # Example `game_row_html` string: '<tr class="Table__TR Table__TR--sm Table__even" data-idx="0">...</tr>'
    try:
        td_elem = game_row_html[2]
        href_str = td_elem.find('a', href=True)['href']
        game_id_index = href_str.index('gameId=') + 7
        game_id = href_str[game_id_index:]
    except:
        print(f'\n\nError occurred while extracting Game ID from the TR tag: {game_row_html}\n\n')
        logfile(f'\n\nError occurred while extracting Game ID from the TR tag: {game_row_html}\n\n')
        game_id = None
    return game_id

def get_all_game_ids(year=2023, season_weeks=15):
    """Function that scrapes the Game ID from each game row for a given season.
       Accepts: `year`: Number, `season_weeks`: Number
       Returns List: game_ids"""
    
    game_ids = []
    espn_schedule_url = 'https://www.espn.com/college-football/schedule/_/'
    print('\n~~ Retrieving Game IDs ~~')
    logfile.write('\n~~ Retrieving Game IDs ~~')
    
    for week in range(season_weeks):
        week += 1
        espn_current_week_url = espn_schedule_url + f'week/{week}/year/2023/'
        print(f'~~~~ Scraping Week {week} Games')
        logfile.write(f'~~~~ Scraping Week {week} Games')

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        custom_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
        }
        resp = requests.get(espn_current_week_url, headers=custom_header)
        week_soup = BeautifulSoup(resp.content, 'html.parser')

        # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
        schedule_div = week_soup.find_all('div', class_='mt3')[1]
        
        # Iterate through each distinct day with games on this particular week
        try:
            for day in schedule_div.children:
                games_table = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')
                
                for game_row in games_table:
                    game_id = get_game_id(game_row)
                    if all(game_id):
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