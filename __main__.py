"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football schedule data from various web pages
"""
import requests
import pandas as pd
from bs4 import BeautifulSoup

etl_log_path = './logs/cfb_etl.log'
logfile = open(etl_log_path, 'a')


def get_school_id(school_container_div):
    """Function that extracts the School ID from the HREF attribute from a given Anchor Tag.
       Accepts `team_anchor_tag`: <a> HTML Element String
       Returns `school_id`: String"""
    try:
        school_anchor_tag = school_container_div.find('div', class_='Gamestrip__TeamContainer').find('a', href=True)
        school_href_attr = school_anchor_tag['href'] # Example `school_href_attr` string: 'https://www.espn.com/college-football/team/_/id/0000/schoolName-schoolMascot
        begin_idx = school_href_attr.index('/id/') + 4
        end_idx = school_href_attr.rfind('/')
        school_id = school_href_attr[begin_idx:end_idx]
    except:
        school_id = None
    return school_id

def get_away_school_id(gamestrip):
    """Function that scrapes the Away School ID from a given 'Gamestrip' DIV tag.
       Accepts `gamestrip`: <div> HTML Element String
       Returns `school_id`: String"""
    away_school_container = gamestrip.find('div', class_='Gamestrip__Team--away')
    school_id = get_school_id(away_school_container)
    if school_id is None: 
        print(f'~~~~ Could not extract Away School ID')
        logfile.write(f'~~~~ Could not extract Away School ID')
    return school_id

def get_home_school_id(gamestrip):
    """Function that scrapes the Home School ID from a given 'Gamestrip' DIV tag.
       Accepts `gamestrip`: <div> HTML Element String
       Returns `school_id`: String"""
    #try:
    home_school_container = gamestrip.find('div', class_='Gamestrip__Team--home')
    school_id = get_school_id(home_school_container)
    if school_id is None: 
        print(f'~~~~ Could not extract Home School ID')
        logfile.write(f'~~~~ Could not extract Home School ID')
    return school_id

def get_box_score_table(gamestrip):
    """Function that scrapes the Box Score from a given 'Gamestrip' DIV tag.
       Accepts `gamestrip`: <div> HTML Element String
       Returns `box_score_table`: <tbody> HTML Element String"""
    try:
        box_score_container = gamestrip.find('div', class_='Gamestrip__Overview').find('div', class_='Gamestrip__Table').find('div', class_='Table__Scroller').find('table')
        box_score_table = box_score_container.find('tbody', class_='Table__TBODY')
    except:
        box_score_table = None
        print(f'~~~~ No Box Score available to scrape')
        logfile.write(f'~~~~ No Box Score available to scrape\n')
    return box_score_table

def get_away_box_score(gamestrip):
    """Function that scrapes the Away school Box Score from a given 'Gamestrip' DIV tag.
       Accepts `gamestrip`: <div> HTML Element String
       Returns `away_box_score`: Dictionary"""
    away_box_score = {}
    try:
        box_score_tbody = get_box_score_table(gamestrip)
        away_box_score_quarters = box_score_tbody.find_all('tr')[0].find_all('td')
        away_box_score['1'] = away_box_score_quarters[1].text
        away_box_score['2'] = away_box_score_quarters[2].text
        away_box_score['3'] = away_box_score_quarters[3].text
        away_box_score['4'] = away_box_score_quarters[4].text
        away_box_score['total'] = away_box_score_quarters[5].text
    except:
        away_box_score['1'] = None
        away_box_score['2'] = None
        away_box_score['3'] = None
        away_box_score['4'] = None
        away_box_score['total'] = None
    return away_box_score

def get_home_box_score(gamestrip):
    """Function that scrapes the Home School Box Score from a given 'Gamestrip' DIV tag.
       Accepts `gamestring`: <div> HTML Element String
       Returns `home_box_score`: Dictionary"""
    home_box_score = {}
    try:
        box_score_tbody = get_box_score_table(gamestrip)
        home_box_score_quarters = box_score_tbody.find_all('tr')[1].find_all('td')
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
    return home_box_score

def get_stadium(information):
    """Function that scrapes the Stadium Name from a given 'GameInfo' Section tag.
       Accepts `information`: <section> HTML Element String
       Returns `stadium_name: String`"""
    # Example `information` string: '<section class="Card GameInfo">...</section>
    try:
        stadium_name = information.find('div', class_='GameInfo__Location__Name').text
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Stadium name from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting Stadium name from the DIV: \n\n{information}\n')
        stadium_name = None
    return stadium_name

def get_location(information):
    """Function that scrapes the City and State from a given 'GameInfo' Section tag.
       Accepts `information`: <section> HTML Element String
       Returns `location`: String"""
    # Example `information` string: '<section class="Card GameInfo">...</section>
    try:
        location = information.find('span', class_='Location__Text').text
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Game Location from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting Game Location from the DIV: \n\n{information}\n')
        location = None
    return location

def get_timestamp(information):
    """Function that scrapes the Game Date and Time from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `game_timestamp`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        game_timestamp = information.find('div', class_='GameInfo__Meta').find('span')[0].text
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Game Date and Time from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting Game Date and Time from the DIV: \n\n{information}\n')
        game_timestamp = None
    return game_timestamp

def get_tv_coverage(information):
    """Function that scrapes the TV Coverage Channel from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `tv_coverage`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        tv_coverage = information.find('div', class_='GameInfo__Meta').find('span')[1].text
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting TV Coverage Channel from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting TV Coverage Channel from the DIV: \n\n{information}\n')
        tv_coverage = None
    return tv_coverage

def get_betting_line(information):
    """Function that scrapes the Betting Line from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `betting_line`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        betting_line = information.find('div', class_='GameInfo__BettingItem line')
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Betting Line from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting Betting Line from the DIV: \n\n{information}\n')
        betting_line = None
    return betting_line

def get_betting_over_under(information):
    """Function that scrapes the Betting Over/Under from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `betting_over_under`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        betting_over_under = information.find('div', class_='GameInfo__BettingItem ou')
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Betting Over/Under from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting Betting Over/Under from the DIV: \n\n{information}\n')
        betting_over_under = None
    return betting_over_under

def get_stadium_capacity(information):
    """Function that scrapes the Stadium Capacity from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `stadium_capacity`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        stadium_capacity = information.find('div', class_='Attendance__Capacity')
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Stadium Capacity from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting Stadium Capacity from the DIV: \n\n{information}\n')
        stadium_capacity = None
    return stadium_capacity

def get_attendance(information):
    """Function that scrapes the Game Attendance from a given 'information' DIV tag.
       Accepts `information`: <div> HTML Element String
       Returns `attendance`: String"""
    # Example `information` string: '<div class="Gamestrip relative overflow-hidden college-football Gamestrip--xl Gamestrip--post bb">...</div>'
    try:
        attendance = information.find('div', class_='Attendance__Capacity')
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Game Attendance from DIV: \n\n{information}\n')
        #logfile(f'\nError occurred while extracting Game Attendance from the DIV: \n\n{information}\n')
        attendance = None
    return attendance

def get_away_winning_probability(matchup):
    """Function that scrapes the winning probability percentage of the Away School for a given 'matchup' DIV tag.
       Accepts `matchup`: <div> HTML Element String
       Returns `win_pct`: String"""
    # Example `matchup` string: '<div class="matchupPredictor">...</div>'
    try:
        win_pct = matchup.find('div', class_='matchupPredictor__teamValue')[0].text
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Away School Winning Percentage from DIV: \n{matchup}\n')
        #logfile(f'\nError occurred while extracting Away School Winning Percentage from the DIV: \n{matchup}\n')
        win_pct = None
    return win_pct

def get_home_winning_probability(matchup):
    """Function that scrapes the winning probability percentage of the Away School for a given 'matchup' DIV tag.
       Accepts `matchup`: <div> HTML Element String
       Returns `win_pct`: String"""
    # Example `matchup` string: '<div class="matchupPredictor">...</div>'
    try:
        win_pct = matchup.find('div', class_='matchupPredictor__teamValue')[1].text
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while extracting Home School Winning Percentage from DIV: \n{matchup}\n')
        #logfile(f'\nError occurred while extracting Home School Winning Percentage from the DIV: \n{matchup}\n')
        win_pct = None
    return win_pct

def get_game_data(game_id):
    """Function that scrapes the webpage of a given Game ID and extracts needed data fields.
       Accepts `game_id`: Number
       Returns `game_data`: Dictionary"""
    print(f'~~ Scraping GameID {game_id} data')
    logfile.write(f'~~ Scraping GameID {game_id} data\n')
    espn_game_url = f'https://www.espn.com/college-football/game?gameId={game_id}'

    # Scrape HTML from HTTP request to the URL above and store in variable `page_soup`
    custom_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    game_resp = requests.get(espn_game_url, headers=custom_header)
    game_soup = BeautifulSoup(game_resp.content, 'html.parser')

    game_data = {
        'game_id': game_id,
        'away_school_id': None,
        'home_school_id': None,
        'away_school_box_score': None,
        'home_school_box_score': None
    }

    #try:
    # Instantiate Gamestring Container data fields
    gamestrip_div = game_soup.find('div', class_='Gamestrip')

    away_school_id = get_away_school_id(gamestrip_div)
    home_school_id = get_home_school_id(gamestrip_div)
    #away_school_box_score = get_away_box_score(gamestrip_div)
    #home_school_box_score = get_home_box_score(gamestrip_div)
    
    if away_school_id is not None:
        game_data['away_school_id'] = away_school_id
    else:
        print(f'~~~~ Could not extract Away School ID for GameID: {game_id}')
        logfile.write(f'~~~~ Could not extract Away School ID forGame ID: {game_id}\n')
    
    if home_school_id is not None:
        game_data['home_school_id'] = home_school_id
    else:
        print(f'~~~~ Could not extract Home School ID for GameID: {game_id}')
        logfile.write(f'~~~~ Could not extract Home School ID for GameID: {game_id}\n')
    
    game_data['away_school_box_score'] = get_away_box_score(gamestrip_div)
    game_data['home_school_box_score'] = get_home_box_score(gamestrip_div)
    
    """except Exception as e:
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            game_data['away_school_box_score'] = None
            game_data['home_school_box_score'] = None
            #print(f'\nBox Score Table not present on webpage.\n')
            #logfile(f'\nBox Score Table not present on webpage.\n')
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\n\nError occurred while scraping `Gamestrip` DIV from Game webpage: \n{game_soup}\n')
        #logfile(f'\n\nError occurred while scraping `Gamestrip` DIV from Game webpage: \n{game_soup}\n')
        game_data['away_school_id'] = None
        game_data['home_school_id'] = None"""
    
    """try:
        # Instantiate Information Section data fields
        info_section = game_soup.find('section', class_='GameInfo')
        game_data['stadium'] = get_stadium(info_section)
        game_data['location'] = get_location(info_section)
        game_data['game_timestamp'] = get_timestamp(info_section)
        game_data['tv_coverage'] = get_tv_coverage(info_section)
        game_data['betting_line'] = get_betting_line(info_section)
        game_data['betting_over_under'] = get_betting_over_under(info_section)
        game_data['stadium_capacity'] = get_stadium_capacity(info_section)
        game_data['attendance'] = get_attendance(info_section)
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        #print(f'\nError occurred while scraping `GameInfo` SECTION from Game webpage: \n{game_soup}\n')
        #logfile(f'\nError occurred while scraping `GameInfo` SECTION from Game webpage: \n{game_soup}\n')
        game_data['stadium'] = None
        game_data['location'] = None
        game_data['game_timestamp'] = None
        game_data['tv_coverage'] = None
        game_data['betting_line'] = None
        game_data['betting_over_under'] = None
        game_data['stadium_capacity'] = None
        game_data['attendance'] = None

    try: 
        matchup_div = game_soup.find('div', class_='matchupPredictor')
        game_data['away_win_pct'] = get_away_winning_probability(matchup_div)
        game_data['home_win_pct'] = get_home_winning_probability(matchup_div)
    except:
        game_data['away_win_pct'] = None
        game_data['home_win_pct'] = None"""
    
    return game_data



def get_game_id(game_row_html):
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

def get_all_game_ids(year=2023, season_weeks=15):
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
                game_id = get_game_id(game_row)
                if game_id is not None:
                    game_ids.append(game_id)
    return game_ids


def main():
    # Begin ETL Process
    game_ids = get_all_game_ids()
    games_df = pd.DataFrame([], columns=['away_school_id', 'home_school_id', 
                                         'away_school_box_score', 'home_school_box_score', 
                                         'stadium', 'location', 'game_timestamp', 
                                         'tv_coverage', 'betting_line', 'betting_over_under', 
                                         'stadium_capacity', 'attendance', 'away_win_pct', 'home_win_pct'])

    for game_id in game_ids:
        game_data = get_game_data(game_id)
        new_game_row = pd.DataFrame([game_data])
        games_df = pd.concat([games_df, new_game_row], ignore_index=True)
    print(len(game_ids))
    print(games_df)
        

if __name__ == '__main__':
    main()