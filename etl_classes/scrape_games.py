import pandas as pd
import requests
import sys
from bs4 import BeautifulSoup
from datetime import datetime, date
from etl_classes.extract_all import ExtractAll

class ScrapeGames(ExtractAll):
    """This class contains the methods needed to scrape college football game 
    schedule data from ESPN at (https://www.espn.com/college-football/schedule)."""
    def __init__(self):
        super().__init__()
    
    def scrape_score(self, game_id):
        """Method to scrape score from https://www.espn.com/college-football/game?gameId=xxxxxxxxx for a given Game ID"""
        self.cfb_etl_log(f'    ~ Game time passed, getting score for Game {game_id}')
        game_url = 'https://www.espn.com/college-football/game?gameId=' + str(game_id)
        
        # Scrape HTML from HTTP request to the URL for the provided Game ID
        game_resp = requests.get(game_url)
        game_soup = BeautifulSoup(game_resp.content, 'html.parser')

        # Instantiate variable for parent/child Gamestrip DIVs
        score_container = game_soup.find('div', class_='Gamestrip__Competitors')
        away_score = score_container.find('div', class_='Gamestrip__Team--away').find('div', class_='Gamestrip__Score').text
        home_score = score_container.find('div', class_='Gamestrip__Team--home').find('div', class_='Gamestrip__Score').text
        
        return away_score, home_score

    def scrape_games(self, year=2023):
        """Method to scrape college football schedule data from https://www.espn.com/college-football/schedule for a given year (default: 2023)."""
        self.cfb_etl_log('\nBeginning scraping games data.\n')

        weeks = 14    
        games_df = pd.DataFrame(columns=['week', 'gameDate', 'awaySchool', 'homeSchool', 'gameID', 'time', 'location'])

        # Iterate through each week of 15 week schedule
        for week in range(weeks):
            week_num = int(week + 1)
            espn_url = 'https://www.espn.com/college-football/schedule/_/week/' + str(week_num) + '/year/' + str(year)
            
            self.cfb_etl_log(f'  ~ Scraping data for Week {week_num}')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
            schedule_div = soup.find('div', class_='mt3')
            if schedule_div is None:
                print(soup)
                sys.exit()
            
            # Iterate through each distinct day with games on this particular week
            for day in schedule_div.children:
                date_str = day.find('div', class_='Table__Title').text.strip()
                game_date = datetime.strptime(date_str, "%A, %B %d, %Y").date()
                games_table = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')

                # Iterate through each game/row in table
                for game_row in games_table:
                    game_columns = game_row.find_all('td')

                    away_school_span = game_columns[0].find('span', class_='Table__Team')
                    home_school_span = game_columns[1].find('span', class_='Table__Team')

                    # Instantiate game data elements
                    away_school = self.get_school_id(away_school_span)
                    home_school = self.get_school_id(home_school_span)
                    game_id = self.get_game_id(game_columns[2])
                    
                    if game_date >= date.today():
                        time = self.get_cell_text(game_columns[2])
                        away_score = 0
                        home_score = 0
                    else:
                        time = 'TBD'
                        away_score, home_score = self.scrape_score(game_id)

                    location = self.get_cell_text(game_columns[5])

                    # Assign new DataFrame row
                    new_game = pd.DataFrame({
                        'week': [week_num],  'gameDate': [date_str], 
                        'awaySchool': [away_school], 'homeSchool': [home_school], 
                        'gameID': [game_id], 'time': [time], 
                        'awayScore': [away_score], 'homeScore': [home_score], 
                        'location': [location]
                    })                    
                    games_df = pd.concat([games_df, new_game], ignore_index=True)
                    
        self.cfb_etl_log('Completed scraping games data.\n')
        return games_df
