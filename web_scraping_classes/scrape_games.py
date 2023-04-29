import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from scrape_all import ScrapeAll

class ScrapeGames:
    """This class contains the methods needed to scrape college football game 
    schedule data from ESPN at (https://www.espn.com/college-football/schedule)."""
    def __init__(self):
        self.weeks = 14        
        self.games_df = pd.DataFrame(columns=['game_date', 'away_school', 'home_school', 'game_id', 'time', 'location'])
    
    def get_game_id(self, td_tag_str):
        """Method to extract GameID from the URL in the underlying href attribute."""
        href_str = td_tag_str.find('a', href=True)['href']        
        game_id_index = href_str.index('gameId=') + 7
        game_id = href_str[game_id_index:]
        return game_id

    def scrape_games(self, year=2023):
        """ Method to scrape college football schedule data from https://www.espn.com/college-football/schedule for a given year (default: 2023). """
        print('\nBeginning scraping games data.')

        # Iterate through each week of 15 week schedule
        for week in range(self.weeks):
            week_num = week + 1
            espn_url = 'https://www.espn.com/college-football/schedule/_/week/' + str(week_num) + '/year/' + str(year)
            
            print(f'  ~ Scraping data for Week {week_num}...')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
            schedule_div = soup.find_all('div', class_='mt3')[1]
            
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
                    away_school = ScrapeAll.get_school_id(away_school_span)
                    home_school = ScrapeAll.get_school_id(home_school_span)
                    game_id = self.get_game_id(game_columns[2])
                    
                    if game_date >= date.today():
                        time = ScrapeAll.get_cell_text(game_columns[2])
                        score = '0-0'
                    else:
                        time = 'TBD'
                        score = ScrapeAll.get_cell_text(game_columns[2])

                    location = ScrapeAll.get_cell_text(game_columns[5])

                    # Assign new DataFrame row
                    new_game = pd.DataFrame({
                        'game_date': [game_date], 'away_school': [away_school],
                        'home_school': [home_school], 'game_id': [game_id],
                        'time': [time], 'score': [score], 'location': [location]
                    })                    
                    self.games_df = pd.concat([self.games_df, new_game], ignore_index=True)
        print('Completed scraping games data.\n')
        return self.games_df
