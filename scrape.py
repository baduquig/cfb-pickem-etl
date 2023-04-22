import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date


class CollegeFootballSchedule:
    """ This class contains all the methods needed to scrape and load college schedule data from ESPN into CFB_SCHEDULE_GB dataabase. """
    def __init__(self, year=2023):
        self.weeks = 14
        self.year = year
        self.games_df = pd.DataFrame(columns=['game_date', 'away_school', 'home_school', 'game_id', 'time', 'location'])
    
    def get_school_id(self, td_html_str):
        """Method to extract TeamID from the URL in the underlying href attribute."""
        try:
            school_span = td_html_str.find('span', class_='Table__Team')
            href_str = school_span.find('a', href=True)['href']

            begin_idx = href_str.index('/id/') + 4
            end_idx = href_str.rfind('/')
            school_id = href_str[begin_idx:end_idx]
        except:
            school_id = 0
        return school_id
    
    def get_game_id(self, td_html_str):
        """Method to extract GameID from the URL in the underlying href attribute."""
        href_str = td_html_str.find('a', href=True)['href']        
        idx = href_str.index('gameId=') + 7
        game_id = href_str[idx:]
        return game_id
    
    def get_cell_text(self, td_html_str):
        """Method to extract the innerHTML text of the child tag of a given table cell."""
        try:
            cell_text = td_html_str.contents[0].text
        except:
            cell_text = 'TBD'
        return cell_text


    def scrape_games(self):
        """ Method to scrape college football schedule data from https://www.espn.com/college-football/schedule for a given year (default: 2023). """
        print('\nBeginning scraping games data.\n')

        # Iterate through each week of 15 week schedule
        for week in range(self.weeks):
            week_num = week + 1
            espn_url = 'https://www.espn.com/college-football/schedule/_/week/' + str(week_num) + '/year/' + str(self.year) + '/'
            
            print(f'  ~ Scraping data for Week {week_num}...')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
            sched_container = soup.find_all('div', class_='mt3')[1]
            
            # Iterate through each distinct day with games on this particular week
            for day in sched_container.children:
                date_str = day.find('div', class_='Table__Title').text
                game_date = datetime.strptime(date_str, "%A, %B %d, %Y")
                games_table = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')

                # Iterate through each game/row in table
                for game_row in games_table:
                    game = game_row.find_all('td')

                    # Instantiate game data elements
                    away_school = self.get_school_id(game[0])
                    home_school = self.get_school_id(game[1])
                    game_id = self.get_game_id(game[2])
                    location = self.get_cell_text(game[5])
                    if game_date > date.today():
                        time = self.get_cell_text(game[2])
                    else:
                        final_score = self.get_cell_text(game[2])

                    # Assign new DataFrame row
                    new_game = pd.DataFrame({
                        'game_date' : [game_date], 'away_school' : [away_school],
                        'home_school' : [home_school], 'game_id' : [game_id],
                        'time' : [time], final_score: [final_score], 'location' : [location]
                    })                    
                    self.games_df = pd.concat([self.games_df, new_game], ignore_index=True)        
        print('Completed scraping games data.')
    
    
    def scrape_schools(self, schools_list):
        """ Method to scrape school data from https://www.espn.com/college-football/team/_/id/000/ for a given school."""
        print('\nBeginning scraping schools data.')
        
        # Iterate through distinct schools in list of away schools
        for school in schools_list:
            espn_url = 'https://www.espn.com/college-football/team/_/id/' + school
            print(f'  ~ Scraping data for School ID: {school}...')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')

        print('Completed scraping schools data.')
                    

cfb_sched = CollegeFootballSchedule(2023)
cfb_sched.scrape_games()
print(cfb_sched.games_df)