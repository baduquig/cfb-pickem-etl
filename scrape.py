import requests
from bs4 import BeautifulSoup


class CollegeFootballSchedule:
    """ This class contains all the methods needed to scrape and load college schedule data from ESPN into CFB_SCHEDULE_GB dataabase. """
    def __init__(self):
        self.weeks = 14
    
    def get_school_id(self, td_html_str):
        """Method to extract TeamID from the URL in the underlying href attribute."""
        print(td_html_str)
        school_span = td_html_str.find('span', class_='Table__Team')
        print(school_span)
        href_str = school_span.find('a', href=True)['href']

        begin_idx = href_str.index('/id/') + 4
        end_idx = href_str.rfind('/')
        school_id = href_str[begin_idx:end_idx]
        return school_id
    
    def get_game_id(self, td_html_str):
        """Method to extract GameID from the URL in the underlying href attribute."""
        href_str = td_html_str.find('a', href=True)['href']        
        idx = href_str.index('gameId=') + 7
        game_id = href_str[idx:]
        return game_id
    
    def get_cell_text(self, td_html_str):
        """Method to extract the innerHTML text of the child tag of a given table cell."""
        cell_text = td_html_str.contents[0].text
        return cell_text

    def scrape_full_schedule(self, year=2023):
        """ Method to scrape all college football schedule data from https://www.espn.com/college-football/schedule for a given year (default: 2023). """
        
        # Iterate through each week of 15 week schedule
        for week in range(self.weeks):
            week_num = week + 1
            espn_url = 'https://www.espn.com/college-football/schedule/_/week/' + str(week_num) + '/year/' + str(year) + '/'
            
            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
            sched_container = soup.find_all('div', class_='mt3')[1]
            
            # Iterate through each distinct day with games on this particular week
            for day in sched_container.children:
                game_date = day.find('div', class_='Table__Title').text
                games_table = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')

                for game in games_table:
                    game = game.find_all('td')

                    away_school = self.get_school_id(game[0])
                    home_school = self.get_school_id(game[1])
                    game_id = self.get_game_id(game[2])
                    #time = self.get_time_attr(game[2])
                    #final_score = self.get_cell_text(game[2])
                    time = self.get_cell_text(game[2])
                    location = self.get_cell_text(game[5])

                    print(f'Date: {game_date} | Away School: {away_school} | Home School: {home_school} | Game ID: {game_id} | Time: {time} | Location: {location}')
        print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')
                    

cfb_sched = CollegeFootballSchedule()
cfb_sched.scrape_full_schedule(2023)