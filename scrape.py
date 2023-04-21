import requests
from bs4 import BeautifulSoup


class CollegeFootballSchedule:
    """ This class contains all the methods needed to scrape and load college schedule data from ESPN into CFB_SCHEDULE_GB dataabase. """
    def __init__(self):
        self.weeks = 14

    def scrape(self, year=2023):
        """ Method to scrape all college football schedule data from https://www.espn.com/college-football/schedule for a given year (default: 2023). """
        
        # Iterate through each week of 15 week schedule
        for week in self.weeks:
            week_num = week + 1
            espn_url = f'https://www.espn.com/college-football/schedule/_/week/{week_num}/year/{year}/'
            
            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
            sched_container = soup.find('div', id='mt3')
            # TODO: day_containers list unique identifier
            day_container = sched_container.find_all()

            # Iterate through each distinct day with games on this particular week
            for day in day_container:
                # TODO: game_rows list unique identifier
                games_table = day_container.find_all()

                for game in games_table:
                    # TODO: Individual methods to extract text from table cells
                    away_school = self.get_school_id(game[0])
                    home_school = self.get_school_id(game[1])
                    game_id = self.get_game_id(game[2])
                    location = self.get_cell_text(game[3])
                    time = self.get_time_attr(game[4])
                    final_score = self.get_cell_text(game[5])
