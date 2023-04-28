import pandas as pd
import pyodbc
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date

# TODO:
# - Create CFB_APPS database
# - Create methods for incremental etl runs
# - Create method to update schedule data into the CFB_APPS database for incremental runs
# - Create log file for ETL job runs


class CollegeFootballSchedule:
    """This class contains all the methods needed to scrape, transform and 
    load college schedule data from ESPN into CFB_SCHEDULE_GB dataabase."""
    def __init__(self, year=2023, db_driver='{ODBC Driver 17 for SQL Server}', db_server='GABE_PC\SQLEXPRESS', db_name='cfb_schedule'):
        self.db_driver = db_driver
        self.db_server = db_server
        self.db_name = db_name

        self.weeks = 14
        self.year = year
        
        self.games_df = pd.DataFrame(columns=['game_date', 'away_school', 'home_school', 'game_id', 'time', 'location'])
        self.schools_df = pd.DataFrame(columns=['school_id', 'logo_url', 'name', 'mascot', 'record', 'wins', 'losses', 'ties'])
        self.school_confs_df = pd.DataFrame(columns=['school_id', 'division_id'])
        self.conferences_df = pd.DataFrame(columns=['division_id', 'conference_id', 'division_name', 'conference_name'])

    
    def connect_to_db(self):
        """Method to initialize connection to the SQL Server database with PYODBC and return the cursor object."""
        driver = self.db_driver
        server = self.db_server
        database = self.db_name

        connection_string = f'Driver={driver};Server={server};Database={database};Trusted_Connection=yes'
        conn = pyodbc.connect(connection_string)
        # cursor = conn.cursor()
        return conn
    
    def get_school_id(self, school_span):
        """Method to extract TeamID from the URL in the underlying href attribute."""
        try:
            href_str = school_span.find('a', href=True)['href']
            begin_idx = href_str.index('/id/') + 4
            end_idx = href_str.rfind('/')
            school_id = href_str[begin_idx:end_idx]
        except:
            school_id = '0'
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
    
    def get_href_attr(self, school_id):
        """Method to extract string value of HREF attribute of a given school (ID)"""
        try:
            logo_url = f'https://a.espncdn.com/combiner/i?img=/i/teamlogos/ncaa/500/{school_id}.png&h=2000&w=2000'
        except:
            logo_url = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/ncaa/500/4.png&h=2000&w=2000'
        return logo_url


    def scrape_games(self):
        """ Method to scrape college football schedule data from https://www.espn.com/college-football/schedule for a given year (default: 2023). """
        print('\nBeginning scraping games data.')

        # Iterate through each week of 15 week schedule
        for week in range(self.weeks):
            week_num = week + 1
            espn_url = 'https://www.espn.com/college-football/schedule/_/week/' + str(week_num) + '/year/' + str(self.year)
            
            print(f'  ~ Scraping data for Week {week_num}...')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Instantiate variable for 'parent' schedule DIV and for each distinct day with games in this particular week
            sched_container = soup.find_all('div', class_='mt3')[1]
            
            # Iterate through each distinct day with games on this particular week
            for day in sched_container.children:
                date_str = day.find('div', class_='Table__Title').text.strip()
                game_date = datetime.strptime(date_str, "%A, %B %d, %Y").date()
                games_table = day.find('div', class_='Table__Scroller').find('table', class_='Table').find('tbody', class_='Table__TBODY').find_all('tr')

                # Iterate through each game/row in table
                for game_row in games_table:
                    game = game_row.find_all('td')

                    away_school_span = game[0].find('span', class_='Table__Team')
                    home_school_span = game[1].find('span', class_='Table__Team')

                    # Instantiate game data elements
                    away_school = self.get_school_id(away_school_span)
                    home_school = self.get_school_id(home_school_span)
                    game_id = self.get_game_id(game[2])
                    location = self.get_cell_text(game[5])
                    if game_date >= date.today():
                        time = self.get_cell_text(game[2])
                        final_score = '0-0'
                    else:
                        time = 'TBD'
                        final_score = self.get_cell_text(game[2])

                    # Assign new DataFrame row
                    new_game = pd.DataFrame({
                        'game_date': [game_date], 'away_school': [away_school],
                        'home_school': [home_school], 'game_id': [game_id],
                        'time': [time], 'final_score': [final_score], 'location': [location]
                    })                    
                    self.games_df = pd.concat([self.games_df, new_game], ignore_index=True)        
        print('Completed scraping games data.\n')
    
    
    def scrape_schools(self, schools_list):
        """ Method to scrape school data from https://www.espn.com/college-football/team/_/id/000/ for a given list of schools."""
        print('\nBeginning scraping schools data.')
        
        # Iterate through distinct schools in list of away schools
        for school in schools_list:
            espn_url = 'https://www.espn.com/college-football/team/_/id/' + str(school) + '/'
            print(f'  ~ Scraping data for School ID: {school}...')

            # Scrape HTML from HTTP request to the URL above and store in variable `soup`
            response = requests.get(espn_url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Instantiate variable for Parent DIV of main school ("clubhouse") banner
            school_banner = soup.find('h1', class_='ClubhouseHeader__Name')
            name_span, mascot_span = school_banner.find_all('span', class_='db')
            # TODO: Scrape record as this is available in the HTML
            # record_li = school_banner.find('ul', class_='ClubhouseHeader__Record').find_all('li')[0]

            # Instantiate school data elements
            logo = self.get_href_attr(school)
            name = self.get_cell_text(name_span)
            mascot = self.get_cell_text(mascot_span)
            #record = self.get_cell_text(record_li)
            record = '0-0'
            try:
                wins, losses, ties = record.split('-')
            except:
                wins, losses = record.split('-')
                ties = '0'
            wins = wins.strip()
            losses = losses.strip()

            # Assign new DataFrame row
            new_school = pd.DataFrame({
                'school_id': [school], 'logo_url': [logo], 'name': [name], 'mascot': [mascot], 
                'record': [record], 'wins': [wins], 'losses': [losses], 'ties': [ties]
            })
            self.schools_df = pd.concat([self.schools_df, new_school], ignore_index=True)
        print('Completed scraping schools data.\n')
    

    def scrape_conferences(self):
        """Method to scrape conference/division data from https://www.espn.com/college-football/standings for a given list of schools."""
        print('\nBeginning scraping conference data.')

        espn_url = 'https://www.espn.com/college-football/standings'

        # Scrape HTML from HTTP request to the URL above and store in variable `soup`
        response = requests.get(espn_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Instantiate parent element variables
        conf_container = soup.find('div', class_='tabs__content').find('section')
        conference_divs = conf_container.find_all('div', class_='standings__table InnerLayout__child--dividers')

        conference_id = 1
        division_id = 10

        # Iterate through each overarching conference DIV
        for conf_div in conference_divs:
            print(f'  ~ Scraping data for Conference ID: {conference_id}...')

            title_div = conf_div.find('div', class_='Table__Title')
            table_div = conf_div.find('div', class_='flex').find('table')

            # Instantiate conference field for all teams in this div
            conference_name = self.get_cell_text(title_div)
            division_name = ''
            conf_tbody = table_div.find('tbody', class_='Table__TBODY')
            conf_trows = conf_tbody.find_all('tr')

            # Iterate through divisions/teams in Table Body
            for row in conf_trows:
                subgroup_header_class = 'subgroup-headers'
                tr_class = row.get('class')

                if subgroup_header_class in tr_class:
                    division_span = row.find('td', class_='Table__TD').find('span')
                    division_name = self.get_cell_text(division_span)
                    division_id += 1
                else:
                    school_span = row.find('td').find('div').find_all('span')[2]
                    school_id = self.get_school_id(school_span)

                # Assign new DataFrame rows
                new_school_conf = pd.DataFrame({
                    'school_id': [school_id], 'division_id': [division_id], 'conference_id': [conference_id],
                    'division_name': [division_name], 'conference_name': [conference_name]
                })
                self.conferences_df = pd.concat([self.conferences_df, new_school_conf], ignore_index=True)

            conference_id += 1
        print('Completed scraping conference data.\n')
        

    def full_extract(self):
        """Primary data extraction method of CollegeFootballSchedule module which calls 
        all other web scraping methods when initially loading the CFB_APPS database."""
        self.scrape_games()
        non_0_schools = self.games_df[self.games_df['home_school'] != '0']
        unique_schools = non_0_schools['home_school'].unique()
        self.scrape_schools(unique_schools)
        self.scrape_conferences()
    
    def full_transform(self):
        """Primary method of CollegeFootballSchedule module which performs necessary 
        data transformations when initially loading the CFB_APPS database."""
        # Create intersection dataframe/database table between schools and conference divisions
        self.school_confs_df = self.conferences_df
        self.school_confs_df = self.school_confs_df.drop(['conference_id', 'division_name', 'conference_name'], axis=1)
        
        # Normalize conference divisions dataframe 
        self.conferences_df = self.conferences_df.drop(['school_id'], axis=1)
        self.conferences_df = self.conferences_df.drop_duplicates()
    
    def full_load(self):
        """Primary method of CollegeFootballSchedule module which performs 
        SQL INSERTs for initial load of CFB_APPS database data."""
        # Instantiate database connection
        db_connection = self.connect_to_db()
        
        # Load the contents of each dataframe into their corresponding database table
        self.games_df.to_sql('CFB_GAMES', db_connection, if_exists='replace', index=False)
        self.schools_df.to_sql('CFB_GAMES', db_connection, if_exists='replace', index=False)
        self.school_confs_df.to_sql('CFB_GAMES', db_connection, if_exists='replace', index=False)
        self.conferences_df.to_sql('CFB_GAMES', db_connection, if_exists='replace', index=False)

    
    def full_etl(self):
        self.full_extract()
        self.full_transform()
        self.full_load()
                    

cfb_sched = CollegeFootballSchedule(2023)
cfb_sched.full_etl()