import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

class LoadData:
    """This class contains methods needed to load data into all 'potential' consumable forms."""
    def __init__(self, games_df=pd.DataFrame(), schools_df=pd.DataFrame(), conferences_df=pd.DataFrame(), locations_df=pd.DataFrame(), all_data=pd.DataFrame()):
        self.logfile = open('./logs/load_all_' + datetime.now().strftime('%Y.%m.%d.%H.%M.%S') + '.log', 'a')
        self.games_df = games_df
        self.schools_df = schools_df
        self.conferences_df = conferences_df
        self.locations_df = locations_df
        self.all_data = all_data
    
    def load_csv(self):
        """This method to load schedule data stored in Pandas DataFrames into CSV files."""
        print('\nBeginning loading data in CSV files.')
        self.logfile.write('\nBeginning loading data in CSV files.\n')

        if not self.games_df.empty:
            print(f'  ~ Loading games data into CSV file...')
            self.logfile.write(f'  ~ Loading games data into CSV file...\n')
            self.games_df.to_csv('./data/games.csv', index=False)

        if not self.schools_df.empty:
            print(f'  ~ Loading schools data into CSV file...')
            self.logfile.write(f'  ~ Loading schools data into CSV file...\n')
            self.schools_df.to_csv('./data/schools.csv', index=False)

        if not self.conferences_df.empty:
            print(f'  ~ Loading conferences data into CSV file...')
            self.logfile.write(f'  ~ Loading conferences data into CSV file...\n')
            self.conferences_df.to_csv('./data/conferences.csv', index=False)
        
        if not self.locations_df.empty:
            print(f'  ~ Loading locations data into CSV file...')
            self.logfile.write(f'  ~ Loading locations data into CSV file...\n')
            self.locations_df.to_csv('./data/locations.csv', index=False)
        
        if not self.all_data.empty:
            print(f'  ~ Loading consolidated data into CSV file...')
            self.logfile.writelines(f'  ~ Loading consolidated data into CSV file...\n')
            self.all_data.to_csv('./data/allData.csv', index=False)
        
        self.logfile.write('Completed loading data into CSV files.\n\n')
        print('Completed loading data into CSV files.\n')
    
    def load_json(self):
        """This method to load schedule data stored in Pandas DataFrames into JSON files."""
        print('\nBeginning loading data in JSON files.')
        self.logfile.write('\nBeginning loading data in JSON files.\n')

        if not self.games_df.empty:
            print(f'  ~ Loading games data into JSON file...')
            self.logfile.write(f'  ~ Loading games data into JSON file...\n')
            self.games_df.to_json('./data/games.json', orient='records')

        if not self.schools_df.empty:
            print(f'  ~ Loading schools data into JSON file...')
            self.logfile.write(f'  ~ Loading schools data into JSON file...\n')
            self.schools_df.to_json('./data/schools.json', orient='records')

        if not self.conferences_df.empty:
            print(f'  ~ Loading conferences data into JSON file...')
            self.logfile.write(f'  ~ Loading conferences data into JSON file...\n')
            self.conferences_df.to_json('./data/conferences.json', orient='records')

        if not self.locations_df.empty:
            print(f'  ~ Loading locations data into JSON file...')
            self.logfile.write(f'  ~ Loading locations data into JSON file...\n')
            self.locations_df.to_json('./data/locations.json', orient='records')

        if not self.all_data.empty:
            print(f'  ~ Loading consolidated data into JSON file...')
            self.logfile.write(f'  ~ Loading consolidated data into JSON file...\n')
            self.all_data.to_json('./data/allData.json', orient='records')
        
        print('Completed loading data into JSON files.\n')
        self.logfile.write('Completed loading data into JSON files.\n\n')

    def load_mysql_db(self):
        """This method to load schedule data stored in Pandas DataFrames into FreeMySQLHosting MySQL database instance."""
        print('\nBeginning loading data into MySQL Database.')
        self.logfile.write('\nBeginning loading data into MySQL Database.\n')

        engine = create_engine('mysql+mysqlconnector://sql9634488:2usSRQH2hu@sql9.freemysqlhosting.net:3306/sql9634488')

        # Locations
        if not self.locations_df.empty:
            print(f'  ~ Loading locations data into MySQL Database...')
            self.logfile.write(f'  ~ Loading locations data into MySQL Database...\n')

            db_column_names = {
                'locationID': 'LOCATION_ID',
                'locationName': 'LOCATION_NAME',
                'city': 'CITY',
                'state': 'STATE_NAME',
                'latitude': 'LATITUDE',
                'longitude': 'LONGITUDE'
            }
            self.locations_df.rename(columns=db_column_names, inplace=True)
            self.locations_df.to_sql(name='CFB_LOCATIONS', con=engine, if_exists='append', index=False)
        
        # Conferences
        if not self.conferences_df.empty:
            print(f'  ~ Loading conferences data into MySQL Database...')
            self.logfile.write(f'  ~ Loading conferences data into MySQL Database...\n')

            db_column_names = {
                'conferenceID': 'CONFERENCE_ID',
                'divisionID': 'DIVISION_ID',
                'conferenceName': 'CONFERENCE_NAME',
                'divisionName': 'DIVISION_NAME'
            }
            self.conferences_df.rename(columns=db_column_names, inplace=True)
            self.conferences_df.to_sql(name='CFB_CONFERENCES', con=engine, if_exists='append', index=False)

        # Schools
        if not self.schools_df.empty:
            print(f'  ~ Loading schools data into MySQL Database...')
            self.logfile.write(f'  ~ Loading schools data into MySQL Database...\n')

            db_column_names = {
                'schoolID': 'SCHOOL_ID',
                'logoUrl': 'LOGO_URL',
                'name': 'SCHOOL_NAME',
                'mascot': 'MASCOT',
                'record': 'RECORD',
                'wins': 'WINS',
                'losses': 'LOSSES',
                'ties': 'TIES',
                'divisionID': 'DIVISION'
            }
            self.schools_df.rename(columns=db_column_names, inplace=True)
            self.schools_df.to_sql(name='CFB_SCHOOLS', con=engine, if_exists='append', index=False)

        # Games
        if not self.games_df.empty:
            print(f'  ~ Loading games data into MySQL Database...')
            self.logfile.write(f'  ~ Loading games data into MySQL Database...\n')

            db_column_names = {
                'week': 'GAME_WEEK',
                'gameDate': 'GAME_DATE',
                'awaySchool': 'AWAY_SCHOOL',
                'homeSchool': 'HOME_SCHOOL',
                'gameID': 'GAME_ID',
                'time': 'GAME_TIME',
                'awayScore': 'AWAY_SCORE',
                'homeScore': 'HOME_SCORE',
                'locationID': 'GAME_LOCATION'
            }
            self.games_df.rename(columns=db_column_names, inplace=True)
            self.games_df.to_sql(name='CFB_GAMES', con=engine, if_exists='append', index=False)

        
        print('Completed loading data into MySQL Database.\n')
        self.logfile.write('Completed loading data into MySQL Database.\n\n')
