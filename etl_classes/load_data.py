import pandas as pd
from datetime import datetime

class LoadData:
    """This class contains methods needed to load data into all 'potential' consumable forms."""
    def __init__(self, games_df=pd.DataFrame(), schools_df=pd.DataFrame(), conferences_df=pd.DataFrame()):
        self.logfile = open('./logs/load_all_' + datetime.now().strftime('%Y.%m.%d.%H.%M.%S') + '.log', 'a')
        self.games_df = games_df
        self.schools_df = schools_df
        self.conferences_df = conferences_df
    
    def load_csv(self):
        """This method to load schedule data stored in Pandas DataFrames into CSV files."""
        if not self.games_df.empty:
            self.games_df.to_csv('./data/games.csv', index=False)

        if not self.schools_df.empty:
            self.schools_df.to_csv('./data/schools.csv', index=False)

        if not self.conferences_df.empty:
            self.conferences_df.to_csv('./data/conferences.csv', index=False)
    
    def load_json(self):
        """This method to load schedule data stored in Pandas DataFrames into JSON files."""
        if not self.games_df.empty:
            self.games_df.to_json('./data/games.json', orient='records')

        if not self.schools_df.empty:
            self.schools_df.to_json('./data/schools.json', orient='records')

        if not self.conferences_df.empty:
            self.conferences_df.to_json('./data/conferences.json', orient='records')