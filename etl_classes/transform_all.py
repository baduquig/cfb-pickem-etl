import pandas as pd
from datetime import datetime

class TransformAll:
    """This class contains methods needed to transform, manipulate and consolidate data from various sources."""
    def __init__(self, games_df, schools_df, conferences_df, locations_df):
        self.logfile = open('./logs/transform_all_' + datetime.now().strftime('%Y.%m.%d.%H.%M.%S') + '.log', 'a')
        self.games_df = games_df
        self.schools_df = schools_df
        self.conferences_df = conferences_df
        self.locations_df = locations_df

    def transform_games_data(self):
        print('Preparing Games Data...')
        self.logfile.write('Preparing Games Data...\n')

        self.games_df = pd.merge(self.games_df, self.locations_df[['locationID', 'locationName']], left_on='location', right_on='locationName')
        self.games_df = self.games_df.drop(['location', 'locationName'], axis=1)
        return self.games_df

    def transform_schools_data(self):
        print('Preparing Schools Data...')
        self.logfile.write('Preparing Schools Data...\n')
        self.schools_df = pd.merge(self.schools_df, self.conferences_df, on='schoolID', how='left')
        self.schools_df = self.schools_df.drop(['conferenceID', 'conferenceName', 'divisionName'], axis=1)
        self.schools_df = self.schools_df.drop_duplicates()
        return self.schools_df

    def transform_conferences_data(self):
        print('Preparing Conferences Data...')
        self.logfile.write('Preparing Conferences Data...\n')
        self.conferences_df = self.conferences_df.drop(['schoolID'], axis=1).drop_duplicates()
        return self.conferences_df

    def consolidate_data(self):
        print('Preparing Consolidated Data...')
        self.logfile.write('Preparing Consolidated Data...\n')

        # Join Away School Data to consolidated DataFrame
        # TODO: Investigate why records are increasing from 858 to 922 record
        all_data = pd.merge(self.games_df, self.schools_df[['schoolID', 'name', 'mascot', 'divisionID']], left_on='awaySchool', right_on='schoolID', how='left')
        all_data = all_data.rename(columns={'name': 'awaySchoolName', 'mascot': 'awaySchoolMascot', 'divisionID': 'awayDivisionID'})
        all_data = all_data.drop(['schoolID'], axis=1)
        
        # Join Home School Data to consolidated DataFrame
        all_data = pd.merge(all_data, self.schools_df[['schoolID', 'name', 'mascot', 'divisionID']], left_on='homeSchool', right_on='schoolID', how='left')
        all_data = all_data.rename(columns={'name': 'homeSchoolName', 'mascot': 'homeSchoolMascot', 'divisionID': 'homeDivisionID'})
        all_data = all_data.drop(['schoolID'], axis=1)

        # Join Away School Conference Data to consolidated DataFrame
        all_data = pd.merge(all_data, self.conferences_df[['divisionID', 'conferenceID', 'conferenceName', 'divisionName']], left_on='awayDivisionID', right_on='divisionID', how='left')
        all_data = all_data.rename(columns={'conferenceID': 'awayConferenceID', 'conferenceName': 'awayConferenceName', 'divisionName': 'awayDivisionName'})
        all_data = all_data.drop(['divisionID'], axis=1)

        # Join Home Shool Conference Data to consolidated DataFrame
        all_data = pd.merge(all_data, self.conferences_df[['divisionID', 'conferenceID', 'conferenceName', 'divisionName']], left_on='homeDivisionID', right_on='divisionID', how='left')
        all_data = all_data.rename(columns={'conferenceID': 'homeConferenceID', 'conferenceName': 'homeConferenceName', 'divisionName': 'homeDivisionName'})
        all_data = all_data.drop(['divisionID'], axis=1)

        # Join Location Data to consolidated DataFrame
        all_data = pd.merge(all_data, self.locations_df, on='locationID', how='left')

        return all_data