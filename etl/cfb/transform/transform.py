"""
CFB Pickem ETL
Author: Gabe Baduqui

Retrieve location data from Geocode.maps forward geocode API
"""

def full_transform(games_df, schools_df, locations_df, logfile='./logs/cfb_etl.log'):
    """Function that applies necessary transformations to format data to desired model
       Accepts `games_df`: Pandas DataFrame, `schools_df`: Pandas DataFrame, `locations_df`: Pandas DataFrame
       Returns ``: """