import pandas as pd
import requests
import time
from etl_classes.extract_all import ExtractAll

class GetGameLocations(ExtractAll):
    """This class contains the methods needed to retrieve geocoordinate data from the `geocode.maps.io` API."""
    def __init__(self):
        super().__init__()

    def get_game_locations(self, locations):
        self.cfb_etl_log('\nBeginning retrieving geocode data.')

        locations_df = pd.DataFrame(columns=['locationID', 'locationName', 'city', 'state', 'latitude', 'longitude'])
        forward_geocode_url = 'https://geocode.maps.co/search?'
        location_id = 1

        for location in locations:
            if location != '':
                self.cfb_etl_log(f'  ~ Retrieving data for location: {location}')

                city = location.split(',')[-2].strip()
                state = location.split(',')[-1].strip()

                if city == 'USAF Academy':
                    query_str = 'q=Falcon Stadium'
                elif city == 'West Point':
                    query_str = 'q=Michie Stadium'
                else:
                    query_str = f'city={city}&state={state}&country=US'
                
                try:
                    response = requests.get(forward_geocode_url + query_str)
                    
                    location_data = response.json()[0]
                    
                    latitude = location_data['lat']
                    longitude = location_data['lon']

                    new_location = pd.DataFrame({
                        'locationID': [location_id], 'locationName': [location], 'city': [city], 
                        'state': [state], 'latitude': [latitude], 'longitude': [longitude]
                    })
                    locations_df = pd.concat([locations_df, new_location], ignore_index=True)

                    location_id += 1
                except:
                    self.cfb_etl_log(f'\n!!! Data not retrieved for location: {location}\n')
                
                time.sleep(.75)

        self.cfb_etl_log('Completed retrieving geocode data.\n')     
        return locations_df
