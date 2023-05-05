import pandas as pd
import requests
from etl_classes.scrape_all import ScrapeAll

class GetGameLocations(ScrapeAll):
    """This class contains the methods needed to retrieve geocoordinate data from the `geocode.maps.io` API."""
    def __init__(self):
        super().__init__()

    def get_game_locations(self, locations):
        print('\nBeginning retrieving geocode data.')
        self.logfile.write('\nBeginning retrieving geocode data.')

        locations_df = pd.DataFrame(columns=['location_id', 'location_name', 'city', 'state', 'latitude', 'longitude'])
        forward_geocode_url = 'https://geocode.maps.co/search?'
        location_id = 1

        for location in locations:
            if location != '':
                print(f'  ~ Retrieving data for location: {location}...')
                self.logfile.write(f'  ~ Retrieving data for location: {location}...')

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
                        'location_id': [location_id], 'location_name': [location], 'city': [city], 
                        'state': [state], 'latitude': [latitude], 'longitude': [longitude]
                    })
                    locations_df = pd.concat([locations_df, new_location], ignore_index=True)

                    location_id += 1
                except:
                    print(f'\n!!! Data not retrieved for location: {location}...\n')
                    self.logfile.write(f'\n!!! Data not retrieved for location: {location}...\n')
                    pass

        print('\nCompleted retrieving geocode data.')
        self.logfile.write('\nCompleted retrieving geocode data.')        
        return locations_df
