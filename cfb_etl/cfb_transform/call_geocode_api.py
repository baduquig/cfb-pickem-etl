"""
CFB Pickem ETL
Author: Gabe Baduqui

Retrieve location data from Geocode.maps forward geocode API
"""
import requests

def get_city_name(location_name):
    """Function that extracts city name from a given location string
       Accepts `location_name`: String
       Returns `city`: String"""
    try:
        city = location_name.split(', ')[0]
    except:
        city = None
    return city

def get_state_name(location_name):
    """Function that extracts state name from a given location string
       Accepts `location_name`: String
       Returns `state`: String"""
    try:
        state = location_name.split(', ')[1]
    except:
        state = None
    return state

def call_forward_geocode_api(stadium, city, state):
    """Fucntion that makes a GET request to 'https://geocode.maps.co/search?q=' for a given location
       Accepts `query_str`: String
       Returns `geocode_record`: Dictionary"""
    general_query = stadium.replace(' ', '+')
    
    if (state is None) and (city is None):
        geocode_api_url = f'https://geocode.maps.co/search?q={general_query}'
    elif state is None:
        geocode_api_url = f'https://geocode.maps.co/search?q={general_query}&city={city}'
    else:
        geocode_api_url = f'https://geocode.maps.co/search?q={general_query}&city={city}&state={state}'

    try:
        response = requests.get(geocode_api_url)
        geocode_record = response.json()[0]
    except:
        geocode_record = None
    return geocode_record

def get_location_data(location_id, stadium, location_name, logfile):
    """Function that calls the Geocode.maps forward geocode API.
       Accepts `location_names`: List of Strings
       Returns location_data: Dictionary"""    
    print(f'~~ Scraping geocode data for {location_name}')
    logfile.write(f'~~ Scraping geocode data for {location_name}\n')

    # Call Forward Geocode API
    city = get_city_name(location_name)
    state = get_state_name(location_name)
    geocode_record = call_forward_geocode_api(stadium, city, state)
    
    # Instantiate `location_data` dictionary
    location_data = {
        'location_id': location_id,
        'stadium': stadium,
        'city': city,
        'state': state,
        'latitude': geocode_record['lat'],
        'longitude': geocode_record['lon']
    }

    return location_data