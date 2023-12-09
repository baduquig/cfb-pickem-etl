"""
Pickem ETL
Author: Gabe Baduqui

Retrieve location data from Geocode.maps forward geocode API
"""
import requests

def get_city_name(location_name: str):
    """Function that extracts city name from a given location string
       Accepts `location_name`: String
       Returns `city`: String"""
    try:
        city = location_name.split(', ')[0]
    except:
        city = None
    return city

def get_state_name(location_name: dict):
    """Function that extracts state name from a given location string
       Accepts `location_name`: String
       Returns `state`: String"""
    try:
        state = location_name.split(', ')[1]
    except:
        state = None
    return state

def get_latitude(geocode_record: dict):
    """Function that extracts latitude property from the Geocode API response
       Accepts `geocode_record`: Dictionary (JSON response)
       Returns `latitude`: Number"""
    try:
        lat = geocode_record['lat']
    except:
        lat = None
    return lat

def get_longitude(geocode_record: dict):
    """Function that extracts longitude property from the Geocode API response
       Accepts `geocode_record`: Dictionary (JSON response)
       Returns `longitude`: Number"""
    try:
        long = geocode_record['lat']
    except:
        long = None
    return long

def call_geocode_api(stadium: str, city: str, state: str):
    """Fucntion that makes a GET request to 'https://geocode.maps.co/search?q=' for a given location
       Accepts `stadium`: String, `cit`: String, `state`: String
       Returns `geocode_record`: Dictionary"""
    if stadium is None:
        geocode_api_url = f'https://geocode.maps.co/search?city={city}&state={state}'
    else:
        general_query = stadium.replace(' ', '+')
        if state is None:
            geocode_api_url = f'https://geocode.maps.co/search?q={general_query}'
        else:
            geocode_api_url = f'https://geocode.maps.co/search?q={general_query}&city={city}&state={state}'
    try:
        response = requests.get(geocode_api_url)
        geocode_record = response.json()[0]
    except:
        geocode_record = None
    return geocode_record

def get_location_data(location_id: str, stadium: str, location_name: str, logfile: object):
    """Function that calls the Geocode.maps forward geocode API.
       Accepts `location_id`: String, `stadium`: String, `location_name`: String, `logfile`: File Object
       Returns `location_data`: Dictionary"""    
    print(f'~~ Scraping geocode data for {stadium}, {location_name}')
    logfile.write(f'~~ Scraping geocode data for {stadium}, {location_name}\n')

    # Call Forward Geocode API
    city = get_city_name(location_name)
    state = get_state_name(location_name)
    geocode_record = call_geocode_api(stadium, city, state)
    lat = get_latitude(geocode_record)
    lon = get_longitude(geocode_record)
    
    # Instantiate `location_data` dictionary
    location_data = {
        'location_id': location_id,
        'stadium': stadium,
        'city': city,
        'state': state,
        'latitude': lat,
        'longitude': lon
    }

    return location_data