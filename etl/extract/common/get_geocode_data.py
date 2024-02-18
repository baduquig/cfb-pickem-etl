"""
Pickem ETL
Author: Gabe Baduqui

Retrieve location data from Geocode.maps forward geocode API
"""
import requests, time
import etl.utils.credentials as cred

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
        state = state.rstrip()
    except:
        state = None
    return state

def get_latitude(geocode_record: dict, logfile: object):
    """Function that extracts latitude property from the Geocode API response
       Accepts `geocode_record`: Dictionary (JSON response), `logfile`: File Object
       Returns `latitude`: Number"""
    try:
        lat = geocode_record['lat']
        logfile.write(f'lat: {lat}\n')
    except Exception as e:
        lat = None
        logfile.write(f'{e}\n')
    return lat

def get_longitude(geocode_record: dict, logfile: object):
    """Function that extracts longitude property from the Geocode API response
       Accepts `geocode_record`: Dictionary (JSON response), `logfile`: File Object
       Returns `longitude`: Number"""
    try:
        lon = geocode_record['lon']
        logfile.write(f'lon: {lon}\n')
    except Exception as e:
        lon = None
        logfile.write(f'{e}\n')
    return lon

def call_geocode_api(stadium: str, city: str, state: str, logfile: object):
    """Fucntion that makes a GET request to 'https://geocode.maps.co/search?q=' for a given location
       Accepts `stadium`: String, `cit`: String, `state`: String, `logfile`: File Object
       Returns `geocode_record`: Dictionary"""
    if city is not None:
        city = city.strip().replace(' ', '+')
    if state is not None:
        state = state.strip()
    if stadium is not None:
        general_query = stadium.replace(' ', '+')

    logfile.write('Geocode URL: ')
    if stadium is None:
        geocode_api_url = f'https://geocode.maps.co/search?city={city}&state={state}&api_key={cred.geo_api_key}'
    elif state is None:
        geocode_api_url = f'https://geocode.maps.co/search?q={general_query}&api_key={cred.geo_api_key}'
    else:
        geocode_api_url = f'https://geocode.maps.co/search?q={general_query}&city={city}&state={state}&api_key={cred.geo_api_key}'
    logfile.write(f'{geocode_api_url}\n')

    logfile.write('Geocode API Response: ')
    try:
        response = requests.get(geocode_api_url)
        while response.status_code == '429':
            time.sleep(2)
            response = requests.get(geocode_api_url)
        geocode_record = response.json()[0]
        logfile.write(f'{geocode_record}\n')
    except Exception as e:
        logfile.write(f'response: {requests.get(geocode_api_url).status_code} | ')
        geocode_record = None
        logfile.write(f'{e}\n')

    return geocode_record

def get_location_data(league: str, location_id: str, stadium: str, stadium_capacity: str, location_name: str, logfile: object):
    """Function that calls the Geocode.maps forward geocode API.
       Accepts `location_id`: String, `stadium`: String, `location_name`: String, `logfile`: File Object
       Returns `location_data`: Dictionary"""    
    print(f'~~ Scraping geocode data for {stadium}, {location_name}')
    logfile.write(f'\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nScraping geocode data for {stadium}, {location_name}\n')

    # Call Forward Geocode API
    city = get_city_name(location_name)
    state = get_state_name(location_name)
    geocode_record = call_geocode_api(stadium, city, state, logfile)
    lat = get_latitude(geocode_record, logfile)
    lon = get_longitude(geocode_record, logfile)
    
    # Instantiate `location_data` dictionary
    location_data = {
        'league': league,
        'location_id': location_id,
        'stadium': stadium,
        'stadium_capacity': stadium_capacity,
        'city': city,
        'state': state,
        'latitude': lat,
        'longitude': lon
    }
    logfile.write('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

    return location_data