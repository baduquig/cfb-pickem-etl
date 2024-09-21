"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare Games data
"""
from math import nan

def transform_box_score(box_score_raw: dict, transform_logfile: object):
    """Function that transforms box score data element into individuals fields
       Accepts `box_score_raw`: Dictionary, `transform_logfile`: Logfile Object
       Returns `quarter1`: Number, `quarter2`: Number, `quarter3`: Number, `quarter4`: Number, `total`: Number"""
    transform_logfile.write(f'Transforming box score {box_score_raw} -> ')
    try:
       if box_score_raw['1'] is None or box_score_raw['1'] is nan or box_score_raw['1'] == ' ':
          quarter1 = 0
       else:
          quarter1 = box_score_raw['1']
       
       if box_score_raw['2'] is None or box_score_raw['2'] is nan or box_score_raw['2'] == ' ':
          quarter2 = 0
       else:
          quarter2 = box_score_raw['2']
       
       if box_score_raw['3'] is None or box_score_raw['3'] is nan or box_score_raw['3'] == ' ':
          quarter3 = 0
       else:
          quarter3 = box_score_raw['3']
       
       if box_score_raw['4'] is None or box_score_raw['4'] is nan or box_score_raw['4'] == ' ':
          quarter4 = 0
       else:
          quarter4 = box_score_raw['4']

       if box_score_raw['overtime'] is None or box_score_raw['overtime'] is nan or box_score_raw['overtime'] == ' ':
          overtime = 0
       else:
          overtime = box_score_raw['overtime']
       
       if box_score_raw['total'] is None or box_score_raw['total'] is nan or box_score_raw['total'] == ' ':
          total = 0
       else:
          total = total = box_score_raw['total']
       
    except Exception as e:
       quarter1 = 0
       quarter2 = 0
       quarter3 = 0
       quarter4 = 0
       overtime = 0
       total = 0
    transform_logfile.write(f'{quarter1}, {quarter2}, {quarter3}, {quarter4}, {overtime}, {total}\n')
    return quarter1, quarter2, quarter3, quarter4, overtime, total

def transform_location(location_raw: str, locations_df: dict, transform_logfile: object):
    """Function that trims trailing space characters from game location
       Accepts `locations_raw`: String, `locations_df`: Pandas DataFrame, `transform_logfile`: File Object
       Returns `location_transformed`: Number"""
    transform_logfile.write(f'Transforming location {location_raw} -> ')
    try:
      location_transformed = locations_df.loc[locations_df['stadium'] == location_raw, 'location_id'].item()
      transform_logfile.write(f'{location_transformed}\n')
    except Exception as e:
      location_transformed = 0
      transform_logfile.write(f'{e}\n')
    
    return location_transformed

def transform_game_time(game_timestamp: str, transform_logfile: object):
    """Function that extracts time from datetime string and converts to datetime object
       Accepts `game_timestamp`: String, `transform_logfile`: File Object
       Returns `game_time`: Datetime"""
    transform_logfile.write(f'Transforming game time {game_timestamp} -> ')
    try:
       game_time = game_timestamp.split(',')[0]
       transform_logfile.write(f'{game_time}\n')
    except Exception as e:
       game_time = 'TBD'
       transform_logfile.write(f'{e}\n')
    return game_time

def transform_game_date(game_timestamp: str, transform_logfile: object):
    """Function that extracts date from datetime string and converts to datetime object
       Accepts `game_timestamp`: String, `transform_logfile`: File Object
       Returns `game_date`: Datetime, `game_month`: Number, `game_day`: Number, `game_year`: Number"""
    transform_logfile.write(f'Transforming game date {game_timestamp} -> ')
    months = {
       'january': 1,
       'february': 2,
       'march': 3,
       'april': 4,
       'may': 5,
       'june': 6,
       'july': 7,
       'august': 8,
       'september': 9,
       'october': 10,
       'november': 11,
       'december': 12
    }
    
    try:
       game_date = game_timestamp.replace(f'{game_timestamp.split(",")[0]}, ', '')
       game_month = months[game_date.split()[0].lower()]
       game_day = int(game_date.split()[1].replace(',', ''))
       game_year = int(game_date.split()[2])
       transform_logfile.write(f'{game_date}\n')
    except Exception as e:
       game_date = 'TBD'
       game_month = int(1)
       game_day = int(1)
       game_year = int(2025)
       transform_logfile.write(f'{e}\n')

    return game_date, game_month, game_day, game_year

def transform_stadium_attendance(attendance_raw: str, transform_logfile: object):
    """Function that converts attendance into a Number type field
       Accepts `attendance_raw`: String, `transform_logfile`: File Object
       Returns `attendance`: Number"""
    transform_logfile.write(f'Transforming attendance {attendance_raw} -> ')
    try:
       attendance = int(attendance_raw.lstrip('Attendance: ').replace(',', ''))
       transform_logfile.write(f'{attendance}\n')
    except Exception as e:
       attendance = 0
       transform_logfile.write(f'{e}\n')
    return attendance