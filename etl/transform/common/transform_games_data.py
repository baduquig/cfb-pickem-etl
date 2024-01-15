"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare Games data
"""

def transform_box_score(box_score_raw: dict, transform_logfile: object):
    """Function that transforms box score data element into individuals fields
       Accepts `box_score_raw`: Dictionary, `transform_logfile`: Logfile Object
       Returns `quarter1`: Number, `quarter2`: Number, `quarter3`: Number, `quarter4`: Number, `total`: Number"""
    transform_logfile.write(f'Transforming box score {box_score_raw} -> ')
    quarter1 = box_score_raw['1']
    quarter2 = box_score_raw['2']
    quarter3 = box_score_raw['3']
    quarter4 = box_score_raw['4']
    overtime = box_score_raw['overtime']
    total = box_score_raw['total']
    transform_logfile.write(f'{quarter1}, {quarter2}, {quarter3}, {quarter4}, {overtime}, {total}\n')
    return quarter1, quarter2, quarter3, quarter4, overtime, total

def transform_location(location_raw: str, transform_logfile: object):
    """Function that trims trailing space characters from game location
       Accepts `locations_raw`: String, `transform_logfile`: File Object
       Returns `location_transformed`: String"""
    transform_logfile.write(f'Transforming location {location_raw} -> ')
    location_transformed = location_raw.rstrip()
    transform_logfile.write(f'{location_transformed}\n')
    return location_transformed

def transform_game_time(game_timestamp: str, transform_logfile: object):
    """Function that extracts time from datetime string and converts to datetime object
       Accepts `game_timestamp`: String, `transform_logfile`: File Object
       Returns `game_time`: Datetime"""
    transform_logfile.write(f'Transforming game time {game_timestamp} -> ')
    game_time = game_timestamp.split(',')[0]
    transform_logfile.write(f'{game_time}\n')
    return game_time

def transform_game_date(game_timestamp: str, transform_logfile: object):
    """Function that extracts date from datetime string and converts to datetime object
       Accepts `game_timestamp`: String, `transform_logfile`: File Object
       Returns `game_date`: Datetime"""
    transform_logfile.write(f'Transforming game date {game_timestamp} -> ')
    game_date = game_timestamp.lstrip(f'{game_timestamp.split(",")[0]}, ')
    transform_logfile.write(f'{game_date}\n')
    return game_date

def transform_stadium_capacity(stadium_capacity_raw: str, transform_logfile: object):
    """Function that converts stadium_capacity into a Number type field
       Accepts `stadium_capacity_raw`: String, `transform_logfile`: File Object
       Returns `stadium_capacity`: Number"""
    transform_logfile.write(f'Transforming stadium capacity {stadium_capacity_raw} -> ')
    stadium_capacity = int(stadium_capacity_raw.lstrip('Capacity: ').replace(',', ''))
    transform_logfile.write(f'{stadium_capacity}\n')
    return stadium_capacity

def transform_stadium_attendance(attendance_raw: str, transform_logfile: object):
    """Function that converts attendance into a Number type field
       Accepts `attendance_raw`: String, `transform_logfile`: File Object
       Returns `attendance`: Number"""
    transform_logfile.write(f'Transforming attendance {attendance_raw} -> ')
    attendance = int(attendance_raw.lstrip('Attendance: ').replace(',', ''))
    transform_logfile.write(f'{attendance}\n')
    return attendance