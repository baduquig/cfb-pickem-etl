"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare Games data
"""
from datetime import datetime


def transform_box_score(box_score_raw: dict):
    """Function that transforms box score data element into individuals fields
       Accepts: `box_score_raw`: Dictionary
       Returns: `quarter1`: Number, `quarter2`: Number, `quarter3`: Number, `quarter4`: Number, `total`: Number"""
    quarter1 = box_score_raw['1']
    quarter2 = box_score_raw['2']
    quarter3 = box_score_raw['3']
    quarter4 = box_score_raw['4']
    overtime = box_score_raw['overtime']
    total = box_score_raw['total']
    return quarter1, quarter2, quarter3, quarter4, overtime, total

def transform_location(location_raw: str):
    """Function that trims trailing space characters from game location
       Accepts: `locations_raw`: String
       Returns: `location_transformed`: String"""
    location_transformed = location_raw.rstrip()
    return location_transformed

def transform_game_time(game_timestamp: str):
    """Function that extracts time from datetime string and converts to datetime object
       Accepts: `game_timestamp`: String
       Returns: `game_time`: Datetime"""
    game_time = game_timestamp.split(',')[0]
    return game_time

def transform_game_date(game_timestamp: str):
    """Function that extracts date from datetime string and converts to datetime object
       Accepts: `game_timestamp`: String
       Returns: `game_date`: Datetime"""
    game_date = game_timestamp.lstrip(f'{game_timestamp.split(",")[0]}, ')
    return game_date

def transform_stadium_capacity(stadium_capacity_raw: str):
    """Function that converts stadium_capacity into a Number type field
       Accepts `stadium_capacity_raw`: String
       Returns `stadium_capacity`: Number"""
    stadium_capacity = int(stadium_capacity_raw.lstrip('Capacity: ').replace(',', ''))
    return stadium_capacity

def transform_stadium_attendance(attendance_raw: str):
    """Function that converts attendance into a Number type field
       Accepts `attendance_raw`: String
       Returns `attendance`: Number"""
    attendance = int(attendance_raw.lstrip('Attendance: ').replace(',', ''))
    return attendance