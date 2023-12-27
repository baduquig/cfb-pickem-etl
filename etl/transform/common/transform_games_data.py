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
    quarter1 = int(box_score_raw[0])
    quarter2 = int(box_score_raw[1])
    quarter3 = int(box_score_raw[2])
    quarter4 = int(box_score_raw[3])
    total = int(box_score_raw[4])
    return quarter1, quarter2, quarter3, quarter4, total

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
    time_raw = game_timestamp.split(',')[0]
    game_time = datetime.strptime(time_raw, '%I:%M %p')
    return game_time

def transform_game_date(game_timestamp: str):
    """Function that extracts date from datetime string and converts to datetime object
       Accepts: `game_timestamp`: String
       Returns: `game_date`: Datetime"""
    date_raw = game_timestamp.lstrip(game_timestamp.split(',')[0])
    game_date = datetime.strptime(date_raw, '%B %d, %Y') 
    return game_date

def transform_stadium_capacity(stadium_capacity_raw: str):
    """Function that converts stadium_capacity into a Number type field
       Accepts `stadium_capacity_raw`: String
       Returns `stadium_capacity`: Number"""
    stadium_capacity = int(stadium_capacity_raw.lstrip('Capacity: '))
    return stadium_capacity

def transform_stadium_attendance(attendance_raw: str):
    """Function that converts attendance into a Number type field
       Accepts `attendance_raw`: String
       Returns `attendance`: Number"""
    attendance = int(attendance_raw.lstrip('Attendance: '))
    return attendance