"""
Pickem ETL
Author: Gabe Baduqui

Return current timestamp
"""
from datetime import datetime

def get_timestamp():
    """Function that returns string of current timestamp in the format: M-D-Y-H:M:S
       Accepts: N/A
       Returns `timestamp`: String"""
    timestamp = datetime.now().strftime('%m%d%Y_%H%M%S')
    return timestamp