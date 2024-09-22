"""
Pickem ETL
Author: Gabe Baduqui

Return all distinct dates from a given date range
"""
from datetime import timedelta

def date_range(start_date, end_date):
    """Fucntion that returns a list of all distinct dates within a given date range"""
    all_dates = []
    step=timedelta(days=1)
    x = start_date

    while x <= end_date:
        all_dates.append(x)
        x += step
    
    return all_dates