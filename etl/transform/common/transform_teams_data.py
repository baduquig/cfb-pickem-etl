"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare Teams data
"""

def transform__record(record_raw: str):
    """Function that transforms record elements into individual fields
       Accepts: `record_raw`: String
       Returns: `wins`: Number, `losses`: Number, `ties`: Number,"""
    record_elements = record_raw.split('-')
    wins = int(record_elements[0])
    losses = int(record_elements[1])
    if len(record_elements) > 2:
        ties = int(record_elements[2])
    else:
        ties = 0
    return wins, losses, ties