"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare Teams data
"""

def transform_conference_name(conference_name_raw: str, transform_logfile: object):
    """Function that extracts just the name of a conference from the given standings header string
       Accepts `conference_name_raw`: String, `transform_logfile`: File Object
       Returns `conference_name`: String"""
    transform_logfile.write(f'Transforming conference name {conference_name_raw} -> ')

    standings_header_words = conference_name_raw.split(' ')
    conference_name = conference_name_raw.lstrip(f'{standings_header_words[0]} ').rstrip(f' {standings_header_words[-1]}')

    transform_logfile.write(f'{conference_name}\n')
    return conference_name

def transform_record(record_raw: str, transform_logfile: object):
    """Function that transforms record elements into individual fields
       Accepts `record_raw`: String, `transform_logfile`: File Object
       Returns `wins`: Number, `losses`: Number, `ties`: Number,"""
    transform_logfile.write(f'Transforming record {record_raw} -> ')
    
    record_elements = record_raw.split('-')
    wins = int(record_elements[0])
    losses = int(record_elements[1])
    if len(record_elements) > 2:
        ties = int(record_elements[2])
    else:
        ties = 0

    transform_logfile.write(f'{wins}, {losses}, {ties}\n')
    return wins, losses, ties