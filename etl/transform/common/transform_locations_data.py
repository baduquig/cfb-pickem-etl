"""
Pickem ETL
Author: Gabe Baduqui

Cleanse, format and prepare Locations data
"""

def transform_stadium_capacity(stadium_capacity_raw: str, transform_logfile: object):
    """Function that converts stadium_capacity into a Number type field
       Accepts `stadium_capacity_raw`: String, `transform_logfile`: File Object
       Returns `stadium_capacity`: Number"""
    transform_logfile.write(f'Transforming stadium capacity {stadium_capacity_raw} -> ')
    
    try:
      stadium_capacity = int(stadium_capacity_raw.lstrip('Capacity: ').replace(',', ''))
      transform_logfile.write(f'{stadium_capacity}\n')
    except Exception as e:
      stadium_capacity = 0
      transform_logfile.write(f'{e}\n')

    return stadium_capacity