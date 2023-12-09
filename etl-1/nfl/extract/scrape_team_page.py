"""
Pickem ETL
Author: Gabe Baduqui

Extract Team Standings from a given HTML <tr> tag.
"""

def get_overall_record(team_standing_row: str):
    """Function that extracts the overall record from a given TR tag
       Accepts `team_standing_row`: <tr> HTML Element String
       Returns `overall_record`: String"""
    wins = team_standing_row[1].text
    losses = team_standing_row[2].text
    ties = team_standing_row[3].text

    if ties == '0':
        overall_record = f'{wins}-{losses}'
    else:
        overall_record = f'{wins}-{losses}-{ties}'
    
    return overall_record