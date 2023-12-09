"""
Pickem ETL
Author: Gabe Baduqui

Extract TeamID from a given Game ID from a given HREF attribute.
"""

def get_team_id(team_href_attr: str):
    """Function that extracts team ID from the given HREF attribute
       Accepts `team_href_attr`: String 
       Returns `team_id`: String"""
    # Example `team_href_attr` string: 'https://www.espn.com/college-football/team/_/id/0000/schoolName-schoolMascot'
    begin_idx = team_href_attr.index('/id/') + 4
    end_idx = team_href_attr.rfind('/')
    team_id = team_href_attr[begin_idx:end_idx]
    return team_id