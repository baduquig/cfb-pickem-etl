"""
Pickem ETL
Author: Gabe Baduqui

Load pickem data from various web sources into MySQL Database.
"""
import mysql.connector
import etl.utils.credentials as cred

def instantiate_connection():
    """Function to instantiate connection to MySQL `PICKEM_GB` database
       Accepts: n/a
       Returns: `conn`: MySQLConnection Object"""
    config = cred.db_config
    print(config)
    conn = mysql.connector.connect(**config)
    return conn

def record_exists_in_table(cursor: object, table_name: str, record: list):
    """Function to verify if record exists in given table
       Accepts: `cursor`: MySQL Database Connection Cursor, `table_name`: String, `record`: List
       Returns: `record_exists`: Boolean"""
    if table_name.lower() == 'games':
        record_exists_query = f"SELECT * FROM PICKEM_GAMES WHERE LEAGUE = {record['league']} AND GAME_ID = {record['game_id']};"
    if table_name.lower() == 'teams':
        record_exists_query = f"SELECT * FROM PICKEM_TEAMS WHERE LEAGUE = {record['league']} AND TEAM_ID = {record['team_id']};"
    if table_name.lower() == 'locations':
        record_exists_query = f"SELECT * FROM PICKEM_LOCATIONS WHERE LEAGUE = {record['league']} AND LOCATION_ID = {record['location_id']};"
    
    cursor.execute(record_exists_query)
    if cursor.fetchall() > 0:
        record_exists = True
    else:
        record_exists = False    
    return record_exists


def update_record(conn: object, cursor: object, table_name: str, record: list, logfile: object):
    """Function to verify if record exists in given table
       Accepts: `conn`: MySQL Database Connection, `cursor`: MySQL Database Connection Cursor, `table_name`: String, `record`: List, `logfile`: File Object 
       Returns: n/a"""
    if table_name.lower() == 'games':
        update_stmt = f"""UPDATE PICKEM_GAMES
                            SET AWAY_TEAM_ID = \'{record['away_team_id']}\',
                                HOME_TEAM_ID = \'{record['home_team_id']}\',
                                STADIUM = \'{record['stadium']}\',
                                LOCATION = \'{record['location']}\',
                                TV_COVERAGE = \'{record['tv_coverage']}\',
                                BETTING_LINE = \'{record['betting_line']}\',
                                BETTING_OVER_UNDER = \'{record['betting_over_under']}\',
                                ATTENDANCE = \'{record['attendance']}\',
                                AWAY_WIN_PCT = \'{record['away_win_pct']}\',
                                HOME_WIN_PCT = \'{record['home_win_pct']}\',
                                AWAY_QUARTER1 = \'{record['away_quarter1']}\',
                                AWAY_QUARTER2 = \'{record['away_quarter2']}\',
                                AWAY_QUARTER3 = \'{record['away_quarter3']}\',
                                AWAY_QUARTER4 = \'{record['away_quarter4']}\',
                                AWAY_OVERTIME = \'{record['away_overtime']}\',
                                AWAY_TOTAL = \'{record['away_total']}\',
                                HOME_QUARTER1 = \'{record['home_quarter1']}\',
                                HOME_QUARTER2 = \'{record['home_quarter2']}\',
                                HOME_QUARTER3 = \'{record['home_quarter3']}\',
                                HOME_QUARTER4 = \'{record['home_quarter4']}\',
                                HOME_OVERTIME = \'{record['home_overtime']}\',
                                HOME_TOTAL = \'{record['home_total']}\',
                                GAME_TIME = \'{record['game_time']}\',
                                GAME_DATE = \'{record['game_date']}\',
                                GAME_MONTH = \'{record['game_month']}\',
                                GAME_DAY = \'{record['game_day']}\',
                                GAME_YEAR = \'{record['game_year']}\',
                            WHERE LEAGUE = \'{record['league']}\'
                                AND GAME_ID = \'{record['game_id']}\'"""

    if table_name.lower() == 'teams':
        update_stmt = f"""UPDATE PICKEM_TEAMS
                            SET NAME = '\{record['name']}\',
                                MASCOT = '\{record['mascot']}\',
                                LOGO_URL = '\{record['logo_url']}\',
                                CONFERENCE_NAME = '\{record['conference_name']}\',
                                CONFERENCE_WINS = '\{record['conference_wins']}\',
                                CONFERENCE_LOSSES = '\{record['conference_losses']}\',
                                CONFERENCE_TIES = '\{record['conference_ties']}\',
                                OVERALL_WINS = '\{record['overall_wins']}\',
                                OVERALL_LOSSES = '\{record['overall_losses']}\',
                                OVERALL_TIES - '\{record['overall_ties']}\'
                            WHERE LEAGUE = \'{record['league']}\'
                                AND TEAM_ID = \'{record['team_id']}\';"""
       
    if table_name.lower() == 'locations':
        update_stmt = f"""UPDATE PICKEM_LOCATIONS
                            SET STADIUM = '\{record['stadium']}\',
                                STADIUM_CAPACITY = '\{record['stadium_capacity']}\',
                                CITY = '\{record['city']}\',
                                STATE = '\{record['state']}\',
                                LATITUDE = '\{record['latitude']}\',
                                LONGITUDE = '\{record['longitude']}\'
                            WHERE LEAGUE = '\{record['league']}\'
                                AND LOCATION_ID = '\{record['location_id']}\'"""
        
    try:
        logfile.write(f'Updating record for {record}\n')
        cursor.execute(update_stmt)
        conn.commit()
    except Exception as e:
        print(f'Error occurred updating record {record}:\n{e}')
        logfile.write(f'Error occurred updating record {record}:\n{e}\n')


def insert_record(conn: object, cursor: object, table_name: str, record: list, logfile: object):
    """Function to verify if record exists in given table
       Accepts: `conn`: MySQL Database Connection, `cursor`: MySQL Database Connection Cursor, `table_name`: String, `record`: List, `logfile`: File Object 
       Returns: n/a"""
    if table_name.lower() == 'games':
        insert_stmt = f"""INSERT INTO PICKEM_GAMES (AWAY_TEAM_ID, HOME_TEAM_ID, STADIUM, LOCATION, TV_COVERAGE, BETTING_LINE, 
                                                    BETTING_OVER_UNDER, ATTENDANCE, AWAY_WIN_PCT, HOME_WIN_PCT, AWAY_QUARTER1, 
                                                    AWAY_QUARTER2, AWAY_QUARTER3, AWAY_QUARTER4, AWAY_OVERTIME, AWAY_TOTAL,
                                                    HOME_QUARTER1, HOME_QUARTER2, HOME_QUARTER3, HOME_QUARTER4, HOME_OVERTIME, 
                                                    HOME_TOTAL, GAME_TIME, GAME_DATE, GAME_MONTH, GAME_DAY, GAME_YEAR)
                            VALUES (\'{record['game_id']}\',, \'{record['league']}\', \'{record['away_team_id']}\', 
                                    \'{record['home_team_id']}\', \'{record['stadium']}\', \'{record['location']}\', 
                                    \'{record['tv_coverage']}\', \'{record['betting_line']}\', \'{record['betting_over_under']}\', 
                                    \'{record['attendance']}\', \'{record['away_win_pct']}\', \'{record['home_win_pct']}\', 
                                    \'{record['away_quarter1']}\', \'{record['away_quarter2']}\', \'{record['away_quarter3']}\', 
                                    \'{record['away_quarter4']}\', \'{record['away_overtime']}\', \'{record['away_total']}\', 
                                    \'{record['home_quarter1']}\', \'{record['home_quarter2']}\', \'{record['home_quarter3']}\', 
                                    \'{record['home_quarter4']}\', \'{record['home_overtime']}\', \'{record['home_total']}\', 
                                    \'{record['game_time']}\', \'{record['game_date']}\', \'{record['game_month']}\', 
                                    \'{record['game_day']}\', \'{record['game_year']}\');"""
    if table_name.lower() == 'teams':
        insert_stmt = f"""INSERT INTO PICKEM_TEAMS (TEAM_ID, LEAGUE, NAME, MASCOT, LOGO_URL, CONFERENCE_NAME, CONFERENCE_WINS, 
                                                    CONFERENCE_LOSSES, CONFERENCE_TIES, OVERALL_WINS, OVERALL_LOSSES, OVERALL_TIES)
                            VALUES (\'{record['team_id']}\', \'{record['league']}\', '\{record['name']}\', '\{record['mascot']}\',
                                    '\{record['logo_url']}\', '\{record['conference_name']}\', '\{record['conference_wins']}\',
                                    '\{record['conference_losses']}\', '\{record['conference_ties']}\', '\{record['overall_wins']}\',
                                    '\{record['overall_losses']}\', '\{record['overall_ties']}\');"""
    if table_name.lower() == 'locations':
        insert_stmt = f"""INSERT INTO PICKEM_LOCATIONS (LEAGUE, LOCATION_ID, STADIUM, STADIUM_CAPACITY, CITY, STATE, LATITUDE, LONGITUDE)
                            VALUES ('\{record['league']}\', '\{record['location_id']}\', '\{record['stadium']}\', 
                                    '\{record['stadium_capacity']}\', '\{record['city']}\', '\{record['state']}\',
                                    '\{record['latitude']}\', '\{record['longitude']}\');"""

    try:
        logfile.write(f'Inserting record for {record}\n')
        cursor.execute(insert_stmt)
        conn.commit()
    except Exception as e:
        print(f'Error occurred inserting record {record}\n{e}')
        logfile.write(f'Error occurred inserting record {record}\n{e}\n')
        