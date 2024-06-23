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
    conn = mysql.connector.connect(**config)
    return conn


def record_exists_in_table(table_name: str, record: list, logfile: object):
    """Function to verify if record exists in given table
       Accepts: `table_name`: String, `record`: List, `logfile`: Logfile object
       Returns: `record_exists`: Boolean"""
    if table_name.lower() == 'games':
        record_exists_query = f"SELECT COUNT(*) FROM GAMES WHERE LEAGUE = '{record.league}' AND GAME_ID = {int(record.game_id)};"
    if table_name.lower() == 'teams':
        record_exists_query = f"SELECT COUNT(*) FROM TEAMS WHERE LEAGUE = '{record.league}' AND TEAM_ID = {record.team_id};"
    if table_name.lower() == 'locations':
        record_exists_query = f"SELECT COUNT(*) FROM LOCATIONS WHERE LEAGUE = '{record.league}' AND LOCATION_ID = {int(record.location_id)};"
    
    try:
        conn = instantiate_connection()
        cursor = conn.cursor()
        cursor.execute(record_exists_query)
        count = cursor.fetchone()
        cursor.close()
        conn.close()

        if count[0] > 0:
            record_exists = True
        else:
            record_exists = False
    except Exception as e:
        record_exists = 0
        print(f'Error occurred updating record {record}:\n{e}')
        logfile.write(f'Error occurred updating record {record}:\n{e}\n')

    return record_exists


def update_record(table_name: str, record: list, logfile: object):
    """Function to verify if record exists in given table
       Accepts: `table_name`: String, `record`: List, `logfile`: File Object 
       Returns: n/a"""
    if table_name.lower() == 'games':
        update_stmt = f"""UPDATE GAMES
                            SET AWAY_TEAM = '{record.away_team}',
                                HOME_TEAM = '{record.home_team}',
                                LOCATION = {record.location},
                                TV_COVERAGE = '{record.tv_coverage}',
                                BETTING_LINE = '{record.betting_line}',
                                BETTING_LINE_OVER_UNDER = '{record.betting_over_under}',
                                ATTENDANCE = {int(record.attendance)},
                                AWAY_WIN_PCT = '{record.away_win_pct}',
                                HOME_WIN_PCT = '{record.home_win_pct}',
                                AWAY_QUARTER1 = '{record.away_quarter1}',
                                AWAY_QUARTER2 = '{record.away_quarter2}',
                                AWAY_QUARTER3 = '{record.away_quarter3}',
                                AWAY_QUARTER4 = '{record.away_quarter4}',
                                AWAY_OVERTIME = '{record.away_overtime}',
                                AWAY_TOTAL = '{record.away_total}',
                                HOME_QUARTER1 = '{record.home_quarter1}',
                                HOME_QUARTER2 = '{record.home_quarter2}',
                                HOME_QUARTER3 = '{record.home_quarter3}',
                                HOME_QUARTER4 = '{record.home_quarter4}',
                                HOME_OVERTIME = '{record.home_overtime}',
                                HOME_TOTAL = '{record.home_total}',
                                GAME_TIME = '{record.game_time}',
                                GAME_DATE = '{record.game_date}',
                                GAME_MONTH = {int(record.game_month)},
                                GAME_DAY = {int(record.game_day)},
                                GAME_YEAR = {int(record.game_year)}
                            WHERE LEAGUE = '{record.league}'
                                AND GAME_ID = {int(record.game_id)};"""

    if table_name.lower() == 'teams':
        update_stmt = f"""UPDATE TEAMS
                            SET NAME = '{record['name']}',
                                MASCOT = '{record.mascot}',
                                LOGO_URL = '{record.logo_url}',
                                CONFERENCE_NAME = '{record.conference_name}',
                                CONFERENCE_WINS = {round(record.conference_wins)},
                                CONFERENCE_LOSSES = {round(record.conference_losses)},
                                CONFERENCE_TIES = {round(record.conference_ties)},
                                OVERALL_WINS = {round(record.overall_wins)},
                                OVERALL_LOSSES = {round(record.overall_losses)},
                                OVERALL_TIES - {round(record.overall_ties)}
                            WHERE LEAGUE = '{record.league}'
                                AND TEAM_ID = '{record.team_id}';"""
       
    if table_name.lower() == 'locations':
        update_stmt = f"""UPDATE LOCATIONS
                            SET STADIUM = '{record.stadium}',
                                STADIUM_CAPACITY = {int(record.stadium_capacity)},
                                CITY = '{record.city}',
                                STATE = '{record.state}',
                                LATITUDE = '{record.latitude}',
                                LONGITUDE = '{record.longitude}'
                            WHERE LEAGUE = '{record.league}'
                                AND LOCATION_ID = {int(record.location_id)};"""
        
    try:
        logfile.write(f'Updating record for {record}\n')
        conn = instantiate_connection()
        cursor = conn.cursor()
        cursor.execute(update_stmt)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f'Error occurred with following update statement\n{update_stmt}')
        logfile.write(f'Error occurred with following update statement\n{update_stmt}\n')


def insert_record(table_name: str, record: list, logfile: object):
    """Function to verify if record exists in given table
       Accepts: `table_name`: String, `record`: List, `logfile`: File Object 
       Returns: n/a"""
    if table_name.lower() == 'games':
        insert_stmt = f"""INSERT INTO GAMES (GAME_ID, LEAGUE, AWAY_TEAM, HOME_TEAM, LOCATION, TV_COVERAGE, BETTING_LINE, 
                                                    BETTING_LINE_OVER_UNDER, ATTENDANCE, AWAY_WIN_PCT, HOME_WIN_PCT, AWAY_QUARTER1, 
                                                    AWAY_QUARTER2, AWAY_QUARTER3, AWAY_QUARTER4, AWAY_OVERTIME, AWAY_TOTAL,
                                                    HOME_QUARTER1, HOME_QUARTER2, HOME_QUARTER3, HOME_QUARTER4, HOME_OVERTIME, 
                                                    HOME_TOTAL, GAME_TIME, GAME_DATE, GAME_MONTH, GAME_DAY, GAME_YEAR)
                            VALUES ({int(record.game_id)}, '{record.league}', '{record.away_team}', 
                                    '{record.home_team}', '{record.location}', '{record.tv_coverage}', 
                                    '{record.betting_line}', '{record.betting_over_under}', 
                                    {int(record.attendance)}, '{record.away_win_pct}', '{record.home_win_pct}', 
                                    {int(record.away_quarter1)}, {int(record.away_quarter2)}, {int(record.away_quarter3)}, 
                                    {int(record.away_quarter4)}, {int(record.away_overtime)}, {int(record.away_total)}, 
                                    {int(record.home_quarter1)}, {int(record.home_quarter2)}, {int(record.home_quarter3)}, 
                                    {int(record.home_quarter4)}, {int(record.home_overtime)}, {int(record.home_total)}, 
                                    '{record.game_time}', '{record.game_date}', {int(record.game_month)}, 
                                    {int(record.game_day)}, {int(record.game_year)});"""
    if table_name.lower() == 'teams':
        insert_stmt = f"""INSERT INTO TEAMS (TEAM_ID, LEAGUE, NAME, MASCOT, LOGO_URL, CONFERENCE_NAME, CONFERENCE_WINS, 
                                                    CONFERENCE_LOSSES, CONFERENCE_TIES, OVERALL_WINS, OVERALL_LOSSES, OVERALL_TIES)
                            VALUES ('{record.team_id}', '{record.league}', '{record['name']}', '{record.mascot}',
                                    '{record.logo_url}', '{record.conference_name}', {round(record.conference_wins)},
                                    {round(record.conference_losses)}, {round(record.conference_ties)}, {round(record.overall_wins)},
                                    {round(record.overall_losses)}, {round(record.overall_ties)});"""
    if table_name.lower() == 'locations':
        insert_stmt = f"""INSERT INTO LOCATIONS (LEAGUE, LOCATION_ID, STADIUM, STADIUM_CAPACITY, CITY, STATE, LATITUDE, LONGITUDE)
                            VALUES ('{record.league}', {int(record.location_id)}, '{record.stadium}', 
                                    {record.stadium_capacity}, '{record.city}', '{record.state}',
                                    '{record.latitude}', '{record.longitude}');"""

    try:
        logfile.write(f'Inserting record for {record}\n')
        conn = instantiate_connection()
        cursor = conn.cursor()
        cursor.execute(insert_stmt)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f'Error occurred with following insert statement:\n{insert_stmt}')
        logfile.write(f'Error occurred with following insert statement:\n{insert_stmt}\n')
        