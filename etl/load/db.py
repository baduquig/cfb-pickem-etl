"""
Pickem ETL
Author: Gabe Baduqui

Load pickem data from various web sources into MySQL Database.
"""
import mysql.connector
import etl.utils.credentials as cred

config = cred.db_config

def instantiate_connection():
    """Function to instantiate connection to MySQL `PICKEM_GB` database
       Accepts: n/a
       Returns: `conn`: MySQLConnection Object"""
    conn = mysql.connector.connect(**config)
    return conn

def record_exists_in_table():
    """Function to verify if record exists in given table"""
    pass