'''
A utility method for generating a connection to the Postgres database
'''

from psycopg2 import connect
from psycopg2.extensions import connection
from config import constants


def get_database_connection() -> connection:
    '''
    Returns a psycopg2.extensions.connection object to interact with the Postgres database
    '''
    conn = connect(
        dbname = constants.POSTGRES_DB,
        user = constants.POSTGRES_USER,
        password = constants.POSTGRES_PASSWORD,
        host= constants.POSTGRES_HOST
    )
    conn.autocommit = True
    return conn
