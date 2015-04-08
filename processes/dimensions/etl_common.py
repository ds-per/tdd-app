#import mysql.connector
import collections
#from mysql.connector import errorcode
from settings import DATABASES_MAPPING
import pymysql.cursors

dim_loaded = collections.namedtuple('Dim_load', ['name', 'datestart', 'dateend', 'status', 'details'])

def get_df_row_for_values(dim_values, val, source):
    return dim_values[
        (dim_values.codigocliente == val)
        & (dim_values.source == source)
    ]

def load_dimension(variables, query, dbi):
    has_error = False
    db = DATABASES_MAPPING[dbi]
    connection = pymysql.connect(host=db['host'],
                             user=db['user'],
                             passwd=db['password'],
                             db=db['database'],
                             charset=db['charset'],
                             cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    try:
        cursor.executemany(query, variables)
        connection.commit()
    except Exception, e:
        has_error = True
        message = str(e)
    finally:
        cursor.close()
        connection.close()
        if has_error:
            raise Exception(message)