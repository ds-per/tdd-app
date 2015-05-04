#import mysql.connector
import collections
#from mysql.connector import errorcode
from settings import DATABASES_MAPPING
import pymysql.cursors

dim_loaded = collections.namedtuple('Dim_load', ['name', 'datestart', 'dateend', 'status', 'details'])


def _get_connection(dbi):
    db = DATABASES_MAPPING[dbi]
    return pymysql.connect(
        host=db['host'],
        user=db['user'],
        passwd=db['password'],
        db=db['database'],
        charset=db['charset'],
        cursorclass=pymysql.cursors.DictCursor)


def get_df_row_for_values(dim_values, val, source):
    return dim_values[
        (dim_values.codigocliente == val)
        & (dim_values.source == source)
    ]


def load_dimension(variables, query, dbi, many=True, is_insert_stmt=False):
    has_error = False
    connection = _get_connection(dbi)
    cursor = connection.cursor()
    inserted_id = None
    try:
        if many:
            cursor.executemany(query, variables)
        else:
            cursor.execute(query, variables)
        if is_insert_stmt:
            inserted_id = cursor.lastrowid
        connection.commit()
    except Exception, e:
        has_error = True
        message = str(e)
    finally:
        cursor.close()
        connection.close()
        if has_error:
            raise Exception(message)
        if is_insert_stmt:
            return inserted_id

def get_sk(x, dim):
    try:
        print x
        return dim[(dim.id == x)].head(1).iloc[0]['sk']
    except LookupError, le:
        print "-", str(le)
        return 0
    except Exception, e:
        print ":", str(e)
        return -1