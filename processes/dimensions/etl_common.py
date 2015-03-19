import mysql.connector
import collections
from mysql.connector import errorcode
from settings import DATABASES_MAPPING

dim_loaded = collections.namedtuple('Dim_load', ['name', 'datestart', 'dateend', 'status', 'details'])

def get_df_row_for_values(dim_values, val, source):
    return dim_values[
        (dim_values.codigocliente == val)
        & (dim_values.source == source)
    ]


def load_dimension(variables, query, db):
    has_error = False
    try:
        cnx = mysql.connector.connect(**DATABASES_MAPPING[db])
        cursor = cnx.cursor()

        for v in variables:
            cursor.execute(query, v)

        cnx.commit()
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        has_error = True
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            message = "Wrong DB user or password"
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            message = "Database does not exist"
        else:
            message = err
        cnx.close()
    finally:
        if has_error:
            raise Exception(message)