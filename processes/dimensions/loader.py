import os
import datetime
import pandas as pd
from pdetl import Pipeline
from settings import load, DATABASES, DIMENSION_UPDATE_STATUS
from etl_common import dim_loaded


def not_updated(today, dims, dw_db):
    """Returns, for a list of dimensions, those that were not updated today

        Args
            today (datetime): today's date
            dims (list of str): dimensions list to update (each process has it's own list of dimensions
                in settings file)
            dw_db (str) dw db connection string
    """
    dim_up = load(os.path.join(os.path.dirname(__file__), os.path.join('dim_sql_files', 'dimensions_updated.sql')))

    p = Pipeline()
    p.add_source("sql", "dimensions_updated", "source", url=DATABASES[dw_db], table="dimensions_updated")
    dims_updated = p.extract("dimensions_updated", params={
        'query': dim_up,
        'params': {
            'dims': ",".join(dims),
            'dateend': today,
            'status': DIMENSION_UPDATE_STATUS['success']
        }})

    return list(set(dims) - set(dims_updated['name'].values))


def update_dimensions(source, target, dims, force=False):
    """Updates dimensions.

        Args
            source (str): source db connection string
            target (str): target db connection string
            dims (list of str): dimensions list to update (each process has it's own list of dimensions
                in settings file)
            force (bool, optional): forces update of dimensions even if they're already up to date. Defaults to False
    """
    today = datetime.date.today()

    if not force:
        dims = not_updated(today, dims, target)

    for dim in dims:
        print "Loading dimension", dim
        try:
            command = __import__("processes.dimensions.etl_%s" % dim, fromlist=['run'])
            result = command.run(source, target)
            p = Pipeline()
            p.add_source("sql", "dimensions_updated", "target", url=DATABASES[target], table="dimensions_updated")
            p.data = pd.DataFrame([result], columns=dim_loaded._fields)
            p.load("dimensions_updated")
        except ImportError:
            print "No etl file to load"