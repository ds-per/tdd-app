from pdetl import Pipeline
from pdetl import sql
from settings import DATABASES, CONSUMOS_CLASS
import datetime
import os
import pandas as pd


sqldir = 'sqlfiles'
stagingdir = 'staging'

def find_class(n, classe):
    """Calcula a classe de n. Classe importada do ficheiro settings.py
    """
    i = 0
    while i < len(classe):
        if n > classe[i] and n <= classe[i+1]:
            return classe[i+1]
        i += 1


def applyclass(self, col, newcol, bins):
    self.data[newcol] = self.data[col].apply(lambda x: find_class(x, bins))


def run(fact_table, dia, source, target):
    print "start ", dia

    yest = dia - datetime.timedelta(days=1)
    idtempo = "{}".format(dia.strftime("%Y%m%d"))

    p = Pipeline()
    p.add_source("sql", "consumos", "source", url=DATABASES[source])
    p.add_source("sql", "churn", "source", url=DATABASES[source])
    p.add_source("sql", "target", "target", url=DATABASES[target], table="fct_consumos")

    filename = "consumos74mz_" + idtempo + ".h5"
    p.add_source("hdf", "staging", "staging", path=stagingdir, filename=filename)

    if p.datastore.staging.exists():
        p.extract("staging", save=True)
        print "Extract from staging, ", len(p.data)
    else:
        query = sql.load(os.path.join(sqldir, 'consumos74.sql'))
        query_churn = sql.load(os.path.join(sqldir, 'churn74.sql'))
        p.extract("consumos", params={'query': query, 'params': {'day': dia}})
        p.extract("churn", params={'query': query_churn,
                                   'params': {'day': dia, 'yest': yest}})
        print "extract: ", len(p.data)

        p.concat(["consumos", "churn"], save=True)
        p.add_column("idtempo", idtempo)
        p.transform(None, applyclass, "demora", "cdemora", CONSUMOS_CLASS)

        p.load("staging")
        print "Save staging, ", p.datastore.staging.fullpath

    r = p.clean("target", conditions=[("idtempo", "eq", idtempo)])
    print "clean: ", r.rowcount
    p.data = p.data.where(pd.notnull(p.data), None)
    n = p.load("target")
    print "load: ", n
    print "-------------------"
