from pdetl import Pipeline
from settings import load
from settings import DATABASES, CONSUMOS_CLASS
import datetime
import os


sqldir = 'sqlfiles'
stagingdir = 'staging'


def run(fact_table, dia, source, target):
    print "start ", dia

    idtempo = "{}".format(dia.strftime("%Y%m%d"))

    p = Pipeline()
    p.add_source("sql", "consumos", "source", url=DATABASES[source])
    p.add_source("sql", "target", "target", url=DATABASES[target], table=fact_table)

    query = load(os.path.join(sqldir, 'tmpconsumos.sql'))
    p.extract("consumos", params={'query': query, 'params': {'day': dia}}, save=True)
    p.add_column("idtempo", idtempo)
    print "extract: ", len(p.data)

    r = p.clean("target", conditions=[("idtempo", "eq", idtempo)])
    print "clean: ", r.rowcount
    n = p.load("target")
    print "load: ", n
    print "-------------------"
