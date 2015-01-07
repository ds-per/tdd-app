from pdetl import Pipeline
from settings import load
from settings import DATABASES, CONSUMOS_CLASS
import datetime
import os


sqldir = 'sqlfiles'
stagingdir = 'staging'


def run(fact_table, dia, source, target):
    print "start ", dia

    idtempo = int("{}".format(dia.strftime("%Y%m%d")))

    p = Pipeline()
    p.add_source("sql", "consumos", "source", url=DATABASES[source])
    p.add_source("sql", "parque", "source", url=DATABASES[target])
    p.add_source("sql", "target", "target", url=DATABASES[target], table="tmp_fct_consumos")

    query = load(os.path.join(sqldir, 'tmpconsumos.sql'))
    p.extract("consumos", params={'query': query, 'params': {'day': dia}}, save=True)
    p.add_column("idtempo", idtempo)
    print "extract: ", len(p.data)

    r = p.clean("target", conditions=[("idtempo", "eq", idtempo)])
    print "clean: ", r.rowcount
    n = p.load("target")
    print "load: ", n
    print "-------------------"

    query = load(os.path.join(sqldir, 'tmpparque.sql'))
    p.extract("parque", params={'query': query, 'params': {'idtempo': idtempo}}, save=True)
    p.add_column("idtempo", idtempo)
    print "extract: ", len(p.data)

    print "tmp_parque"
    p.add_source("sql", "target2", "target", url=DATABASES[target], table="tmp_fct_parque")
    r = p.clean("target2", conditions=[("idtempo", "eq", idtempo)])
    print "clean: ", r.rowcount
    n = p.load("target2")
    print "load: ", n
    print "-------------------"
