from pdetl import Pipeline
from settings import load
from settings import DATABASES, CONSUMOS_CLASS
import datetime
import os
import pandas as pd

sqldir = 'sqlfiles'
stagingdir = 'staging'


def get_second_prod(prods):
    l = prods.split(',')
    try:
        prod = l[1]
    except IndexError:
        prod = "Nenhum"
    return prod


def products(self):
    self.data['produto'] = self.data['nome'].apply(lambda x: x.split(',')[0])
    self.data['produto2'] = self.data['nome'].apply(get_second_prod)


def run(fact_table, dia, source, target):
    print "start ", dia
    idtempo = "{}".format(dia.strftime("%Y%m%d"))

    p = Pipeline()
    p.add_source("sql", "parquews", "source", url=DATABASES[source])
    p.add_source("sql", "target", "target", url=DATABASES[target], table="fct_parquews")

    filename = "parquews_" + idtempo + ".h5"
    p.add_source("hdf", "staging", "staging", path=stagingdir, filename=filename)

    if p.datastore.staging.exists():
        p.extract("staging", save=True)
        print "Extract from staging, ", len(p.data)
    else:
        p.extract("parquews",
                  params={"query": "call p_fact('%s')" % dia.strftime("%Y-%m-%d")},
                  save=True)
        print "extract: ", len(p.data)
        p.add_column("idtempo", idtempo)
        p.load("staging")
        print "Save staging, ", p.datastore.staging.fullpath

    p.transform(None, products)
    p.data = p.data.groupby(['produto', 'produto2'],
                            as_index=False).agg({'numerocartao': pd.Series.nunique})
    p.data.rename(columns={'numerocartao': 'fact_count'}, inplace=True)
    p.add_column("idtempo", idtempo)
    r = p.clean("target", conditions=[("idtempo", "eq", idtempo)])
    print "clean: ", r.rowcount
    p.data = p.data.where(pd.notnull(p.data), None)
    n = p.load("target")
    print "load: ", n
    print "-------------------"

