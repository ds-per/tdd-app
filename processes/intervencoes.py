from pdetl import Pipeline
from settings import load
from settings import DATABASES, INTERVENCAO_CLASS
import datetime
import os
import pandas as pd
import numpy as np

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

    idtempo = "{}".format(dia.strftime("%Y%m%d"))
    idtempo = int(idtempo)

    p = Pipeline()
    p.add_source("sql", "intervencoes", "source", url=DATABASES[source])
    p.add_source("sql", "target", "target", url=DATABASES[target], table=fact_table)

    query = load(os.path.join(sqldir, 'intervencoes.sql'))

    p.extract("intervencoes", params={'query': query, 'params': {'day': dia}}, save=True)

    if not p.data.empty:
        p.add_column("idtempo", idtempo)
        p.transform(None, applyclass, "demora", "classe_demora", INTERVENCAO_CLASS)
        p.transform(None, applyclass, "demora_exec", "cdemora_exec", INTERVENCAO_CLASS)
        print "extract: ", len(p.data)

        r = p.clean("target", conditions=[("idtempo", "eq", idtempo)])
        print "clean: ", r.rowcount
        n = p.load("target")
        print "load: ", n
    else:
        print "nothing to load"
    print "-------------------"


