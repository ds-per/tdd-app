import os
from settings import load, DATABASES, DIMENSION_UPDATE_STATUS
from pdetl import Pipeline
import pandas as pd
import datetime
from etl_common import load_dimension, dim_loaded

sql_dir = "dim_sql_files"
my_name = "dim_filme"


def run(source, target):
    datestart = datetime.datetime.now()
    print "Starting etl_dim_filme at", datestart

    p = Pipeline()
    # TODO: recheck this
    p.add_source("sql", "source_aaaa_movies", "source", url=DATABASES["129-aaaa"])

    p.add_source("sql", "source_dw_dim_filmes", "source", url=DATABASES[target])
    p.add_source("sql", "target_dw_dim_loja", "target", url=DATABASES[target], table="dim_filme")

    get_source_filmes = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'source_aaaa_filme.sql')))
    get_dw_filmes = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'source_dw_dim_filme.sql')))

    filmes = p.extract("source_aaaa_movies", {'query': get_source_filmes})
    dim_filmes = p.extract("source_dw_dim_filmes", {'query': get_dw_filmes})

    filmes = filmes[~filmes['codigo'].isin(dim_filmes['codigo'].values)]

    status = DIMENSION_UPDATE_STATUS['success']
    details = ""

    if not filmes.empty:
        try:
            p.data = filmes
            p.load("target_dw_dim_loja")
            details += str(len(p.data.index)) + " new movies.\n"
        except Exception, e:
            status = DIMENSION_UPDATE_STATUS['failure']
            details += "At insert\n" + str(e)
            details += "\n"

    dateend = datetime.datetime.now()

    d = dim_loaded(my_name, datestart, dateend, status, details)
    return d