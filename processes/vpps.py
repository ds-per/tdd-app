from pdetl import Pipeline
from settings import load
from settings import DATABASES, DIM_DATE_FMT, FACT_DIMENSIONS
import datetime
import os
import pandas as pd
from dimensions.loader import update_dimensions

sqldir = 'sqlfiles'
lookupsdir = os.path.join(sqldir, 'lookups')

def get_sk(x, dim_lojas):
    try:
        return dim_lojas[(dim_lojas.id == x)].head(1).iloc[0]['sk']
    except LookupError, le:
        return 0
    except Exception, e:
        return -1

def run(fact_table, day, source, target):
    update_dimensions(source, target, FACT_DIMENSIONS['vpps'])

    p = Pipeline()
    p.add_source("sql", "vpps", "source", url=DATABASES[source])
    # TODO: dynamic lookup using FACT_DIMENSIONS
    p.add_source("sql", "source_dw_dim_loja", "source", url=DATABASES[target], table="dim_loja")
    p.add_source("sql", "fact_vpps", "target", url=DATABASES[target], table=fact_table)

    q_vpps = load(os.path.join(sqldir, 'vpps.sql'))
    q_dim_loja = load(os.path.join(lookupsdir, 'target_dim_loja_lookup.sql'))

    vpps = p.extract("vpps", params={'query': q_vpps, 'params': {'day': day}})
    dim_lojas = p.extract("source_dw_dim_loja", params={'query': q_dim_loja})

    vpps['idloja'] = vpps['idloja'].map(lambda x: get_sk(x, dim_lojas))

    p.data = vpps
    p.add_column('iddate', day.strftime(DIM_DATE_FMT))
    p.load("fact_vpps")

