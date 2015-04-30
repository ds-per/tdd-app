from pdetl import Pipeline
from settings import load
from settings import DATABASES, CONSUMOS_CLASS
import datetime
import os
import pandas as pd
import numpy as np

sqldir = 'sqlfiles'
stagingdir = 'staging'


def get_equipement(df):
    if len(df) == 1:
        return df.index[0]
    act = df[df.dataanulacao.isnull()]
    size = len(act.index)
    if size == 1:
        return act.index[0]
    elif size > 1:
        return act[act.idequipamento == act.idequipamento.max()].index[0]
    else:
        return df[df.idequipamento == df.idequipamento.max()].index[0]


def parque_activo(df, data):

    activas = (df.datainicio <= data) & (df.datafim >= data)
    # expiradas = df.datafim < data
    nanuladas = (df.dataanulacao.isnull()) | (df.dataanulacao > data)
    pa = df[activas & (df.contahotel == 0)]
    ph = df[activas & (df.contahotel == 1) & nanuladas]
    # pex = df[expiradas & nanuladas & (df.contahotel == 0)]

    # Contas
    pa1 = pa[['idcontaservico', 'idequipamento', 'dataanulacao']]
    idx = pa1.groupby(['idcontaservico'], sort=False).apply(get_equipement)
    pa2 = pa.loc[idx.values]
    pa2.loc[:, 'fact_count'] = 1

    # Hoteis
    ph1 = ph.groupby(['idcontaservico']).agg({'numeroserie': pd.Series.nunique})
    ph1.rename(columns={"numeroserie": "fact_count"}, inplace=True)
    ph2 = pd.merge(ph, ph1, left_on='idcontaservico', right_index=True)
    ph2.drop_duplicates('idcontaservico', inplace=True)

    # parque
    parque = pd.concat([ph2, pa2])
    cols = ['provincia', 'municipio', 'produto', 'modelomaterial']
    grouped = parque.groupby(cols, as_index=False).agg({"fact_count": np.sum})
    grouped.modelomaterial = grouped.modelomaterial.astype(np.int64)

    return grouped


def parque_exp(df, data):
    intervals = [-1, 7, 15, 30, 60, 90, 120, 180, 270, 360, 540, np.inf]
    labels = ['[000,007]', '[008,015]', '[015,030]', '[031,060]', '[061,090]',
              '[091,120]', '[121,180]', '[181,270]', '[271,360]', '[361,540]',
              '[541,...]']

    expiradas = df.datafim < data
    nanuladas = (df.dataanulacao.isnull()) | (df.dataanulacao > data)
    pex = df[expiradas & nanuladas & (df.contahotel == 0)]
    pex.loc[:, 'demora'] = data - pex.datafim
    pex.loc[:, 'demora'] = pex.demora.astype('timedelta64[D]')
    pex.loc[:, 'tempoexp'] = pd.cut(pex['demora'], intervals, labels=labels, right=True)

    cols = ['provincia', 'municipio', 'produto', 'modelomaterial', 'tempoexp']
    grouped = pex.groupby(cols, sort=False, as_index=False).agg(
        {'idcontaservico': pd.Series.nunique})
    grouped.modelomaterial = grouped.modelomaterial.astype(int)
    grouped.rename(columns={"idcontaservico": "fact_count"}, inplace=True)
    return grouped


def run(fact_table, dia, source, target):
    print "start ", dia

    idtempo = "{}".format(dia.strftime("%Y%m%d"))
    if "mz" in source:
        filename = "parqueMZ_" + idtempo + ".h5"
    else:
        filename = "parque_" + idtempo + ".h5"
    idtempo = int(idtempo)

    p = Pipeline()
    p.add_source("hdf", "staging", "staging", path=stagingdir, filename=filename)
    p.add_source("sql", "fct_parque", "target", url=DATABASES[target], table="fct_parque")
    p.add_source("sql", "fct_expirados", "target", url=DATABASES[target], table="fct_expirados")
    p.add_source("sql", "parque_activo", "source", url=DATABASES[source])

    query = load(os.path.join(sqldir, 'parque_activo.sql'))
    p.extract("parque_activo", params={'query': query, 'params': {'day': dia}}, save=True)
    print "extract activo: ", len(p.data)

    p.to_datetime(["datainicio", "datafim", "dataanulacao"])
    p.add_column("idtempo", idtempo)
    # ACTIVO
    p.data = parque_activo(p.data, dia)
    p.add_column("idtempo", idtempo)

    r = p.clean("fct_parque", conditions=[("idtempo", "eq", idtempo)])
    print "clean fct_parque: ", r.rowcount

    p.load("fct_parque")
    print "load fct_parque: ", len(p.data)


    if p.datastore.staging.exists():
        p.extract("staging", save=True)
        print "Extract from staging, ", len(p.data)
    else:
        p.add_source("sql", "parque", "source", url=DATABASES[source])
        query = load(os.path.join(sqldir, 'parque.sql'))
        p.extract("parque", params={'query': query, 'params': {'day': dia}}, save=True)
        print "extract: ", len(p.data)

        p.to_datetime(["datainicio", "datafim", "dataanulacao"])
        p.add_column("idtempo", idtempo)
        p.load("staging")
        print "Save staging, ", p.datastore.staging.fullpath

    parque = p.data

    # EXPIRADO
    p.data = parque_exp(parque, dia)
    p.add_column("idtempo", idtempo)

    r = p.clean("fct_expirados", conditions=[("idtempo", "eq", idtempo)])
    print "clean fct_expirado: ", r.rowcount

    p.load("fct_expirados")
    print "load fct_expirado: ", len(p.data)

    print "-------------------"
