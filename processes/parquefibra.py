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
    pex.loc[:, 'demora'] = datetime.date.today() - pex.datafim
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
    filename = "parque_fibra_" + idtempo + ".h5"
    idtempo = int(idtempo)

    p = Pipeline()
    # staging
    p.add_source("hdf", "staging", "staging", path=stagingdir, filename=filename)
    # targets
    p.add_source("sql", "fct_parque_fibra", "target", url=DATABASES[target], table="fct_parque_fibra")
    p.add_source("sql", "fct_expirados_fibra", "target", url=DATABASES[target], table="fct_expirados_fibra")

    if p.datastore.staging.exists():
        p.extract("staging", save=True)
        print "Extract from staging, ", len(p.data)
    else:
        p.add_source("sql", "parque", "source", url=DATABASES[source])
        query = load(os.path.join(sqldir, 'parque_fibra.sql'))
        p.extract("parque", params={'query': query, 'params': {'day': dia}}, save=True)
        print "extract: ", len(p.data)

        p.to_datetime(["datainicio", "datafim", "dataanulacao"])
        p.add_column("idtempo", idtempo)
        p.load("staging")
        print "Save staging, ", p.datastore.staging.fullpath

    parque = p.data
    # ACTIVO
    p.data = parque_activo(parque, dia)
    p.add_column("idtempo", idtempo)

    r = p.clean("fct_parque_fibra", conditions=[("idtempo", "eq", idtempo)])
    print "clean fct_parque_fibra: ", r.rowcount

    p.load("fct_parque_fibra")
    print "load fct_parque_fibra: ", len(p.data)

    # EXPIRADO
    p.data = parque_exp(parque, dia)
    if not p.data.empty:
        p.add_column("idtempo", idtempo)

        r = p.clean("fct_expirados_fibra", conditions=[("idtempo", "eq", idtempo)])
        print "clean fct_expirado_fibra: ", r.rowcount

        p.load("fct_expirados_fibra")
        print "load fct_expirado_fibra: ", len(p.data)

    print "-------------------"
