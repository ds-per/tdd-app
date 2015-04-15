import os
from settings import load, DATABASES, DIMENSION_UPDATE_STATUS
from pdetl import Pipeline
import pandas as pd
import hashlib
import datetime
from etl_common import load_dimension, dim_loaded

sql_dir = "dim_sql_files"
my_name = "dim_loja"


def run(source, target):
    datestart = datetime.datetime.now()
    print "Starting etl_dim_loja at", datestart

    p = Pipeline()
    p.add_source("sql", "source_zw_lojas", "source", url=DATABASES[source], table="loja")

    p.add_source("sql", "source_dw_dim_loja", "source", url=DATABASES[target], table="dim_loja")
    p.add_source("sql", "target_dw_dim_loja", "target", url=DATABASES[target], table="dim_loja")

    get_source_lojas = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'source_zw_lojas.sql')))
    get_dw_lojas = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'source_dw_dim_loja.sql')))
    target_query = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'target_dw_dim_loja.sql')))
    target_update_query = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'target_dw_update_dim_loja.sql')))

    lojas = p.extract("source_zw_lojas", {'query': get_source_lojas})

    lojas = pd.DataFrame(
        map(transform_and_hash_loja,
            lojas.idloja,
            lojas.nome,
            lojas.estrutura,
            lojas.estado,
            lojas.nome_provincia,
            lojas.nome_municipio,
            lojas.codigo,
            lojas.parceiro)
    )

    # retrieve all dim_loja hash
    dim_lojas = p.extract("source_dw_dim_loja", {'query': get_dw_lojas})

    insert_variables = []
    update_variables = []
    status = DIMENSION_UPDATE_STATUS['success']
    details = ""

    for index, row in lojas.iterrows():

        if row['d_hash'] not in dim_lojas['hash'].values:
            if row['idloja'] not in dim_lojas['idloja'].values:
                insert_variables.append([
                    int(row['idloja'])
                    , row['nome']
                    , row['estrutura']
                    , row['nome_provincia']
                    , row['nome_municipio']
                    , row['codigo']
                    , row['parceiro']
                    , row['estado']
                    , row['d_hash']
                    , 1
                ])
            else:
                r = dim_lojas[(dim_lojas.idloja == row['idloja'])]
                r = r.head(1)
                update_variables.append([
                    row['nome']
                    , row['estrutura']
                    , row['nome_provincia']
                    , row['nome_municipio']
                    , row['codigo']
                    , row['parceiro']
                    , row['estado']
                    , row['d_hash']
                    , int(r.iloc[0]['version'])+1
                    , int(row['idloja'])
                ])

    if insert_variables:
        try:
            load_dimension(insert_variables, target_query, target)
            details += str(len(insert_variables)) + " new stores.\n"
        except Exception, e:
            status = DIMENSION_UPDATE_STATUS['failure']
            details += "At insert\n" + str(e)
            details += "\n"
    if update_variables:
        try:
            load_dimension(update_variables, target_update_query, target)
            details += str(len(insert_variables)) + " stores updated.\n"
        except Exception, e:
            status = DIMENSION_UPDATE_STATUS['failure']
            details += "At update\n" + str(e)
            details += "\n"

    dateend = datetime.datetime.now()

    d = dim_loaded(my_name, datestart, dateend, status, details)
    return d

def transform_and_hash_loja(
        idloja,
        nome,
        estrutura,
        estado,
        nome_provincia,
        nome_municipio,
        codigo,
        parceiro):

    if estrutura not in ['ZLOJ', 'ZBKO', 'ZVIA', 'ZPAG', 'ZAGE', 'ZOCA', 'ZACO', 'ZVIR']:
        estrutura = 'NA'

    if nome_provincia is None:
        nome_provincia = ''

    if nome_municipio is None:
        nome_municipio = ''

    d_hash = hashlib.sha1(('%s%s%s%s%s%s%s%s' % (
        idloja,
        nome,
        estrutura,
        estado,
        nome_provincia,
        nome_municipio,
        codigo,
        parceiro)).encode('utf-8')
    )

    return {
        'idloja': idloja,
        'nome': nome,
        'estrutura': estrutura,
        'estado': estado,
        'nome_provincia': nome_provincia,
        'nome_municipio': nome_municipio,
        'codigo': codigo,
        'parceiro': parceiro,
        'd_hash': d_hash.hexdigest()
    }