import os
from settings import load, DATABASES, DIMENSION_UPDATE_STATUS
from pdetl import Pipeline
import pandas as pd
import hashlib
import re
import datetime
from etl_common import get_df_row_for_values, load_dimension, dim_loaded

sql_dir = "dim_sql_files"
my_name = "dim_cliente"
canonical_source = "ZW"

def run(source, target):
    datestart = datetime.datetime.now()
    print "Starting etl_dim_cliente at", datestart

    # Fetch ZW clients
    p = Pipeline()
    p.add_source("sql", "zw_cliente", "source", url=DATABASES[source])

    # cuz Exception: Cannot extract from target stype
    p.add_source("sql", "source_dw_dim_cliente", "source", url=DATABASES[target])

    get_source_clientes = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'source_zw_clientes.sql')))
    get_dw_clientes = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'source_dwao_clientes.sql')))
    target_query = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'target_dw_insert_dim_cliente.sql')))
    target_update_query = load(os.path.join(os.path.dirname(__file__), os.path.join(sql_dir, 'target_dw_update_dim_cliente.sql')))

    print "Extracting clients. Elapsed:", datetime.datetime.now() - datestart
    clients = p.extract("zw_cliente", {'query': get_source_clientes})

    print "Extracting clients from DW. Elapsed:", datetime.datetime.now() - datestart
    # retrieve all dim_client hash
    dim_clients = p.extract("source_dw_dim_cliente", {'query': get_dw_clientes})

    print "Applying transformations and filtering clients. Elapsed:", datetime.datetime.now() - datestart
    nu_clients = pd.DataFrame(
        filter(lambda y: y['d_hash'] not in dim_clients['hash'].values,
               map(transform_and_hash_client,
               clients.codigocliente,
               clients.nome,
               clients.sexo,
               clients.datanascimento,
               clients.estadocivil,
               clients.telefone,
               clients.telemovel,
               clients.email,
               clients.provincia,
               clients.municipio)
               )
    )

    insert_variables = []
    update_variables = []
    status = DIMENSION_UPDATE_STATUS['success']
    details = ""

    print "Selecting for insert or update. Elapsed:", datetime.datetime.now() - datestart
    for index, row in nu_clients.iterrows():

        try:
            tlf = int(row['telefone'])
        except:
            tlf = 0
        try:
            tlm = int(row['telemovel'])
        except:
            tlm = 0

        r = get_df_row_for_values(dim_clients, row['codigocliente'], canonical_source)
        try:
            r = r.head(1)
            update_variables.append([
                row['nome'],
                row['provincia'],
                row['municipio'],
                row['sexo'],
                row['datanascimento'],
                row['estadocivil'],
                tlf,
                tlm,
                row['email'],
                int(r.iloc[0]['version'])+1,
                row['d_hash'],
                canonical_source,
                r.iloc[0]['codigocliente'],
                canonical_source
            ])
        except:
            insert_variables.append([
                row['codigocliente'],
                row['nome'],
                row['provincia'],
                row['municipio'],
                row['sexo'],
                row['datanascimento'],
                row['estadocivil'],
                tlf,
                tlm,
                row['email'],
                1,
                row['d_hash'],
                canonical_source
            ])

    if insert_variables:
        try:
            print "Inserting new clients. Elapsed:", datetime.datetime.now() - datestart
            load_dimension(insert_variables, target_query, target)
            details += str(len(insert_variables)) + " new clients.\n"
        except Exception, e:
            status = DIMENSION_UPDATE_STATUS['failure']
            details += "At insert\n" + str(e)
            details += "\n"
    if update_variables:
        try:
            print "Updating clients. Elapsed:", datetime.datetime.now() - datestart
            load_dimension(update_variables, target_update_query, target)
            details += str(len(update_variables)) + " clients updated.\n"
        except Exception, e:
            status = DIMENSION_UPDATE_STATUS['failure']
            details += "At update\n" + str(e)
            details += "\n"

    dateend = datetime.datetime.now()

    print "Dimension dim_cliente completed. Elapsed:", dateend - datestart
    d = dim_loaded(my_name, datestart, dateend, status, details)
    return d


# Transformations
def transform_and_hash_client(
        codigocliente,
        nome,
        sexo,
        datanascimento,
        estadocivil,
        telefone,
        telemovel,
        email,
        provincia,
        municipio):

    if sexo != 'F' and sexo != 'M':
        sexo = 'I'

    if estadocivil not in ["solteiro", "casado", "divorciado", "viuvo", "separado"]:
        estadocivil = "indeterminado"

    if datanascimento is None:
        datanascimento = "1800-01-01 00:00:00"

    if email is not None \
            and not re.match(
                "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
                email
            ):
        email = None

    d_hash = hashlib.sha1(('%s%s%s%s%s%s%s%s%s%s' % (
        codigocliente
        , nome
        , sexo
        , datanascimento
        , estadocivil
        , telefone
        , telemovel
        , email
        , provincia
        , municipio)).encode('utf-8')
    )

    return {
        'codigocliente': codigocliente,
        'nome': nome,
        'sexo': sexo,
        'datanascimento': datanascimento,
        'estadocivil': estadocivil,
        'telefone': telefone,
        'telemovel': telemovel,
        'email': email,
        'provincia': provincia,
        'municipio': municipio,
        'd_hash': d_hash.hexdigest()
    }