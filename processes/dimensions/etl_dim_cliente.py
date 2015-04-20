import os
from settings import load, DATABASES, DIMENSION_UPDATE_STATUS
from pdetl import Pipeline
import pandas as pd
import hashlib
import re
import datetime
import collections
from etl_common import get_df_row_for_values, load_dimension, dim_loaded

sql_dir = "dim_sql_files"
my_name = "dim_cliente"
canonical_source = "ZW"
dim_clients = None
cpn = collections.namedtuple('cpn', ['tlf', 'tlm'])
count = 0
count_update = 0


def run(source, target):
    datestart = datetime.datetime.now()
    print "Starting etl_dim_cliente at", datestart

    # Fetch ZW clients
    p = Pipeline()
    p.add_source("sql", "zw_cliente", "source", url=DATABASES[source])

    # cuz Exception: Cannot extract from target stype
    p.add_source("sql", "source_dw_dim_cliente", "source", url=DATABASES[target])

    get_source_clientes = load(os.path.join(os.path.dirname(__file__),
                                            os.path.join(sql_dir, 'source_zw_clientes.sql')))
    get_dw_clientes = load(os.path.join(os.path.dirname(__file__),
                                        os.path.join(sql_dir, 'source_dwao_clientes.sql')))
    target_query = load(os.path.join(os.path.dirname(__file__),
                                     os.path.join(sql_dir, 'target_dw_insert_dim_cliente.sql')))
    target_update_query = load(os.path.join(os.path.dirname(__file__),
                                            os.path.join(sql_dir, 'target_dw_update_dim_cliente.sql')))

    print "Extracting clients. Elapsed:", datetime.datetime.now() - datestart
    clients = p.extract("zw_cliente", {'query': get_source_clientes})

    print "Extracting clients from DW. Elapsed:", datetime.datetime.now() - datestart
    # retrieve all dim_client hash
    global dim_clients
    dim_clients = p.extract("source_dw_dim_cliente", {'query': get_dw_clientes, 'params': {'source': canonical_source}})

    print "Applying transformations and filtering clients. Elapsed:", datetime.datetime.now() - datestart
    clients = pd.DataFrame(
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
    nu_clients = clients[~clients.d_hash.isin(dim_clients['hash'])]

    status = DIMENSION_UPDATE_STATUS['success']
    details = ""

    print "Selecting for insert or update. Elapsed:", datetime.datetime.now() - datestart
    nu_insert_clients = nu_clients[~nu_clients.codigocliente.isin(dim_clients['codigocliente'])]

    insert_variables = map(get_new_clients,
                           nu_insert_clients.codigocliente,
                           nu_insert_clients.nome,
                           nu_insert_clients.provincia,
                           nu_insert_clients.municipio,
                           nu_insert_clients.sexo,
                           nu_insert_clients.datanascimento,
                           nu_insert_clients.estadocivil,
                           nu_insert_clients.telefone,
                           nu_insert_clients.telemovel,
                           nu_insert_clients.email,
                           nu_insert_clients.d_hash
                           )

    print "Updates now"
    nu_update_clients = nu_clients[nu_clients.codigocliente.isin(dim_clients['codigocliente'])]

    # new column codigocliente as int for faster search
    dim_clients.loc[:, "codigoclienteint"] = dim_clients.codigocliente.astype(int)
    update_variables = map(get_clients_update,
                           nu_update_clients.codigocliente,
                           nu_update_clients.nome,
                           nu_update_clients.provincia,
                           nu_update_clients.municipio,
                           nu_update_clients.sexo,
                           nu_update_clients.datanascimento,
                           nu_update_clients.estadocivil,
                           nu_update_clients.telefone,
                           nu_update_clients.telemovel,
                           nu_update_clients.email,
                           nu_update_clients.d_hash
                           )

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


def get_new_clients(
        codigocliente,
        nome,
        provincia,
        municipio,
        sexo,
        datanascimento,
        estadocivil,
        telefone,
        telemovel,
        email,
        d_hash):

    if codigocliente is None:
        return

    phones = clean_phone_numbers(telefone, telemovel)
    global count
    count += 1
    print count, codigocliente

    return [
        codigocliente,
        nome,
        provincia,
        municipio,
        sexo,
        datanascimento,
        estadocivil,
        phones.tlf,
        phones.tlm,
        email,
        1,
        d_hash,
        canonical_source
    ]


def get_clients_update(
        codigocliente,
        nome,
        provincia,
        municipio,
        sexo,
        datanascimento,
        estadocivil,
        telefone,
        telemovel,
        email,
        d_hash):

    if codigocliente is None:
        return

    global count_update
    count_update += 1
    print count_update, codigocliente

    r = dim_clients[dim_clients.codigoclienteint == int(codigocliente)]
    r = r.head(1)

    phones = clean_phone_numbers(telefone, telemovel)

    return [
        nome,
        provincia,
        municipio,
        sexo,
        datanascimento,
        estadocivil,
        phones.tlf,
        phones.tlm,
        email,
        int(r.iloc[0]['version'])+1,
        d_hash,
        canonical_source,
        r.iloc[0]['codigocliente'],
        canonical_source
    ]


def clean_phone_numbers(tlf, tlm):
    try:
        tlf = int(tlf)
    except:
        tlf = 0
    try:
        tlm = int(tlm)
    except:
        tlm = 0

    return cpn(tlf, tlm)