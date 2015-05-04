import pandas as pd
from pdetl import Pipeline
from settings import load
from settings import DATABASES, DIM_DATE_FMT, FACT_DIMENSIONS, MONGODB_COLLECTIONS
import os, re, time, datetime
from pymongo import MongoClient
from dimensions.etl_common import get_sk
from dimensions.loader import update_dimensions

sqldir = 'sqlfiles'
lookupsdir = os.path.join(sqldir, 'lookups')

def run(fact_table, day, source, target):
    update_dimensions(source, target, FACT_DIMENSIONS['tipi'])

    # TODO: check if ETL is to run only once a day

    db_client = MongoClient(DATABASES[source]['host'], DATABASES[source]['port'])
    db = db_client[DATABASES[source]['db']]
    collection = db[MONGODB_COLLECTIONS[source]['contactos']]

    # http://git.zapsi.net/zapccm/src/c1f1423b0ce6ca93f38e5d544f3da821588435bf/zapccm/templates/registo/chamada.html?at=master
    # 63    var timestamp = moment().format('YYMMDDHHmmss');
    start = str(day.year - 2000) + '{:02d}'.format(day.month) + '{:02d}'.format(day.day)
    end = start + "235959"
    start += "000000"

    # get contactos
    contacts = list(collection.find({
        "tipific": {
            "$elemMatch": {
                "timestamp": {
                    "$gt": start,
                    "$lt": end
                }
            }
        }
    }))

    p = Pipeline()
    p.add_source("sql", "tree", "source", url=DATABASES[target])
    p.add_source("sql", "time", "source", url=DATABASES[target])
    p.add_source("sql", "clients", "source", url=DATABASES[target])
    p.add_source("sql", "fact_tipi", "target", url=DATABASES[target], table="fact_tipi")

    q_tree = load(os.path.join(lookupsdir, 'target_dim_tipi_tree_lookup.sql'))
    q_time = load(os.path.join(lookupsdir, 'target_dim_time_lookup.sql'))
    q_clients = load(os.path.join(lookupsdir, 'target_dim_cliente_lookup.sql'))

    d_tree = p.extract("tree", params={'query': q_tree})
    d_time = p.extract("time", params={'query': q_time})
    #d_clients = p.extract("clients", params={'query': q_clients})
    d_clients = pd.DataFrame(columns=('sk', 'id'))

    tipied_contacts = pd.DataFrame([extract_contacts(c, start, end) for c in contacts])

    # lookup ids
    tipied_contacts.loc[:, "tipiint"] = tipied_contacts.tipi.astype(int)

    tipied_contacts['iddim_tipi_arvore'] = tipied_contacts['tipiint'].map(lambda x: get_sk(x, d_tree))
    tipied_contacts['iddim_time'] = tipied_contacts['time'].map(lambda x: get_sk(x, d_time))
    tipied_contacts['iddim_cliente'] = tipied_contacts['zw_client'].map(lambda x: get_sk(x, d_clients))

    # remove extra columns
    tipied_contacts.drop(['tipi', 'tipiint', 'time', 'zw_client'], inplace=True, axis=1)

    if not tipied_contacts.empty:
        print "Loading contacts..."
        p.data = tipied_contacts
        p.load("fact_tipi")


def extract_contacts(contact, start, end):
    # https://docs.python.org/2.7/glossary.html#term-eafp
    try:
        number = re.search('sip:(.+?)@', contact['info_origem']['numero']).group(1)
    except:
        number = 'sem numero'

    tc = {}

    for c in contact['tipific']:
        if start <= c['timestamp'] <= end:
            # hacking date SK
            tc.update({
                'number': number,
                'iddim_date': int(c['timestamp'][0:6]),
                'time': str(c['timestamp'][6:8]) + ":" + str(c['timestamp'][8:10]) + ":" + str(c['timestamp'][10:12]),
                'zw_client': contact['clientezw'],
                'agent': contact['info_origem']['agent'],
                'tipi': c['id']
            })

    return tc