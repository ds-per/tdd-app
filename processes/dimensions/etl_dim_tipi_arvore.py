import os
from settings import load, DATABASES, DIMENSION_UPDATE_STATUS, MONGODB_COLLECTIONS
from pdetl import Pipeline
import pandas as pd
import hashlib
import re
import datetime
import collections
from pymongo import MongoClient
from etl_common import get_df_row_for_values, load_dimension, dim_loaded

sql_dir = "dim_sql_files"
my_name = "dim_tipi_arvore"
canonical_source = "ZW"
dim_clients = None
cpn = collections.namedtuple('cpn', ['tlf', 'tlm'])
count = 0
count_update = 0

def run(source, target):
    datestart = datetime.datetime.now()
    print "Starting dim_tipi_arvore at", datestart

    db_client = MongoClient(DATABASES[source]['host'], DATABASES[source]['port'])
    db = db_client[DATABASES[source]['db']]
    collection = db[MONGODB_COLLECTIONS[source]['arvore']]

    get_source_dw_tipi_a = load(os.path.join(
        os.path.dirname(__file__),
        os.path.join(sql_dir, 'source_dw_tipi_arvore.sql')
    ))

    # get tree from source
    tree_source = list(collection.find())

    # Fetch tree from dw
    p = Pipeline()
    p.add_source("sql", "dw_tipi_a", "source", url=DATABASES[target])

    tree_dw = p.extract("dw_tipi_a", {'query': get_source_dw_tipi_a})

    # prepare return states
    status = DIMENSION_UPDATE_STATUS['success']
    details = ""

    # assuming parents and children in order (a parent as a lower id value than it's children)
    children = {}
    [children.update({c: e['_id']}) for e in tree_source for c in e.get('childs', [])]

    new_nodes = []
    update_nodes = []
    error_new_nodes = []

    target_query_insert = load(
        os.path.join(
            os.path.dirname(__file__),
            os.path.join(sql_dir, 'target_dw_insert_tipi_arvore.sql')
        )
    )

    target_query_update = load(
        os.path.join(
            os.path.dirname(__file__),
            os.path.join(sql_dir, 'target_dw_update_tipi_arvore.sql')
        )
    )

    # for each node check it's state in DW
    for elem in tree_source:
        elem_dw = tree_dw[tree_dw.id == elem['_id']]
        groups = ",".join(elem.get('grupos', []))

        if elem_dw.empty:
            # insert in db and retrieve dim key

            # check if node has a parent
            try:
                t = tree_dw[tree_dw.id == children[elem['_id']]]
                # TODO: check. Possible bug introduced when children not in order (at least for first insert)
                parent = None\
                    if elem['_id'] not in children.keys() and t.empty else t.iloc[0]['iddim_tipi_arvore']
            except:
                parent = None

            node = [
                int(elem['_id']),
                elem['nome'],
                int(elem['state']),
                groups,
                int(parent) if parent is not None else parent
            ]

            # insert node in DB
            # inserting a node at a time to get the new SK of the DW to add it to it's children
            try:
                nid = load_dimension(node,
                                     target_query_insert,
                                     target,
                                     many=False,
                                     is_insert_stmt=True)
                node.insert(0, nid)
                new_nodes.append(node)

                # update dw tree has it is now in DB to avoid fetching a new tree
                tree_dw.loc[len(tree_dw)] = {
                    'iddim_tipi_arvore': nid,
                    'id': elem['_id'],
                    'nome': elem.get('grupos', []),
                    'state': elem['nome'],
                    'grupos': elem['state'],
                    'parent': parent
                }
            except Exception, e:
                """
                    TODO: check later how to catch this exception in order to not affect tree structure
                    On error should exit to avoid damaging the rest of the ETL process
                    For now just finish execution throwing an exception
                """
                raise e
                # status = DIMENSION_UPDATE_STATUS['failure']
                # error_new_nodes.append({node[0]: str(e)})

        else:
            """
                If node already exists, update it if needed
                This time, since we already have the SK from DW, we can do this operation in one go, adding it to
                an array to update all later
            """
            if elem_dw['nome'].item() != elem['nome']\
                    or elem_dw['grupos'].item() != groups\
                    or elem_dw['state'].item() != elem['state']:

                # TODO: later, check if parent has changed (will this ever happen?)
                update_nodes.append([
                    elem['_id'],
                    elem['nome'],
                    elem['state'],
                    groups,
                    elem_dw['parent'],
                    elem_dw['iddim_tipi_arvore'],
                    elem['_id'],
                ])

    # logging to DB
    if new_nodes:
        details += str(len(new_nodes)) + " new nodes.\n"

    if error_new_nodes:
        details += str(len(error_new_nodes)) + " nodes failed.\n"
        details += "Failing nodes:\n" + [str(key) + " : " + value + "\n" for key, value in error_new_nodes]

    if update_nodes:
        try:
            load_dimension(node, target_query_update, target)
            details += str(len(update_nodes)) + " updated nodes.\n"
        except Exception, e:
            status = DIMENSION_UPDATE_STATUS['failure']
            details += "At update\n" + str(e)
            details += "\n"

    dateend = datetime.datetime.now()

    print "Dimension dim_tipi_arvore completed. Elapsed:", dateend - datestart
    d = dim_loaded(my_name, datestart, dateend, status, details)
    return d