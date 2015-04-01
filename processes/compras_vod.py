import pandas as pd
from pdetl import Pipeline
from settings import load
from settings import DATABASES, DIM_DATE_FMT, FACT_DIMENSIONS
import os
from dimensions.loader import update_dimensions

sqldir = 'sqlfiles'
lookupsdir = os.path.join(sqldir, 'lookups')
col = ('iddim_filme', 'fact_count', 'genero')
col2 = ('iddim_filme', 'fact_count', 'categoria')

def get_sk(x, dim):
    try:
        return dim[(dim.id == x)].head(1).iloc[0]['sk']
    except LookupError, le:
        return 0
    except Exception, e:
        return -1


def get_genre(x, dim):
    try:
        return dim[(dim.sk == x)].head(1).iloc[0]['genero'].split(",")
    except LookupError, le:
        return ['error']
    except Exception, e:
        return ['error']


def get_categorias(x, dim):
    try:
        final_categories = []
        categories = dim[(dim.assetID == x)].head(1).iloc[0]['path'].split("|")
        for c in categories:
            c_tmp = c.split(":")
            final_categories.append(c_tmp[1].strip())
        return final_categories
    except LookupError, le:
        return ['error']
    except Exception, e:
        return ['error']


def add_to_df(df, movie, dim, key):
    gc = get_genre(movie, dim) if key == 'genero' else get_categorias(movie, dim)
    for g in gc:
        g = g.strip()
        try:
            if key == 'genero':
                i = df[(df.iddim_filme == movie) & (df.genero == g)].head(1).iloc[0].name
            else:
                i = df[(df.iddim_filme == movie) & (df.categoria == g)].head(1).iloc[0].name
            df.loc[i, 'fact_count'] += 1
        except LookupError, le:
            if key == 'genero':
                df.loc[len(df)] = {'iddim_filme': movie, 'fact_count': 1, 'genero': g}
            else:
                df.loc[len(df)] = {'iddim_filme': movie, 'fact_count': 1, 'categoria': g}


def run(fact_table, day, source, target):
    #update_dimensions(source, target, FACT_DIMENSIONS['compras_vod'])

    p = Pipeline()

    p.add_source("sql", "compras", "source", url=DATABASES[source])
    # TODO: dynamic lookup using FACT_DIMENSIONS
    p.add_source("sql", "source_dw_dim_cliente", "source", url=DATABASES[target])
    p.add_source("sql", "source_dw_dim_filme", "source", url=DATABASES[target])
    p.add_source("sql", "source_aaaa_path", "source", url=DATABASES["129-aaaa"])
    p.add_source("sql", "fact_vod_compras", "target", url=DATABASES[target], table="fact_vod_compras")
    p.add_source("sql", "fact_vod_genero", "target", url=DATABASES[target], table="fact_vod_genero")
    p.add_source("sql", "fact_vod_categoria", "target", url=DATABASES[target], table="fact_vod_categoria")

    q_compras = load(os.path.join(sqldir, 'compras_vod.sql'))
    q_dim_filme = load(os.path.join(lookupsdir, 'target_dim_filme_lookup.sql'))
    q_dim_cliente = load(os.path.join(lookupsdir, 'target_dim_cliente_lookup.sql'))
    q_movie_path = load(os.path.join(lookupsdir, 'source_movie_paths.sql'))

    compras = p.extract("compras", params={'query': q_compras, 'params': {'day': day}})
    dim_filme = p.extract("source_dw_dim_filme", params={'query': q_dim_filme})
    dim_cliente = p.extract("source_dw_dim_cliente", params={'query': q_dim_cliente})
    movie_path = p.extract("source_aaaa_path", params={'query': q_movie_path})

    compras['iddim_filme'] = compras['filme'].map(lambda x: get_sk(x, dim_filme))
    compras['iddim_cliente'] = compras['codigocliente'].map(lambda x: get_sk(x, dim_cliente))
    movie_path['assetID'] = movie_path['assetID'].map(lambda x: get_sk(x, dim_filme))

    if not compras.empty:
        # load compras fact
        del compras['codigocliente']
        del compras['filme']
        p.data = compras
        p.add_column('iddim_date', day.strftime(DIM_DATE_FMT))
        p.load("fact_vod_compras")

        # load genero fact
        g = pd.DataFrame(columns=col)
        for index, row in compras.iterrows():
            add_to_df(g, row['iddim_filme'], dim_filme, 'genero')
        p.data = g
        p.add_column('iddim_date', day.strftime(DIM_DATE_FMT))
        p.load("fact_vod_genero")

        # load categoria fact
        c = pd.DataFrame(columns=col2)
        for index, row in compras.iterrows():
            add_to_df(c, row['iddim_filme'], movie_path, 'categoria')
        p.data = c
        p.add_column('iddim_date', day.strftime(DIM_DATE_FMT))
        p.load("fact_vod_categoria")




