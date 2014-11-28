from pdetl import Pipeline
from pdetl import sql
from settings import DATABASES
import datetime
import os

sqldir = 'sqlfiles'
d = datetime.date(2014, 1, 1)
idtempo = "{}".format(d.strftime("%Y%m%d"))

p = Pipeline()
p.add_source(DATABASES['128-zapweb'], 'zapweb')
parque = sql.load(os.path.join(sqldir, 'parque.sql'))
p.extract(parque, day=d)
#p.transform(funcao, args)
p.add_target(DATABASES['129-dwao'], 'dwao', 'fct_parque')
r = p.clean(conditions=[("idtempo", "eq", idtempo)])
import IPython; IPython.embed()
print r.rowcount
p.load()

