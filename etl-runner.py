#!/usr/bin/env python2.7
import argparse
from pdetl import Pipeline
from pdetl import sql
from settings import DATABASES
import datetime
import os

sqldir = 'sqlfiles'

def run(process, fact_table, dia, source, target):
    print "start ", dia
    idtempo = "{}".format(dia.strftime("%Y%m%d"))
    p = Pipeline()
    p.add_source(DATABASES[source], 'zapweb')
    query = sql.load(os.path.join(sqldir, '{}.sql'.format(process)))
    p.extract(query, day=dia)
    # p.transform(funcao, args)
    p.add_target(DATABASES[target], 'zapweb', fact_table)
    r = p.clean(conditions=[("idtempo", "eq", idtempo)])
    print "clean: ", r.rowcount
    p.load()
    print "load: ", len(p.data)
    print "-------------------"



def daterange(start, end):
    for n in range(int((end - start).days + 1)):
        yield start + datetime.timedelta(days=n)


def mkdate(datestring):
    return datetime.datetime.strptime(datestring, '%Y-%m-%d').date()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Etl runner script")
    parser.add_argument('-s', '--start', help="Start date")
    parser.add_argument('-e', '--end', help="End date")
    parser.add_argument('-p', '--process', help="Process to run")
    parser.add_argument('-f', '--fact', help="fact_table to load")
    parser.add_argument('-c', '--source', help="source from settings")
    parser.add_argument('-t', '--target', help="target from settings")
    parser.add_argument('-n', '--now', action="count", help="Runs for yesterday")
    args = parser.parse_args()

    if not args.process and not args.fact \
            and not args.source and not args.target \
            and (not args.now and not (args.start and args.end)):
        parser.error("--process and --fact are required,"
                     "--now or --start and --end must also be given")

    if args.now:
        d = datetime.date.today() - datetime.timedelta(1)
        run(args.process, args.fact, d, args.source, args.target)
    else:
        for dia in daterange(mkdate(args.start), mkdate(args.end)):
            run(args.process, args.fact, dia, args.source, args.target)
