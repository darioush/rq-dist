#! /usr/bin/env python
import sys
import json

from redis import StrictRedis
from optparse import OptionParser
from itertools import groupby

from cvgmeasure.conf import REDIS_URL_OUT
from cvgmeasure.d4 import iter_versions
from cvgmeasure.common import chunks

from sqlalchemy import create_engine, MetaData, Table, bindparam
from sqlalchemy.orm import sessionmaker

Session = None
def connect_db():
    global Session
#    engine = create_engine('sqlite:///result.db')
    engine = create_engine('mysql://root@127.0.0.1:3306/results')
    metadata = MetaData(engine)
    testselection = Table('testselection', metadata, autoload=True)
    testselection_ri = Table('testselection_ri', metadata, autoload=True)

    Session = sessionmaker(bind=engine)

    return (testselection, testselection_ri)

def save_rows(table, vals):
    keys = iter(table.indexes).next().columns

    for chunk in chunks(vals, 50):
        ins_result = table.insert().prefix_with(" IGNORE ").values(chunk).execute()
        result = table.update().where(reduce(lambda a,b: a&b, [col==bindparam('_' + col.name) for col in keys])).values(
                **{col.name:bindparam('_' + col.name) for col in table.columns if col.name not in keys and col.name != 'key'}).execute(
                        [{'_'+k:v for k,v in val.iteritems()} for val in chunk])

def sel(table, key):
    keys = iter(table.indexes).next().columns
    result = table.select().where(reduce(lambda a,b: a&b, [col==bindparam('_'+col.name) for col in keys])).execute(
            **{'_'+k:v for k,v in key.iteritems()})
    fr = result.fetchall()
    return len(fr)

def main(options):
    ts, ts_ri = connect_db()
    r = StrictRedis.from_url(REDIS_URL_OUT)
    for qm in options.qms:
        for granularity in options.grans:
            for experiment in options.experiments:
                bases, _, pools = experiment.partition(',')
                for project, v in iter_versions(restrict_project=options.restrict_project, restrict_version=options.restrict_version):
                    bases_srtd = '.'.join(sorted(bases.split('.')))
                    pools_srtd = '.'.join(sorted(pools.split('.')))
                    fmt = '{qm}:{granularity}:{bases_srtd}:{pools_srtd}:{project}:{v}'.format(**locals())
                    if options.skip:
                        print '{fmt}...'.format(fmt=fmt)
                        exists = sel(ts_ri, {'qm':qm, 'granularity': granularity, 'project': project, 'version': v,
                            'base':bases_srtd, 'select_from':pools_srtd })
                        if exists > 0:
                            print "SKIP"
                            continue
                    session = Session()
                    runs = {k: json.loads(v) for (k, v) in r.hgetall('out:{fmt}'.format(fmt=fmt)).iteritems()}
                    keys = sorted([(alg, int(x)) for (alg, x) in [tuple(key.split(':')) for key in runs.keys()]])
                    rows = []
                    fixed_info = {
                                    'qm': qm, 'granularity': granularity,
                                    'project': project, 'version': v, 'base': bases_srtd, 'select_from': pools_srtd,
                            }
                    suite_schema = lambda x: ['{0}_{1}'.format(x,i) for i in ('triggers', 'count', 'tgs', 'time')]
                    schema = [ 'relevant_tgs', 'determined_by',] + \
                            suite_schema('base') + suite_schema('base_relevant') + suite_schema('aug') + suite_schema('aug_relevant') \
                            + suite_schema('aug_additional')
                    ri_val = dict(zip(schema, json.loads(r.get('out:{fmt}:info'.format(fmt=fmt)))))
                    for algo, it in groupby(keys, key=lambda (a,b):a):
                        for algo, run_id in it:
                            uniq_key = '{fmt}:{algo}:{run_id}'.format(**locals())
                            run_result = runs['{0}:{1}'.format(algo, run_id)]
                            row_info = {
                                    'algorithm': algo, 'run_id': run_id,
                                    'fault_detection':  run_result[0], 'determined_by':  run_result[1],
                                    'selected_triggers': run_result[2], 'selected_count': run_result[3],
                                    'selected_tgs': run_result[4], 'selected_time': run_result[5],
                                }
                            row_info.update(fixed_info)
                            rows.append(row_info)
                    save_rows(ts, rows)

                    ri_val.update(fixed_info)
                    save_rows(ts_ri, [ri_val])
                    print '{0}: {1} records'.format(fmt, len(rows))
                    session.commit()



if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append", default=[])
    parser.add_option("-v", "--version", dest="restrict_version", action="append", default=[])
    parser.add_option("-x", "--experiments", dest="experiments", action="append", default=[])
    parser.add_option("-M", "--metric", dest="qms", action="append", default=[])
    parser.add_option("-G", "--granularities", dest="grans", action="append", default=[])
    parser.add_option("-s", "--skip", dest="skip", action="store_true", default=False)
    (options, args) = parser.parse_args(sys.argv)
    main(options)

