#! /usr/bin/env python
import sys
import redis
import json

from optparse import OptionParser

from cvgmeasure.analyze import minimization
from cvgmeasure.conf import get_property, REDIS_URL_TG, REDIS_URL_OUT
from cvgmeasure.d4 import iter_versions

def connect_db():
    return 
def main(options):
    r = redis.StrictRedis.from_url(get_property('redis_url'))
    rr = redis.StrictRedis.from_url(REDIS_URL_TG)
    rrr = redis.StrictRedis.from_url(REDIS_URL_OUT)

    for qm in options.qms:
        for gran in options.grans:
            for experiment in options.experiments:
                bases, _, pools = experiment.partition(',')
                if options.print_only:
                    print '''./main.py qb cvgmeasure.select.m {project} {version} -j '{json}' {additional}'''.format(
                            project=''.join('-p {0}'.format(rp) for rp in options.restrict_project),
                            version=''.join('-v {0}'.format(rv) for rv in options.restrict_version),
                            json=json.dumps({'granularity': gran, 'bases': bases, 'pools': pools, "qm": qm}),
                            additional=options.print_only)
                else:
                    for project, v in iter_versions(restrict_project=options.restrict_project, restrict_version=options.restrict_version):
                        print "----( %s %d --  %s : %s)----" % (project, v, qm, gran)
                        minimization(r, rr, rrr, qm, gran, project, v, bases.split('.'), pools.split('.'))
                        print

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append", default=[])
    parser.add_option("-v", "--version", dest="restrict_version", action="append", default=[])
    parser.add_option("-B", "--base", dest="bases", action="append", default=[])
    parser.add_option("-P", "--pool", dest="pools", action="append", default=[])
    parser.add_option("-M", "--metric", dest="qms", action="append", default=[])
    parser.add_option("-G", "--granularity", dest="grans", action="append", default=[])
    parser.add_option("-x", "--experiments", dest="experiments", action="append", default=[])
    parser.add_option("-c", "--print-enqueue-cmds", dest="print_only", action="store", type="string", default=None)

    (options, args) = parser.parse_args(sys.argv)
    main(options)

