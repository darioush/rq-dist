#! /usr/bin/env python
import sys
import redis

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
        for bases in options.bases:
            for pools in options.pools:
                for project, v in iter_versions(restrict_project=options.restrict_project, restrict_version=options.restrict_version):
                    print "----( %s %d --  %s )----" % (project, v, qm)
                    minimization(r, rr, rrr, qm, project, v, bases.split('.'), pools.split('.'))
                    print

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append", default=[])
    parser.add_option("-v", "--version", dest="restrict_version", action="append", default=[])
    parser.add_option("-B", "--base", dest="bases", action="append", default=[])
    parser.add_option("-P", "--pool", dest="pools", action="append", default=[])
    parser.add_option("-M", "--metric", dest="qms", action="append", default=[])
    (options, args) = parser.parse_args(sys.argv)
    main(options)

