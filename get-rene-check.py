#! /usr/bin/env python
import sys
import json

from redis import StrictRedis
from cvgmeasure.conf import REDIS_URL_OUT
from cvgmeasure.d4 import iter_versions
from cvgmeasure.common import mk_key

from optparse import OptionParser

def main(options):
    r = StrictRedis.from_url(REDIS_URL_OUT)
    for project, v in iter_versions(restrict_project=options.restrict_project, restrict_version=options.restrict_version):
        reasons = []
        for qm in ['line', 'mutant', 'mutant-line']:
            key = mk_key('out', [qm, 'file', '0', 'B.F', project, v]) + ':info'
            info = json.loads(r.get(key))
            reasons.append(info[1]) 
        print '{project}:{v}'.format(**locals()), ' '.join(reasons)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append", default=[])
    parser.add_option("-v", "--version", dest="restrict_version", action="append", default=[])
    (options, args) = parser.parse_args(sys.argv)
    main(options)

