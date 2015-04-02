#! /usr/bin/env python
import sys
import json

from plumbum import local
from redis import StrictRedis
from optparse import OptionParser

from cvgmeasure.conf import get_property
from cvgmeasure.d4 import iter_versions
from cvgmeasure.common import mk_key
from main import single_enqueue


def main():
    r = StrictRedis.from_url(get_property('redis_url'))
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append")
    parser.add_option("-v", "--version", dest="restrict_version", action="append")
    parser.add_option("-x", "--commit", dest="action", action="store_true", default=False)
    parser.add_option("-q", "--queue", dest="queue", action="store", default='default')

    (options, args) = parser.parse_args(sys.argv)

    #TOOLS  = ['cobertura', 'codecover', 'jmockit', 'major']
    SUITES = ['evosuite-weakmutation-fse.{i}'.format(i=i) for i in xrange(1,31)]
             # ['randoop.{i}'.format(i=i) for i in xrange(1,11)] + \
             #   ['evosuite-branch.{i}'.format(i=i) for i in xrange(0,10)]

    for suite in SUITES:
        for project, v in iter_versions(options.restrict_project, options.restrict_version):
            #for tool in TOOLS:
                #result = r.get(mk_key('fetch', [project, v, suite]))
                #if result == 'ok':
                single_enqueue('cvgmeasure.cvg.test_lists_gen', json.dumps({
                    "project": project,
                    "version": v,
                    "suite": suite,
                    "redo": True,
                    }), print_only=not options.action, timeout=1800, queue_name=options.queue)

if __name__ == "__main__":
    main()

