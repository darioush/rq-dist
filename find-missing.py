import json
import sys

from plumbum import LocalPath
from redis import StrictRedis

from cvgmeasure.conf import get_property


def main(project, version):
    tools = ['cobertura', 'codecover', 'jmockit']
    key = "%s:%d" % (project, version)
    r = StrictRedis.from_url(get_property('redis_url'))

    rkey =  ':'.join(['results', 'test-methods-agree-cvg-nonempty', key])
    tms = r.lrange(rkey, 0, -1)

    for tool in tools:
        missings = [tm for (fn, tm)  in
            [('/scratch/darioush/files/%s:%s:%s.tar.gz' % (tool, key, tm), tm)
                for tm in tms] if not LocalPath(fn).exists()]
        command = {
                'redo': True,
                'cvg_tool': tool,
                'test_methods': missings,
                'project': project,
                'version': version
            }
        if missings:
            print "python main.py q cvgmeasure.nocover_test_cases.test_cvg_methods -j '%s' -t 1800 -q localH --commit" % json.dumps(command)


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
