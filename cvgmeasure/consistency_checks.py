import os

from plumbum import local
from plumbum.cmd import rm, mkdir, ls
from redis import StrictRedis

from cvgmeasure.conf import get_property
from cvgmeasure.common import job_decorator, mk_key
from cvgmeasure.d4 import d4, refresh_dir, add_to_path, checkout, test


class SubMismatch(Exception):
    pass

class LenMismatch(Exception):
    pass

class DupMismatch(Exception):
    pass

class TestFail(Exception):
    pass

def no_dups(it1, label1):
    if len(it1) != len(set(it1)):
        raise DupMismatch("There are duplicated in %s" % label1)

def check_sub(it1, label1, it2, label2):
    s2 = set(it2)
    diff = [item for item in it1 if item not in s2]
    if len(diff) > 0:
        missings_str = ' '.join(diff)
        raise SubMismatch("(%s) were in %s but missing from %s" % (missings_str, label1, label2))

def check_eq(it1, label1, it2, label2):
    check_sub(it1, label1, it2, label2)
    check_sub(it2, label2, it1, label1)


def with_fails(fun):
    try:
        fun()
    except Exception as e:
        return [e]
    return []

@job_decorator
def non_empty_match(input, hostname, pid):
    project = input['project']
    version = input['version']

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    r = StrictRedis.from_url(redis_url)
    key = mk_key('test-classes', [project, version])
    test_classes = r.llen(key) #r.lrange(key, 0, -1)


    cobertura, codecover, jmockit = [r.hkeys(mk_key('test-classes-cvg-nonempty', [tool, project, version]))
            for tool in ['cobertura', 'codecover', 'jmockit']]



    core = set(cobertura) & set(codecover) & set(jmockit)

    cobertura_, codecover_, jmockit_ = [[t for t in l if t not in core] for l in (cobertura, codecover, jmockit)]


    print test_classes
    print len(core)

    print "---"
    print len(cobertura_), cobertura_
    print len(codecover_), codecover_
    print len(jmockit_), jmockit_

    fails = []
    fails.extend(with_fails(lambda: check_sub(cobertura, 'cobertura', codecover, 'codecover')))
    fails.extend(with_fails(lambda: check_sub(codecover, 'codecover', cobertura, 'cobertura')))
    fails.extend(with_fails(lambda: check_sub(cobertura, 'cobertura', jmockit, 'jmockit')))
    fails.extend(with_fails(lambda: check_sub(jmockit, 'jmockit', cobertura, 'cobertura')))
    fails.extend(with_fails(lambda: check_sub(codecover, 'codecover', jmockit, 'jmockit')))
    fails.extend(with_fails(lambda: check_sub(jmockit, 'jmockit', codecover, 'codecover')))


    if fails:
        raise SubMismatch(' AND '.join([str(ex) for ex in fails]))


    return "Success"


