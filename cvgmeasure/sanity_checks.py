import os

from plumbum import local
from plumbum.cmd import rm, mkdir, ls
from redis import StrictRedis

from cvgmeasure.conf import get_property
from cvgmeasure.common import job_decorator, mk_key
from cvgmeasure.d4 import d4, refresh_dir, add_to_path, checkout


class SubMismatch(Exception):
    pass

class LenMismatch(Exception):
    pass

class DupMismatch(Exception):
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

@job_decorator
def method_list_matches(input, hostname, pid):
    project = input['project']
    version = input['version']

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    r = StrictRedis.from_url(redis_url)
    key = mk_key('test-methods', [project, version])
    test_methods_from_redis = r.lrange(key, 0, -1)

    work_dir_path = local.path(work_dir) / ('child.%d' % os.getpid())

    with refresh_dir(work_dir_path, cleanup=True):
        with add_to_path(d4j_path):
            with checkout(project, version, work_dir_path / 'checkout'):
                d4()('compile')
                test_methods_from_d4 = d4()('list-tests').rstrip().split('\n')
                with local.env(SUCCESS_OUT="passing-tests.txt"):
                    d4()('test')
                    with open("passing-tests.txt") as f:
                        test_methods_from_run = [x[len('--- '):] for x in f.read().rstrip().split('\n')]
                    with open("count-of-tests.txt") as f:
                        per_run_counts = [int(line.rstrip()) for line in f]
                        print "Number of processes run to test: %d" % len(per_run_counts)
                        count_of_tests_from_run = sum(per_run_counts)

    # Sanity check #1 -- number of tests counted through the runner should equal
    #                    the length of the list of passing tests the runner outputs
    if len(test_methods_from_d4) != count_of_tests_from_run:
        raise LenMismatch("Test methods from d4 don't match counter")

    # Sanity check #2 -- we should not be running duplicate tests
    no_dups(test_methods_from_run, 'test methods from run')

    # Sanity check #3 -- we should not be list-outputting duplicate tests
    no_dups(test_methods_from_d4, 'test methods from d4')

    # Sanity check #4 -- we should not have duplicate tests in redis store
    no_dups(test_methods_from_redis, 'test methods from redis')

    # Sanity check #5 -- tests output from the runner should match the tests output from
    #                    d4 list-tests
    check_eq(test_methods_from_run, 'test methods from run', test_methods_from_d4, 'test methods from d4')

    # Sanity check #6 -- test methods from d4 should match ones in redis
    #
    #   Preprocess step: We know that these methods were wrongly inserted:
    lang_methods = [
        'org.apache.commons.lang3.EnumUtilsTest::test_processBitVectors_longClass',
        'org.apache.commons.lang3.builder.ReflectionToStringBuilderConcurrencyTest::testLinkedList',
        'org.apache.commons.lang3.builder.ReflectionToStringBuilderConcurrencyTest::testArrayList',
        'org.apache.commons.lang3.builder.ReflectionToStringBuilderConcurrencyTest::testCopyOnWriteArrayList',
        'org.apache.commons.lang3.builder.ReflectionToStringBuilderMutateInspectConcurrencyTest::testConcurrency',
    ]
    lang_methods_in_redis = [method for method in lang_methods if method in test_methods_from_redis]

    for lang_method in lang_methods_in_redis:
        print "Removing %s from redis:" % lang_method
        print r.lrem(key, 1, lang_method)

    if lang_methods_in_redis:
        print "Redis store was modified, reloading list before testing"
        test_methods_from_redis = r.lrange(key, 0, -1)

    check_eq(test_methods_from_redis, 'test methods from redis', test_methods_from_d4, 'test methods from d4')

