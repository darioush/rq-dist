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
    print work_dir_path

    with refresh_dir(work_dir_path, cleanup=True):
        with add_to_path(d4j_path):
            with checkout(project, version, work_dir_path / 'checkout'):
                d4()('compile')
                test_methods_from_d4 = d4()('list-tests').rstrip().split('\n')
                with local.env(SUCCESS_OUT="passing-tests.txt"):
                    failing_tests = test()

                    with open("passing-tests.txt") as f:
                        test_methods_from_run = [x[len('--- '):] for x in f.read().rstrip().split('\n')]
                    with open("count-of-tests.txt") as f:
                        per_run_counts = [int(line.rstrip()) for line in f]
                        count_of_tests_from_run = sum(per_run_counts)


                if project == 'Lang' and version >= 37:
                    ## In this case, we know that some tests may fail
                    ## this is really ugly, but I'm doing it.
                    klass_name = 'org.apache.commons.%s.builder.ToStringBuilderTest' % (
                            'lang' if version > 39 else 'lang3',
                    )

                    expected_fails = [method for method in failing_tests if method.startswith(klass_name)]
                    single_run_fails = test(['-t', klass_name])
                    if len(single_run_fails) > 0:
                        raise TestFail('Single run failed: ' + ' '.join(single_run_fails))
                elif project == 'Time':
                    ## In this case, org.joda.time.chrono.gj.MainTest
                    ## isn't really a jUnit test because it doesn't have a public
                    ## constructor. We fix this during run by replacing it
                    ## with two classes with a public constructor, each of which
                    ## initializes the original class with parameters used during
                    ## testing

                    bad_class = 'org.joda.time.chrono.gj.MainTest'
                    good_class1, good_class2 = ['edu.washington.cs.testfixer.time.GjMainTest' + s for s in ('1', '2')]
                    tname = '::testChronology'
                    tcs = [tc for tc, _, _ in [method.partition('::') for method in test_methods_from_run]]
                    idx = tcs.index(bad_class)
                    test_methods_from_run[idx]   = good_class1 + tname
                    test_methods_from_run[idx+1] = good_class2 + tname

                    tcsd4 = [tc for tc, _, _ in [method.partition('::') for method in test_methods_from_d4]]
                    idxd4 = tcsd4.index(bad_class)
                    test_methods_from_d4 = test_methods_from_d4[:idxd4] + [good_class1 + tname,
                            good_class2 + tname] + test_methods_from_d4[idxd4+1:]

                else:
                    expected_fails = []

                unexpected_fails = [method for method in failing_tests if method not in expected_fails]

        # Sanity check #0 -- check out the test fails
        if len(unexpected_fails) > 0:
            raise TestFail(' '.join(unexpected_fails))

        # Sanity check #1 -- number of tests counted through the runner should equal
        #                    the length of the list of passing tests the runner outputs
        num_tests = len(test_methods_from_d4)
        if num_tests != count_of_tests_from_run:
            raise LenMismatch("Test methods from d4 (%d) don't match counter (%d)" %
                    (num_tests, count_of_tests_from_run))

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
        #lang_methods = [
        #    'org.apache.commons.lang3.EnumUtilsTest::test_processBitVectors_longClass',
        #    'org.apache.commons.lang3.builder.ReflectionToStringBuilderConcurrencyTest::testLinkedList',
        #    'org.apache.commons.lang3.builder.ReflectionToStringBuilderConcurrencyTest::testArrayList',
        #    'org.apache.commons.lang3.builder.ReflectionToStringBuilderConcurrencyTest::testCopyOnWriteArrayList',
        #    'org.apache.commons.lang3.builder.ReflectionToStringBuilderMutateInspectConcurrencyTest::testConcurrency',
        #]
        #lang_methods_in_redis = [method for method in lang_methods if method in test_methods_from_redis]

        #for lang_method in lang_methods_in_redis:
        #    print "Removing %s from redis:" % lang_method
        #    print r.lrem(key, 1, lang_method)

        #if lang_methods_in_redis:
        #    print "Redis store was modified, reloading list before testing"
        #    test_methods_from_redis = r.lrange(key, 0, -1)

        check_eq(test_methods_from_redis, 'test methods from redis', test_methods_from_d4, 'test methods from d4')

    return "Success"

