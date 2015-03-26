#! /usr/bin/env python

import re
import json

from redis import StrictRedis
from cvgmeasure.d4 import iter_versions
from cvgmeasure.common import tn_i_s

SRC = "redis://nest.cs.washington.edu:6379/1"
DST = "redis://buffalo.cs.washington.edu:6379/1"
PRE_SRC = 'results'






def mk_index():
    s, d = StrictRedis.from_url(SRC), StrictRedis.from_url(DST)

    for suite in ['dev'] + ['evosuite-branch.{0}'.format(i) for i in xrange(0,10)]:
        for project, version in iter_versions():
            key = ':'.join([PRE_SRC, 'test-methods', project, str(version), suite])
            print key
            tm_list = s.lrange(key, 0, -1)
            idxes =  tn_i_s(d, tm_list, suite)
            assert(all(type(idx) is type(0) for idx in idxes))



def mk_tms():
    s, d = StrictRedis.from_url(SRC), StrictRedis.from_url(DST)

    for suite in  ['dev'] + ['evosuite-branch.{0}'.format(i) for i in xrange(0,10)] + ['randoop.{0}'.format(i+1) for i in xrange(0, 10)]:
        for project, version in iter_versions():
            key = ':'.join([PRE_SRC, 'test-methods', project, str(version), suite])
            print key
            tm_list = s.lrange(key, 0, -1)
            idxes =  tn_i_s(d, tm_list, suite)
            dst_key = ':'.join(['tms', project, str(version), suite])
            assert(len(idxes) == len (tm_list))
            for chunk in chunks(idxes, 100):
                if len(chunk) == 0:
                    continue
                d.rpush(dst_key, *chunk)

def mk_bundles():
    s, d = StrictRedis.from_url(SRC), StrictRedis.from_url(DST)

    for suite in  ['evosuite-branch.{0}'.format(i) for i in xrange(0,10)] + ['randoop.{0}'.format(i+1) for i in xrange(0, 10)]:
        for project, version in iter_versions():
            for tool in ['cobertura', 'codecover', 'jmockit']:
                key = ':'.join([PRE_SRC, 'test-methods-exec', tool, project, str(version), suite, 'bundles'])
                result_key = ':'.join([PRE_SRC, 'cvg', tool, project, str(version), suite])
                print key
                execed_bundles = s.hkeys(key)
                if len(execed_bundles) == 0:
                    continue
                idxes = tn_i_s(d, execed_bundles, suite)
                results = s.hmget(result_key, *execed_bundles)
                results_ = [result if result is not None else json.dumps(None) for result in results]

                dst_key = ':'.join(['exec', tool, project, str(version), suite])
                mapping = dict(zip(idxes, results_)) #{idx:  for idx in idxes}
                if len(mapping) > 0:
                    d.hmset(dst_key, mapping)

def mk_additional_test_info():
    s, d = StrictRedis.from_url(SRC), StrictRedis.from_url(DST)

    for suite in  ['evosuite-branch.{0}'.format(i) for i in xrange(0,10)] + ['randoop.{0}'.format(i+1) for i in xrange(0, 10)]:
        for project, version in iter_versions():
            for KN in ['passcnt']:
                fail_key = ':'.join([PRE_SRC, KN, project, str(version), suite])
                dst_fail_key = ':'.join([KN, project, str(version), suite])

                fail_members = list(s.hkeys(fail_key))
                if len(fail_members) > 0:
                    fail_idxes = tn_i_s(d, fail_members, suite)
                    results = s.hmget(fail_key, *fail_members)
                    mapping = dict(zip(fail_idxes, results))
                    d.hmset(dst_fail_key, mapping)

            for tool in ['cobertura', 'codecover', 'jmockit', 'exec']:
                for KN in ['fail']:
                    fail_key = ':'.join([PRE_SRC, KN, tool, project, str(version), suite])
                    print fail_key
                    dst_fail_key = ':'.join([KN, tool, project, str(version), suite])

                    fail_members = list(s.smembers(fail_key))
                    fail_idxes = tn_i_s(d, fail_members, suite)
                    if len(fail_members) > 0:
                        d.sadd(dst_fail_key, fail_idxes)

                #for KN in ['nonempty']:
                #    fail_key = ':'.join([PRE_SRC, KN, tool, project, str(version), suite])
                #    dst_fail_key = ':'.join([KN, tool, project, str(version), suite])

                #    fail_members = list(s.hkeys(fail_key))
                #    if len(fail_members) > 0:
                #        fail_idxes = tn_i_s(d, fail_members, suite)
                #        results = s.hmget(fail_key, *fail_members)
                #        mapping = dict(zip(fail_idxes, results))
                #        d.hmset(dst_fail_key, mapping)



def mk_fetch_result():
    s, d = StrictRedis.from_url(SRC), StrictRedis.from_url(DST)

    for suite in  ['evosuite-branch.{0}'.format(i) for i in xrange(0,10)] + ['randoop.{0}'.format(i+1) for i in xrange(0, 10)]:
        for project, version in iter_versions():
            key = ':'.join([PRE_SRC, 'fetch-result', project, str(version), suite])
            dst_key = ':'.join(['fetch', project, str(version)])

            res = s.get(key)
            if res is not None:
                d.hset(dst_key, suite, res)

def mk_copy(key_in, key_):
    s, d = StrictRedis.from_url(SRC), StrictRedis.from_url(DST)

    key = ':'.join([PRE_SRC, key_in, 'bundles'])
    dst_key = ':'.join([key_, ])

    res = s.hkeys(key)
    print key, len(res)
    if res:
        d.hmset(key_, {k: 1 for k in res})

if __name__ == "__main__":
#    mk_index()
#    mk_tms()
#    mk_bundles()
#    mk_additional_test_info()
#    mk_fetch_result()
    mk_copy('compile-cache', 'compile-cache')
    mk_copy('test-lists-created', 'test-lists-created')


