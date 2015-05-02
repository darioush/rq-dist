#! /usr/bin/env python

import json

from redis import StrictRedis
from plumbum import LocalPath

from cvgmeasure.d4 import iter_versions, is_empty
from cvgmeasure.common import mk_key, i_tn_s, tn_i_s
from cvgmeasure.s3 import put_into_s3


FROM_URL='redis://monarch.cs.washington.edu:6379/1'
TO_URL='redis://tern.cs.washington.edu:6379/1'

def rename_test_methods(r_from):
    keys = r_from.keys('results:test-methods:*')
    for key in keys:
        r_from.rename(key, '{key}:dev'.format(key=key))

def migrate_test_methods(r_from):
    keys = r_from.keys('results:test-methods:*:dev')
    for key in keys:
        print "MIGRATE nest.cs.washington.edu 6379 {key} 1 10000".format(key=key)

def migrate_test_classes(r_from, r_to, projects=[], versions=[]):
    for project, version in iter_versions(projects, versions):
        print project, version
        tm_key = 'tms:{project}:{version}:dev'.format(project=project, version=version)
        tm_is = r_to.lrange(tm_key, 0, -1)
        tms = i_tn_s(r_to, tm_is, 'dev')

        unique_set = set([])
        def is_unique(tt):
            retval = tt not in unique_set
            unique_set.add(tt)
            return retval
        tc_tns = [tc for tc, _, _ in [tm.partition('::') for tm in tms] if is_unique(tc)]
        tc_tis = tn_i_s(r_to, tc_tns, 'dev', allow_create=False)

#        for tool in ['codecover', 'jmockit']:
#            class_key = 'results:test-classes-cvg:{tool}:{project}:{version}'.format(
#                    tool=tool,project=project,version=version
#            )
#            class_cvgs = r_from.hmget(class_key, *tc_tns)
#            assert all(lambda x: x is not None for x in class_cvgs)
#            assert len(tc_tis) == len(class_cvgs)
#            to_key_class = 'exec:{tool}:{project}:{version}:dev'.format(
#                    tool=tool, project=project, version=version
#            )
#            r_to.hmset(to_key_class, {ck: cv for (ck, cv) in zip(tc_tis, class_cvgs)})

        for tool in ['cobertura', 'codecover', 'jmockit', 'major']:
            method_key = 'results:test-methods-run-cvg:{tool}:{project}:{version}'.format(
                    tool=tool, project=project, version=version
            )
            res_dict = r_from.hgetall(method_key)
            assert(type(res_dict) == dict)

            res_list = res_dict.items()
            res_idxs  = tn_i_s(r_to, [k for (k, v) in res_list], 'dev')
            res_vals  = [v for (_, v) in res_list]
            assert len(res_vals) == len(res_idxs)
            res_map = {ki: v for (ki, v) in zip(res_idxs, res_vals)}
            to_key = 'exec:{tool}:{project}:{version}:dev'.format(
                    tool=tool, project=project, version=version
            )
            if res_map:
                r_to.hmset(to_key, res_map)



def s3_files_for(r_from, r_to, projects=[], versions=[]):
    for project, version in iter_versions(projects, versions):
        key = 'results:test-methods:{project}:{version}:dev'.format(
                project=project, version=version
        )
        test_methods = r_to.lrange(key, 0, -1)
        print "{project}:{version} has {tms} methods".format(
                project=project, version=version,
                tms=len(test_methods)
        )
        for test_method in test_methods:
            c_name, _, m_name = test_method.partition('::')

            is_class_empty = []
            for tool in ["codecover", "cobertura", "jmockit"]:
                class_key = mk_key('test-classes-cvg', [tool, project, version])
                class_r = json.loads(r_from.hget(class_key, c_name))
                is_class_empty.append(is_empty(tool, class_r))

            if all(is_class_empty):
                continue

            is_method_nonempty = []
            for tool in ["codecover", "cobertura", "jmockit"]:
                method_key = mk_key('test-methods-run-cvg', [tool, project, version])
                try:
                    method_r = json.loads(r_from.hget(method_key, test_method))
                    if not is_empty(tool, method_r):
                        key = ':'.join([tool, project, str(version), test_method])
                        DIR = '/scratch/darioush/files'
                        p = LocalPath(DIR) / '{key}.tar.gz'.format(key=key)
                        assert p.exists()
                        is_method_nonempty.append(True)
                        with open(str(p)) as f:
                            s3_up = put_into_s3('cvg-files', [tool, project, version, 'dev'], test_method, f)
                    else:
                        is_method_nonempty.append(False)
                except TypeError:
                    print "--> Missing result {tool}:{project}:{version}:dev:{test}".format(tool=tool,test=test_method, project=project, version=version)
                except AssertionError:
                    print "--> Missing file {tool}:{project}:{version}:dev:{test}".format(tool=tool,test=test_method, project=project, version=version)

            if any(is_method_nonempty):
                mut_key = mk_key('test-methods-run-cvg', ['major', project, version])
                try:
                    mut_r = json.loads(r_from.hget(mut_key, test_method))
                    if not is_empty('major', mut_r):
                        key = ':'.join(['major', project, str(version), test_method])
                        DIR = '/scratch/darioush/files'
                        p = LocalPath(DIR) / '{key}.tar.gz'.format(key=key)
                        assert p.exists()
                        with open(str(p)) as f:
                            s3_up = put_into_s3('cvg-files', ['major', project, version, 'dev'], test_method, f)
                except TypeError:
                    tool = 'major'
                    print "--> Missing result {tool}:{project}:{version}:dev:{test}".format(tool=tool,test=test_method, project=project, version=version)
                except AssertionError:
                    print "--> Missing file {tool}:{project}:{version}:dev:{test}".format(tool=tool,test=test_method, project=project, version=version)



def fix_fails(r_to):
    for key in r_to.keys('fail:*'):
        print key
        for member in r_to.smembers(key):
            loaded = json.loads(member)
            the_type = type(loaded)
            if the_type is not int:
                assert the_type is list
                assert r_to.sismember(key, loaded)
                r_to.sadd(key, *loaded)
                for item in loaded:
                    assert r_to.sismember(key, item)
                print r_to.srem(key, loaded)

if __name__ == "__main__":
    r_from = StrictRedis.from_url(FROM_URL)
    r_to   = StrictRedis.from_url(TO_URL)
#    rename_test_methods(r_from)
#    migrate_test_methods(r_from)
#    s3_files_for(r_from, r_to)
#    migrate_test_classes(r_from, r_to)
    fix_fails(r_to)
