import os
import json
import socket
import traceback
import tarfile
import msgpack

from contextlib import contextmanager
from plumbum import local, LocalPath
from plumbum.cmd import rm, mkdir, ls
from redis import StrictRedis
from cStringIO import StringIO
from datetime import datetime, timedelta

from cvgmeasure.common import job_decorator, job_decorator_tg
from cvgmeasure.common import check_key, filter_key_list, mk_key
from cvgmeasure.common import tn_i_s, i_tn_s, tg_i_s
from cvgmeasure.common import put_list, put_into_hash, put_key
from cvgmeasure.common import get_key, inc_key, put_into_set, chunks
from cvgmeasure.conf import get_property
from cvgmeasure.d4 import d4, checkout, refresh_dir, test, get_coverage
from cvgmeasure.d4 import get_coverage_files_to_save, get_tar_gz_file, add_to_path, compile_if_needed, add_timeout
from cvgmeasure.d4 import is_empty, denominator_empty, CoverageCalculationException
from cvgmeasure.s3 import put_into_s3, get_compiled_from_s3, NoFileOnS3, get_file_from_cache_or_s3, list_from_s3
from cvgmeasure.consts import JAR_PATH, ALL_TGS


def jar():
    return local['java']['-jar', JAR_PATH]

TOOL_TO_FILES = {
        'cobertura': {'coverage/coverage_fails': 'coverage_fails', 'cobertura.ser': 'cobertura.ser'},
        'codecover': {'coverage/coverage_fails': 'coverage_fails', 'coverage/test.clf': 'test.clf', 'coverage/empty.clf': 'empty.clf'},
        'jmockit':   {'coverage/coverage_fails': 'coverage_fails', 'coverage/coverage.ser': 'coverage.ser'},
        'major':     {'coverage/coverage_fails': 'coverage_fails', 'kill.csv': 'kill.csv', 'mutants.log': 'mutants.log'},
}


def get_files(work_dir, tool, project, version, suite, t):
    key_name = '/'.join(map(str, [tool, project, version, suite, t]))
    tar_name = str(work_dir / '{tool}.tar.gz'.format(tool=tool))
    get_file_from_cache_or_s3('cvg-files', key_name, tar_name)
    with tarfile.open(tar_name, 'r') as f:
        for src, dst in TOOL_TO_FILES[tool].iteritems():
            dst_fn = str(work_dir / dst)
            with open(dst_fn, 'w') as dst_f:
                inf = f.extractfile(src)
                dst_f.write(inf.read())
                inf.close()

@job_decorator_tg
def remove_redundant_files_s3(r, rr, work_dir, input):
    tool        = input['tool']
    project     = input['project']
    version     = input['version']
    suite       = input['suite']
    redo        = input.get('redo', False)

    bundle = [tool, project, version, suite]
    with check_key(
        rr,
        'redundant-s3',
        bundle,
        redo=redo,
        other_keys=[],
    ) as done:
        ts = list(list_from_s3('cvg-files', bundle))
        orig_sum, upload_sum = 0, 0
        for idx, key in enumerate(ts):
            orig_size = key.size
            prefix, _, t_name = key.name.rpartition('/')
            print '{0}/{1} {2}'.format(idx+1, len(ts), t_name),
            get_files(work_dir, tool, project, version, suite, t_name)
            file_list = TOOL_TO_FILES[tool].values()
            with get_tar_gz_file(file_list) as f:
                upload_size = put_into_s3('cvg-files-min', bundle, t_name, f)
            assert upload_size > 0
            print '... upload  {0} -> {1}'.format(orig_size/1024, upload_size/1024) 
            orig_sum += orig_size
            upload_sum += upload_size

        done({'orig': orig_sum, 'upload': upload_sum})
        return "Success ({0}k -> {1}k : {2} tests)".format(orig_sum/1024, upload_sum/1024, len(ts))

@job_decorator_tg
def map_tgs(r, rr, work_dir, input):
    project     = input['project']
    version     = input['version']
    redo        = input.get('redo', False)
    with check_key(
        r,
        'tgs-mapped',
        [project, version],
        redo=redo,
        other_keys=[],
    ) as done:
        map_file_name = '{project}:{version}'.format(project=project, version=version)
        get_file_from_cache_or_s3('darioush-map-files', map_file_name, str(work_dir / 'map.txt'))
        for tool in ['cobertura', 'codecover', 'jmockit', 'major']:
            # - get a test name
            for suite_type in ['dev', 'randoop.1', 'evosuite-strongmutation-fse.5']:
                test_list = r.hkeys(mk_key('nonempty', [tool, project, version, suite_type])) [:1]
                if tool != 'major':
                    assert len(test_list) == 1
                if len(test_list) == 1:
                    break

            if test_list:
                [test_name,] = i_tn_s(r, test_list, suite_type)
                print tool, test_name
                # - prep the tmp dir
                get_files(work_dir, tool, project, version, suite_type, test_name)

        # - invoke java
        if tool == 'major' and len(test_list) == 0:
            tgs = [tg for tg in ALL_TGS if not tg.endswith('major')]
        else:
            tgs = ALL_TGS

        result = jar()[work_dir](*tgs)
        result = result.rstrip()
        tgs = [s.partition(' ')[2] for s in result.split('\n')]
        tg_i_s(rr, tgs, project, version, allow_create=True)
        done(len(tgs))
    return "Success (tgs={tgs})".format(tgs=len(tgs))


@job_decorator_tg
def tabulate_tgs(r, rr, work_dir, input):
    project     = input['project']
    version     = input['version']
    redo        = input.get('redo', False)
    suite       = input['suite']
    tests       = input['tests']
    generated   = not (suite == 'dev')
    redo        = input.get('redo', False)

    bundle=[project, version, suite]
    with filter_key_list(
        rr,
        key='tgs',
        bundle=bundle,
        list=tests,
        redo=redo,
        other_keys=[],
        worklist_map=lambda tns: tn_i_s(r, tns, suite)
    ) as worklist:
        total = {'t': 0, 'c': 0, 'b': 0}
        count = 0
        for (tc, tc_idx), progress_callback in worklist:
            def handle_single():
                with refresh_dir(work_dir / tc_idx, cleanup=True):
                    print tc_idx, tc

                    map_file_name = '{project}:{version}'.format(project=project, version=version)
                    get_file_from_cache_or_s3('darioush-map-files', map_file_name, str(work_dir / tc_idx / 'map.txt'))
                    # - prep the tmp dir
                    call_tgs = ALL_TGS
                    for tool in ['cobertura', 'codecover', 'jmockit', 'major']:
                        try:
                            get_files(work_dir / tc_idx, tool, project, version, suite, tc)
                        except NoFileOnS3:
                            exec_result = json.loads(r.hget(mk_key('exec', [tool] + bundle), tc_idx))
                            print exec_result, tool
                            if exec_result is None:
                                has_failed = r.sismember(mk_key('fail', ['exec'] + bundle), tc_idx)
                                if has_failed:
                                    print "-- Has failed"
                                    return [], []
                            is_it_empty = is_empty(tool, exec_result)
                            if is_it_empty:
                                if tool in ('major' ,'codecover', 'jmockit'):
                                    print "-> Empty results for {0} noticed, ignoring this tool".format(tool)
                                    call_tgs = [tg for tg in call_tgs if not tg.endswith(tool)]
                                else:
                                    raise
                            else:
                                raise

                    result = jar()[work_dir / tc_idx](*call_tgs)
                    all_tgs = result.strip().split('\n')
                    tgs = [tg for covered, _, tg in [s.partition(' ') for s in all_tgs] if covered == '+']

                    return all_tgs, tgs

            all_tgs, tgs = handle_single()
            # bandaid
            tgs_jmockit = [tg for tg in tgs if tg.find('jmockit') != -1]
            tg_i_s(rr, tgs_jmockit, project, version, allow_create=True)
            # end bandaid

            tg_idxs = tg_i_s(rr, tgs, project, version, allow_create=False)
            assert len(tg_idxs) == len(tgs)
            result = msgpack.packb(tg_idxs, use_bin_type=True)
            results_readable = {'t': len(all_tgs), 'c': len(tgs), 'b': len(result)}
            for key in total:
                total[key] += results_readable[key]
            count += 1
            print '{r[c]}/{r[t]} packed: {r[b]}'.format(r=results_readable)
            progress_callback(result)
    return "Success ({r[c]}/{r[t]} packed: {r[b]} totals, count={count})".format(r=total, count=count)

