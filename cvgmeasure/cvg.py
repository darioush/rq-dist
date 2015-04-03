import os
import json
import socket
import traceback
import tarfile

from contextlib import contextmanager
from plumbum import local
from plumbum.cmd import rm, mkdir, ls
from redis import StrictRedis
from cStringIO import StringIO
from datetime import datetime, timedelta

from cvgmeasure.common import job_decorator
from cvgmeasure.common import check_key, filter_key_list, mk_key
from cvgmeasure.common import tn_i_s
from cvgmeasure.common import put_list, put_into_hash, put_key
from cvgmeasure.common import get_key, inc_key, put_into_set
from cvgmeasure.conf import get_property
from cvgmeasure.d4 import d4, checkout, refresh_dir, test, get_coverage
from cvgmeasure.d4 import get_coverage_files_to_save, get_tar_gz_file, add_to_path, compile_if_needed, add_timeout
from cvgmeasure.d4 import is_empty, denominator_empty, CoverageCalculationException
from cvgmeasure.s3 import put_into_s3, get_compiled_from_s3, NoFileOnS3

def test_list_special_case(tc):
    cl, _, method = tc.partition('::')
    if 'org.joda.time.chrono.gj.MainTest' == cl:
        classes = ['edu.washington.cs.testfixer.time.GjMainTest' + suffix for suffix in ('1', '2')]
        return ['%s::%s' % (c, method) for c in classes]

    return [tc]


@job_decorator
def test_lists(input, hostname, pid):
    project = input['project']
    version = input['version']
    redo    = input.get('redo', False)

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    work_dir_path = local.path(work_dir) / ('child.%d' % os.getpid())
    print work_dir

    r = StrictRedis.from_url(redis_url)
    with check_key(
            r,
            'test-lists-created',
            [project, version],
            redo=redo,
            other_keys=['test-methods', 'test-classes']
    ):
        with refresh_dir(work_dir_path, cleanup=True):
            with add_to_path(d4j_path):
                with checkout(project, version, work_dir_path / 'checkout'):
                    d4()('compile')
                    test_methods = reduce(
                        lambda a,b: a+b,
                        map(test_list_special_case, d4()('list-tests').rstrip().split('\n'))
                    )
                    # uniq
                    test_classes_seen = set()
                    def uniq(test_class):
                        retval = test_class not in test_classes_seen
                        test_classes_seen.add(test_class)
                        return retval

                    test_classes = [test_class for test_class, _, _ in
                            map(lambda str: str.partition('::'), test_methods)
                            if uniq(test_class)]

    print project, version
    print work_dir, d4j_path

    method_len = put_list(r, 'test-methods', [project, version], test_methods)
    class_len =  put_list(r, 'test-classes', [project, version], test_classes)
    assert method_len == len(test_methods)
    assert class_len  == len(test_classes)

    return "Success"


@job_decorator
def run_tests_gen(input, hostname, pid):
    project = input['project']
    version = input['version']
    suite   = input['suite']
    passcnt = input['passcnt']
    tests   = input['tests']
    redo    = input.get('redo', False)

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    work_dir_path = local.path(work_dir) / ('child.%d' % os.getpid())
    print work_dir

    r = StrictRedis.from_url(redis_url)
    with filter_key_list(
            r,
            key='test-methods-exec',
            bundle=['exec', project, version, suite],
            list=tests,
            redo=redo,
            other_keys=[],
    ) as worklist:
        with refresh_dir(work_dir_path, cleanup=True):
            with add_to_path(d4j_path):
                with checkout(project, version, work_dir_path / 'checkout'):
                    d4()('compile')
                    gen_tool, _, suite_id = suite.partition('.')
                    fetch_result = d4()('fetch-generated-tests', '-T', gen_tool, '-i', suite_id).strip()
                    if fetch_result != "ok":
                        raise Exception('Unexpected return value from d4 fetch-generated-tests')
                    d4()('compile', '-g')

                    for t, progress_callback in worklist:
                        num_success = int(get_key(r, 'passcnt', [project, version, suite], t, default=0))
                        num_runs = passcnt - num_success
                        for run in xrange(0, num_runs):
                            print "run {run}/{num_runs} of {t}".format(run=(run+1), num_runs=num_runs, t=t)
                            fails = test(extra_args=['-g', '-t', t])
                            if len(fails) == 0:
                                inc_key(r, 'passcnt', [project, version, suite], t)
                            elif len(fails) == 1:
                                put_into_set(r, 'fail', ['exec', project, version, suite], t)
                                break
                            else:
                                raise Exception('Bad code')
                        progress_callback()
    return "Success"


@job_decorator
def find_agree_classes(input, hostname, pid):
    extras = {'in_key' : 'test-classes-cvg-nonempty', 'out_key': 'test-methods-cvg-nonempty'}
    extras.update(input)
    return handle_test_method_non_emptylists(
        extras,
        hostname,
        pid,
        split_fun = lambda m: m.partition('::')[0],
    )


@job_decorator
def find_agree_methods(input, hostname, pid):
    extras = {'in_key' : 'test-methods-run-cvg-nonempty', 'out_key': 'test-methods-agree-cvg-nonempty'}
    extras.update(input)
    return handle_test_method_non_emptylists(
        extras,
        hostname,
        pid,
    )


def handle_test_method_non_emptylists(input, hostname, pid, split_fun=lambda x: x):
    project = input['project']
    version = input['version']
    in_key  = input['in_key']
    out_key = input['out_key']

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    r = StrictRedis.from_url(redis_url)
    keys = [mk_key(in_key, [tool, project, version]) for tool in ('cobertura', 'codecover', 'jmockit')]
    test_classes = [set(r.hkeys(key)) for key in keys]
    test_classes_core = reduce(lambda a,b: a&b, test_classes)
    #print test_classes_core

    test_methods = r.lrange(mk_key('test-methods', [project, version]), 0, -1)
    test_methods_filtered = filter(lambda m: split_fun(m) in test_classes_core, test_methods)

    put_list(r, out_key, [project, version], test_methods_filtered)
    assert len(test_methods_filtered) > 0
    return "Success: %d / %d" % (len(test_methods_filtered), len(test_methods))


@job_decorator
def test_cvg_bundle(input, hostname, pid):
    return handle_cvg_bundle(
        input,
        hostname,
        pid,
        input_key='test_classes',
        check_key='test-classes-checked-for-emptiness',
        result_key='test-classes-cvg',
        non_empty_key='test-classes-cvg-nonempty',
    )


@job_decorator
def test_cvg_methods(input, hostname, pid):
    return handle_test_cvg_bundle(
        input,
        hostname,
        pid,
        input_key='test_methods',
        check_key='test-methods-run',
        result_key='test-methods-run-cvg',
        non_empty_key='test-methods-run-cvg-nonempty',
    )


###### Restructured for new schema
###### Refactored jobs #######
@job_decorator
def compile_cache(r, work_dir, input, key_to_check='compile-cache'):
    project   = input['project']
    version   = input['version']
    cvg_tool  = input['cvg_tool']
    suite     = input['suite']
    redo      = input.get('redo', False)

    upload_bytes = 0
    with check_key(
            r,
            key_to_check,
            [cvg_tool, project, version, suite],
            redo=redo,
            other_keys=[],
            split_at=0,
    ) as done:
        with checkout(project, version, work_dir / 'checkout'):
            coverage_setup_and_reset(cvg_tool, suite)
            # here is making the tar
            sio = StringIO()
            with tarfile.open(fileobj=sio, mode='w:gz') as tar:
                tar.add(name='.', exclude=lambda fn: fn.startswith('./.git'))
            sio.seek(0)
            upload_bytes = put_into_s3('compile-cache', [cvg_tool, project, version], suite, sio)
            sio.close()
            print "Uploaded {upload_bytes} bytes to s3".format(upload_bytes=upload_bytes)
        done()
    return "Success (bytes={bytes})".format(bytes=upload_bytes)


def coverage_setup_and_reset(cvg_tool, suite):
    generated = not (suite == 'dev')
    compile_if_needed(cvg_tool)
    print "reset"
    results = get_coverage(cvg_tool, 'reset')
    print results
    assert is_empty(cvg_tool, results) # make sure result of reset is successful
    assert not denominator_empty(cvg_tool, results)

    if generated:
        print "gen compile"
        gen_tool, _, suite_id = suite.partition('.')
        fetch_result = d4()('fetch-generated-tests', '-T', gen_tool, '-i', suite_id).strip()
        if fetch_result != "ok":
            raise Exception('Unexpected return value from d4 fetch-generated-tests: {response}'.format(
                response=fetch_result))
        d4()('compile', '-g')


@contextmanager
def cache_or_checkout_and_coverage_setup_and_reset(project, version, bucket, bundle, t, dest_dir,
        exception_on_cache_miss=False):
    try:
        with get_compiled_from_s3(bucket, bundle, t, dest_dir):
            print "Cache fetch successful"
            yield
    except NoFileOnS3:
        print "File not on S3", NoFileOnS3
        if exception_on_cache_miss:
            raise
        with checkout(project, version, dest_dir):
            coverage_setup_and_reset(cvg_tool, suite)
            yield

@job_decorator
def generated_cvg(r, work_dir, input):
    return handle_test_cvg_bundle(
        r,
        work_dir,
        input,
        input_key='tests',
        check_key='exec',
        non_empty_key='nonempty',
        pass_count_key='passcnt',
        fail_key='fail',
    )


def handle_test_cvg_bundle(r, work_dir, input, input_key, check_key, non_empty_key,
        pass_count_key=None, fail_key=None):
    project            = input['project']
    version            = input['version']
    cvg_tool           = input['cvg_tool']
    suite              = input['suite']
    redo               = input.get('redo', False)
    timeout            = input.get('timeout', 1800)
    individual_timeout = input.get('individual_timeout', None)
    tests              = input[input_key]
    generated          = not (suite == 'dev')

    if timeout:
        die_time = datetime.now() + timedelta(seconds=timeout)

    empty, nonempty, fail = 0, 0, 0
    bundle = [cvg_tool, project, version, suite]
    with filter_key_list(
            r,
            key=check_key,
            bundle=bundle,
            list=tests,
            redo=redo,
            other_keys=[non_empty_key],
            worklist_map=lambda tns: tn_i_s(r, tns, suite)
    ) as worklist:
        with cache_or_checkout_and_coverage_setup_and_reset(
                project,
                version,
                'compile-cache',
                [cvg_tool, project, version], suite,
                work_dir / 'checkout',
                exception_on_cache_miss=True,
        ):
            for (tc, tc_idx), progress_callback in worklist:
                print "{tc} (= {idx})".format(tc=tc, idx=tc_idx)
                try:
                    if timeout is None:
                        results = get_coverage(cvg_tool, tc, generated=generated)
                    else:
                        remaining_time = max(int((die_time - datetime.now()).total_seconds() * 1000), 0) + 1000
                        if individual_timeout is not None:
                            remaining_time = min(remaining_time, individual_timeout * 1000)
                        print "Timeout to be set @ {remaining_time}".format(remaining_time=remaining_time)
                        with add_timeout(remaining_time):
                            results = get_coverage(cvg_tool, tc, generated=generated)

                    if pass_count_key is not None:
                        inc_key(r, pass_count_key, [project, version, suite], tc_idx) # note the different bundle

                    print results

                    if is_empty(cvg_tool, results):
                        empty += 1
                        put_into_hash(r, non_empty_key, bundle, tc_idx, None)
                    else:
                        nonempty += 1
                        put_into_hash(r, non_empty_key, bundle, tc_idx, 1)
                        file_list = get_coverage_files_to_save(cvg_tool)
                        try:
                            with get_tar_gz_file(file_list) as f:
                                upload_size = put_into_s3('cvg-files', [cvg_tool, project, version, suite], tc, f)
                                print "Uploaded {bytes} bytes to s3".format(bytes=upload_size)
                        except:
                            print "Could not upload to s3"
                except CoverageCalculationException as ex:
                    fail += 1
                    results = None
                    print "-- {suite}:{test} (= {idx}) failed with {tool}".format(suite=suite, test=tc, idx=tc_idx,
                            tool=cvg_tool)
                    print traceback.format_exc()
                    if fail_key is not None:
                        put_into_set(r, fail_key, bundle, tc_idx)

                progress_callback(results)
    return "Success ({empty}/{nonempty}/{fail} ENF)".format(empty=empty, nonempty=nonempty, fail=fail)

@job_decorator
def test_lists_gen(r, work_dir, input):
    project = input['project']
    version = input['version']
    suite   = input['suite']
    redo    = input.get('redo', False)

    with check_key(
            r,
            'fetch',
            [project, version, suite],
            redo=redo,
            other_keys=['tms'],
    ) as done:
        with checkout(project, version, work_dir / 'checkout'):
            d4()('compile')
            gen_tool, _, suite_id = suite.partition('.')
            fetch_result = d4()('fetch-generated-tests', '-T', gen_tool, '-i', suite_id).strip()
            if fetch_result not in ['ok', 'missing', 'empty']:
                raise Exception('Unexpected return value from d4 fetch-generated-tests: {response}'.format(
                    response=fetch_result))
            if fetch_result == 'ok':
                d4()('compile', '-g')
                test_methods = d4()('list-tests', '-g').rstrip().split('\n')
                print "Got methods"

                method_indexes = tn_i_s(r, test_methods, suite, allow_create=True)
                assert len(method_indexes) == len(test_methods)
                method_len = put_list(r, 'tms', [project, version, suite], method_indexes)
                assert method_len == len(test_methods)
            done(fetch_result)

    return "Success (fetch={0})".format(fetch_result)

