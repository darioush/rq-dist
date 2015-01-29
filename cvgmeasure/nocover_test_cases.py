import os
import socket

from plumbum import local
from plumbum.cmd import rm, mkdir, ls
from contextlib import contextmanager
from redis import StrictRedis

from cvgmeasure.common import job_decorator, check_key, add_to_path, checkout
from cvgmeasure.common import d4, checkout, refresh_dir
from cvgmeasure.common import put_list
from cvgmeasure.conf import get_property


@job_decorator
def test_lists(input, hostname, pid):
    project = input['project']
    version = input['version']
    redo    = input.get('redo', False)

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    r = StrictRedis.from_url(redis_url)
    with check_key(
            r,
            'test-lists-created',
            [project, version],
            redo=redo,
            other_keys=['test-methods', 'test-classes']
    ):
        with refresh_dir(work_dir, cleanup=False):
            with add_to_path(d4j_path):
                with checkout(project, version, local.path(work_dir) / 'checkout'):
                    d4()('compile')
                    test_methods = d4()('list-tests').rstrip().split('\n')
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
def test_cvg_bundle(input, hostname, pid):
    project = input['project']
    version = input['version']
    cvg_tool = input['cvg_tool']
    redo    = input.get('redo', False)
    test_classes = input['test_classes']

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    r = StrictRedis.from_url(redis_url)

    with refresh_dir(work_dir, cleanup=False):
        with add_to_path(d4j_path):
            with checkout(project, version, local.path(work_dir) / 'checkout'):
                d4()('compile')
                print "Hi"
