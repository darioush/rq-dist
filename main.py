import sys
import json
import importlib

from optparse import OptionParser
from rq import Queue
from redis import StrictRedis

from cvgmeasure.common import mk_key
from cvgmeasure.conf import REDIS_URL_RQ, get_property
from cvgmeasure.d4 import get_num_bugs, PROJECTS

def get_fun(fun_dotted):
    module_name = '.'.join(fun_dotted.split('.')[:-1])
    fun_name    = fun_dotted.split('.')[-1]
    return getattr(importlib.import_module(module_name), fun_name)

def single_run(fun_dotted, json_str, **kwargs):
    my_function = get_fun(fun_dotted)
    my_function(json.loads(json_str))


def doQ(q, fun_dotted, json_str, timeout, print_only):
    if print_only:
        print q.name, '<-', (fun_dotted, (json_str,), timeout)
    else:
        return q.enqueue_call(
                func=fun_dotted,
                args=(json_str,),
                timeout=timeout
        )

def is_ok(i, v):
    min, _, max = v.partition("-")

    if max == '':
        return i == int(min)

    if max == 'MAX':
        return int(min) <= i

    return int(min) <= i <= int(max)

def single_enqueue(fun_dotted, json_str, queue_name='default', timeout=10000, print_only=False, **kwargs):
    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    doQ(q, fun_dotted, json_str, timeout, print_only)

def enqueue_bundles(fun_dotted, json_str, queue_name='default',
        timeout=180, print_only=False, restrict_project=None, restrict_version=None, **kwargs):
    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    for project in PROJECTS:
        if restrict_project and project not in restrict_project:
            continue

        for i in xrange(1, get_num_bugs(project) + 1):
            if restrict_version and not any(is_ok(i, v) for v in restrict_version):
                continue

            input = {'project': project, 'version': i}
            additionals = json.loads(json_str)
            input.update(additionals)
            doQ(q, fun_dotted, json.dumps(input), timeout, print_only)

def enqueue_bundles_sliced(fun_dotted, json_str, bundle_key, queue_name='default',
        timeout=180, print_only=False, restrict_project=None, restrict_version=None,
        bundle_size=10, bundle_offset=0, bundle_max=None,
        alternates=None, alternate_key=None,
        remove_completed=None,
        **kwargs):
    if bundle_key is None:
        raise Exception("bundle key not provided [-k]")

    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    r = StrictRedis.from_url(get_property('redis_url'))
    for project in PROJECTS:
        if restrict_project and project not in restrict_project:
            continue
        for i in xrange(1, get_num_bugs(project) + 1):
            if restrict_version and not any(is_ok(i, v) for v in restrict_version):
                continue

            key = mk_key('test-classes', [project, i])
            size = r.llen(key)
            if bundle_max is not None:
                size = min(size, bundle_max)

            already_computed = {}
            if alternate_key and remove_completed:
                for alternate in alternates:
                    _key = mk_key('test-classes-checked-for-emptiness', [alternate, project, i, 'bundles'])
                    already_computed[alternate] = set(r.hkeys(_key))
                    #print "Using key: ", _key, len(already_computed[alternate])

            for j in xrange(bundle_offset, size, bundle_size):
                bundle = r.lrange(key, j, j+bundle_size-1)

                if alternate_key:
                    for alternate in alternates:
                        input = {'project': project, 'version': i, bundle_key: bundle, alternate_key: alternate}
                        additionals = json.loads(json_str)
                        input.update(additionals)
                        if remove_completed:

                            filtered_list = [item for item in bundle if item not in already_computed[alternate]]
                            if len(filtered_list) == 0:
                                #print "Skipping empty bundle"
                                continue
                            input[bundle_key] = filtered_list

                        doQ(q, fun_dotted, json.dumps(input), timeout, print_only)
                else:
                    input = {'project': project, 'version': i, bundle_key: bundle}
                    additionals = json.loads(json_str)
                    input.update(additionals)
                    doQ(q, fun_dotted, json.dumps(input), timeout, print_only)

if __name__ == "__main__":

    cmd, fun_name = sys.argv[1], sys.argv[2]

    parser = OptionParser()
    parser.add_option("-q", "--queue", dest="queue_name", action="store", type="string", default="default")
    parser.add_option("-j", "--json", dest="json_str", action="store", type="string", default="{}")
    parser.add_option("-t", "--timeout", dest="timeout", action="store", type="int", default=180)
    parser.add_option("-b", "--bundle-size", dest="bundle_size", action="store", type="int", default=10)
    parser.add_option("-c", "--commit", dest="print_only", action="store_false", default=True)
    parser.add_option("-p", "--project", dest="restrict_project", action="append")
    parser.add_option("-v", "--version", dest="restrict_version", action="append")
    parser.add_option("-k", "--bundle-key", dest="bundle_key", action="store", type="string")
    parser.add_option("-o", "--bundle-offset", dest="bundle_offset", action="store", type="int", default=0)
    parser.add_option("-m", "--bundle-max", dest="bundle_max", action="store", type="int")

    parser.add_option("-a", "--alternate",     dest="alternates", action="append")
    parser.add_option("-K", "--alternate-key", dest="alternate_key", action="store", type="string", default=None)

    parser.add_option("-Z", "--remove-completed", dest="remove_completed", action="store_true")

    (options, args) = parser.parse_args(sys.argv[3:])

    funs = {
        'single': single_run,
        'q': single_enqueue,
        'qb': enqueue_bundles,
        'qb-slice': enqueue_bundles_sliced,
    }

    funs[cmd](fun_name, **vars(options))
