#! /usr/bin/env python
import sys
import json
import importlib

from optparse import OptionParser
from rq import Queue
from redis import StrictRedis
from collections import defaultdict

from cvgmeasure.common import mk_key, get_fun, doQ
from cvgmeasure.conf import REDIS_URL_RQ, get_property
from cvgmeasure.d4 import get_num_bugs, PROJECTS, iter_versions

def single_run(fun_dotted, json_str, **kwargs):
    my_function = get_fun(fun_dotted)
    my_function(json.loads(json_str))

def single_enqueue(fun_dotted, json_str, queue_name='default', timeout=10000, print_only=False, at_front=False, **kwargs):
    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    doQ(q, fun_dotted, json_str, timeout, print_only, at_front)

def enqueue_bundles(fun_dotted, json_str, queue_name='default',
        timeout=1800, print_only=False, restrict_project=None, restrict_version=None,
        tail_keys=[], tail_key_descr=None, at_front=False, **kwargs):
    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    for project, i in iter_versions(restrict_project, restrict_version):
        input = {'project': project, 'version': i}
        additionals = json.loads(json_str)
        input.update(additionals)
        input.update({'timeout': timeout})
        if tail_keys == []:
            tail_keys_to_iterate = [[]] # run the forloop once, but don't add any tail_key
        else:
            tail_keys_to_iterate = [[tk] for tk in tail_keys] # each of the tk's now counts, but singly
        for tail_key in tail_keys_to_iterate:
            input.update({} if tail_key_descr is None else {tail_key_descr: ':'.join(tail_key)})
            doQ(q, fun_dotted, json.dumps(input), timeout, print_only, at_front)

def enqueue_bundles_sliced(fun_dotted, json_str, bundle_key,
        source_key, tail_keys=[], tail_key_descr=None,
        queue_name='default',
        timeout=1800, print_only=False, restrict_project=None, restrict_version=None,
        bundle_size=10, bundle_offset=0, bundle_max=None,
        alternates=None, alternate_key=None,
        check_key=None, filter_function=None, filter_arg=None,
        map_function=None,
        at_front=False,
        **kwargs):
    if bundle_key is None:
        raise Exception("bundle key not provided [-k]")
    if tail_keys == []:
        tail_keys_to_iterate = [[]] # run the forloop once, but don't add any tail_key
    else:
        tail_keys_to_iterate = [[tk] for tk in tail_keys] # each of the tk's now counts, but singly

    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    r = StrictRedis.from_url(get_property('redis_url'))

    key_type = None # such hack
    if source_key.startswith('file:'):
        key_type = 'file'
        _, _, fn = source_key.partition(':')
        source_key = 'file'
        file_data = defaultdict(list)
        with open(fn) as f:
            for line in f:
                line_data = json.loads(line)
                file_data.update(line_data)

    for tail_key in tail_keys_to_iterate:
        for project, i in iter_versions(restrict_project, restrict_version):
            key = mk_key(source_key, [project, i] + tail_key)

            if key_type != 'file':
                key_type = r.type(key)

            if key_type == 'list':
                size = r.llen(key)
            elif key_type == 'hash':
                size = r.hlen(key)
            elif key_type == 'file':
                size = len(file_data[key])
            elif key_type == 'none':
                size = 0
            else:
                raise Exception('-- Unexpected key type: {0}'.format(key_type))

            if bundle_max is not None:
                size = min(size, bundle_max)

            mf = (lambda _1, x, _2: x) if map_function is None else get_fun(map_function)
            already_computed = {}
            if alternate_key and check_key:
                for alternate in alternates:
                    _key = mk_key(check_key, [alternate, project, i] + tail_key)
                    already_computed[alternate] = set(mf(r, r.hkeys(_key), tail_key))

            if key_type == 'hash':
                all_items = r.hkeys(key)
            elif key_type == 'file':
                all_items = file_data[key]

            for j in xrange(bundle_offset, size, bundle_size):
                if key_type == 'list':
                    bundle = r.lrange(key, j, j+bundle_size-1)
                elif key_type in ('hash', 'file'):
                    bundle = all_items[j:j+bundle_size]
                elif key_type == 'none':
                    bundle = []

                if filter_function is not None:
                    ff = get_fun(filter_function)
                    bundle = ff(r, project, i, tail_key, filter_arg, bundle)


                if len(bundle) == 0:
                    continue

                bundle = mf(r, bundle, tail_key)

                if alternate_key:
                    for alternate in alternates:
                        input = {'project': project, 'version': i, bundle_key: bundle, alternate_key: alternate}
                        tk_input = {} if tail_key_descr is None else {tail_key_descr: ':'.join(tail_key)}
                        additionals = json.loads(json_str)
                        input.update(additionals)
                        input.update(tk_input)
                        if check_key:
                            filtered_list = [item for item in bundle if item not in already_computed[alternate]]
                            if len(filtered_list) == 0:
                                #print "Skipping empty bundle"
                                continue
                            input[bundle_key] = filtered_list

                        input.update({'timeout': timeout})
                        doQ(q, fun_dotted, json.dumps(input), timeout, print_only, at_front)
                else:
                    input = {'project': project, 'version': i, bundle_key: bundle}
                    tk_input = {} if tail_key_descr is None else {tail_key_descr: ':'.join(tail_key)}
                    additionals = json.loads(json_str)
                    input.update(additionals)
                    input.update(tk_input)
                    input.update({'timeout': timeout})
                    doQ(q, fun_dotted, json.dumps(input), timeout, print_only, at_front)

if __name__ == "__main__":

    cmd, fun_name = sys.argv[1], sys.argv[2]

    parser = OptionParser()
    parser.add_option("-q", "--queue", dest="queue_name", action="store", type="string", default="default")
    parser.add_option("-f", "--front", dest="at_front", action="store_true", default=False)
    parser.add_option("-j", "--json", dest="json_str", action="store", type="string", default="{}")
    parser.add_option("-t", "--timeout", dest="timeout", action="store", type="int", default=1800)
    parser.add_option("-b", "--bundle-size", dest="bundle_size", action="store", type="int", default=10)
    parser.add_option("-c", "--commit", dest="print_only", action="store_false", default=True)
    parser.add_option("-p", "--project", dest="restrict_project", action="append")
    parser.add_option("-v", "--version", dest="restrict_version", action="append")
    parser.add_option("-k", "--bundle-key", dest="bundle_key", action="store", type="string")
    parser.add_option("-o", "--bundle-offset", dest="bundle_offset", action="store", type="int", default=0)
    parser.add_option("-m", "--bundle-max", dest="bundle_max", action="store", type="int")

    parser.add_option("-a", "--alternate",     dest="alternates", action="append")
    parser.add_option("-K", "--alternate-key", dest="alternate_key", action="store", type="string", default=None)

    parser.add_option("-T", "--tail-key-alternate", dest="tail_keys", action="append", default=[])
    parser.add_option("-R", "--tail-key-alternate-range", dest="tail_key_range", action="store", type="string", default=None)
    parser.add_option("-s", "--tail-key", dest="tail_key_descr", action="store", type="string", default=None)

    parser.add_option("-Z", "--remove-completed", dest="check_key", action="store", type="string", default=None)
    parser.add_option("-S", "--source-key", dest="source_key", action="store", type="string")
    parser.add_option("-F", "--filter-function", dest="filter_function", action="store", type="string", default=None)
    parser.add_option("-A", "--filter-arg", dest="filter_arg", action="store", type="string", default=None)

    parser.add_option("-M", "--map-function", dest="map_function", action="store", type="string", default=None)


    (options, args) = parser.parse_args(sys.argv[3:])

    if options.tail_keys and options.tail_key_range:
        rmin, _, rmax = options.tail_key_range.partition('-')
        rmin, rmax = int(rmin), int(rmax)
        suites = ['{name}.{id}'.format(name=tk, id=id) for id in xrange(rmin, rmax+1)
                for tk in options.tail_keys]

        options.tail_keys = suites

    funs = {
        'single': single_run,
        'q': single_enqueue,
        'qb': enqueue_bundles,
        'qb-slice': enqueue_bundles_sliced,
        'qbs': enqueue_bundles_sliced,
    }

    funs[cmd](fun_name, **vars(options))

