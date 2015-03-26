import re
import importlib
import json
import redis

from contextlib import contextmanager
from plumbum import local

from cvgmeasure.conf import get_property_defaults
from cvgmeasure.d4 import refresh_dir, add_to_path

def get_fun(fun_dotted):
    module_name = '.'.join(fun_dotted.split('.')[:-1])
    fun_name    = fun_dotted.split('.')[-1]
    return getattr(importlib.import_module(module_name), fun_name)


def doQ(q, fun_dotted, json_str, timeout, print_only):
    if print_only:
        print q.name, '<-', (fun_dotted, (json_str,), timeout)
    else:
        return q.enqueue_call(
                func=fun_dotted,
                args=(json_str,),
                timeout=timeout
        )


def job_decorator(f):
    def decorated(input, f=f, *args, **kwargs):
        f_in = json.loads(input)
        work_dir, d4j_path, redis_url = map(
                lambda property: get_property_defaults(property),
                ['work_dir', 'd4j_path', 'redis_url']
        )

        work_dir_path = local.path(work_dir)
        print "Working directory {0}".format(work_dir_path)

        with refresh_dir(work_dir_path, cleanup=True):
            with add_to_path(d4j_path):
                with connect_to_redis(redis_url) as r:
                    return f(r, work_dir_path, f_in, *args, **kwargs)

    return decorated


@contextmanager
def connect_to_redis(url):
    pool = redis.ConnectionPool.from_url(url)
    yield redis.StrictRedis(connection_pool=pool)
    pool.disconnect()


def mk_key(key, bundle):
    return ':'.join([key] + map(unicode, bundle))


def put_into_set(r, key, bundle, member):
    _key = mk_key(key, bundle)
    return r.sadd(_key, member)


def put_list(r, key, bundle, list):
    _key = mk_key(key, bundle)
    r.delete(_key)
    return r.rpush(_key, *list)


def put_key(r, key, bundle, value):
    _key = mk_key(key, bundle)
    return r.set(_key, value)


def inc_key(r, key, bundle, field, increment=1):
    _key = mk_key(key, bundle)
    return r.hincrby(_key, field, increment)


def put_into_hash(r, key, bundle, hashkey, data):
    _key = mk_key(key, bundle)
    if data is None:
        return r.hdel(_key, hashkey)
    else:
        return r.hset(_key, hashkey, data)


def get_key(r, key, bundle, field, default=None):
    _key = mk_key(key, bundle)
    result = r.hget(_key, field)
    if result is None:
        return default
    else:
        return result


@contextmanager
def check_key(r, key, bundle, redo=False, other_keys=[]):
    _key = mk_key(key, [])
    _bundle = ':'.join(map(unicode, bundle))
    if r.hexists(_key, _bundle):
        if redo:
            print "Results already computed for %s %s, but redo is forced." % (_key, _bundle)
            # delete results
            r.hdel(_key, _bundle)
            for key in other_keys:
                r.delete(mk_key(key, bundle))
        else:
            raise DuplicateBundleAttempt("Results already computed for %s %s" % (_key, _bundle))
    def complete(result=1):
        r.hset(_key, bundle, json.dumps(result))
    yield complete


class DuplicateBundleAttempt(Exception):
    pass


## New schema
@contextmanager
def filter_key_list(r, key, bundle, list, redo=False, other_keys=[], worklist_map=lambda x: x):
    _key = mk_key(key, bundle)

    list_pairs = zip(list, worklist_map(list))
    assert len(list_pairs) == len(list)

    already_computed = set(r.hkeys(_key))
    already_computed_list = [(item, idx) for (item, idx) in list_pairs if idx in already_computed]
    for (item, idx) in already_computed_list:
        print "Results already computed for {0} {1} (= {2})".format(_key, item, idx)
        if redo:
            r.hdel(_key, idx)
            for key in other_keys:
                r.hdel(mk_key(key, bundle), idx)

    filtered_list = [(item, idx) for (item, idx) in list_pairs if idx not in already_computed]

    if len(filtered_list) == 0 and redo is False:
        raise DuplicateBundleAttempt("No more items left to compute for %s" % _key)

    if redo:
        worklist_pairs = list_pairs
    else:
        worklist = filtered_list

    yield zip(worklist, [lambda result=1, my_item=item: r.hset(_key, my_item, json.dumps(result)) for (_, item)
                in worklist])


def chunks(l, n):
    """ Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def flatten(l):
    return reduce(lambda a, b : a+b, l, [])


def tn_i_s(r, tns, suite, allow_create=False):
    # Randoop names are deterministic. Save DB space by not putting them there
    if suite.startswith('randoop'):
        groups = [re.match(r'RandoopTest(\d+)::test(\d+)', tn) for tn in tns]
        if any([group is None for group in groups]):
            raise Exception("Bad randoop test name")
        nums = [(int(group.group(1)), int(group.group(2))) for group in groups]
        idxs = [class_num * 1000 + method_num for (class_num, method_num) in nums]
        return idxs

    if suite.startswith('evo'):
        prefix = 'evo'
    elif suite == 'dev':
        prefix = 'dev'
    else:
        raise Exception("Bad type of suite {0}".format(suite))

    if allow_create:
        last = r.get('tn-i:max:{pre}'.format(pre=prefix))
        last = 0 if last is None else int(last)

    key = 'tn-i:{pre}'.format(pre=prefix)
    key_rev = 'i-tn:{pre}'.format(pre=prefix)
    results = []
    for chunk in chunks(tns, 100):
        idxes = r.hmget(key, *chunk)
        assert(len(idxes) == len(chunk))
        missings = [tn for (tn, idx) in zip(chunk, idxes) if idx is None]
        if missings:
            if not allow_create:
                raise Exception("Could not find idx for tests: {0}".format(' '.join(missings)))

            missings_idx = {tn: last + idx for (idx, tn) in enumerate(missings)}
            missings_idx_rev = {(last + idx): tn for (idx, tn) in enumerate(missings)}
            r.incrby('tn-i:max:{pre}'.format(pre=prefix), len(missings))
            last += len(missings)
            r.hmset(key, missings_idx)
            r.hmset(key_rev, missings_idx_rev)
            assert(len(missings_idx) == len(missings_idx_rev))
            assert(r.hlen(key) == r.hlen(key_rev))
            results.append([int(idx) for idx in r.hmget(key, *chunk)])
        else:
            results.append([int(idx) for idx in idxes])
    return flatten(results)


def i_tn_s(r, i_s, suite):
    i_s = map(int, i_s) # make sure everything is a number

    if suite.startswith('randoop'):
        return ["RandoopTest{class_num}::test{test_num}".format(
            class_num = num / 1000,
            test_num =  num % 1000
            ) for num in i_s]

    if suite.startswith('evo'):
        prefix = 'evo'
    elif suite == 'dev':
        prefix = 'dev'
    else:
        raise Exception("Bad type of suite {0}".format(suite))

    key_rev = 'i-tn:{pre}'.format(pre=prefix)
    results = []
    for chunk in chunks(i_s, 100):
        tns = r.hmget(key_rev, *chunk)
        assert(len(tns) == len(chunk))
        results.append(tns)
    return flatten(results)

# helper function for calling from main.py
def M(r, i_s, tail_key):
    assert(len(tail_key) == 1)
    return i_tn_s(r, i_s, tail_key[0])

