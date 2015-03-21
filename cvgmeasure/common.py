import importlib
import json
import redis

from contextlib import contextmanager
from rq.job import _job_stack
from plumbum import local

from cvgmeasure.conf import REDIS_PREFIX, DATA_PREFIX, TMP_PREFIX, get_property_defaults
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


def mk_key(key, bundle, prefix=REDIS_PREFIX):
    return ':'.join([prefix, key] + map(unicode, bundle))


def mk_data_key(key, bundle, prefix=DATA_PREFIX):
    return mk_key(key, bundle, prefix=prefix)


def mk_tmp_key(key, bundle, prefix=TMP_PREFIX):
    return mk_key(key, bundle, prefix=TMP_PREFIX)


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


# TODO: A bit shady
def get_current_job_id():
    return _job_stack.top


@contextmanager
def check_key(r, key, bundle, redo=False, other_keys=[]):
    _key = mk_key(key, ['bundles'])
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
    yield
    job = get_current_job_id()
    done_by = 1 if job is None else job
    r.hset(_key, _bundle, done_by)


class DuplicateBundleAttempt(Exception):
    pass


@contextmanager
def filter_key_list(r, key, bundle, list, redo=False, other_keys=[]):
    _key = mk_key(key, bundle + ['bundles'])

    already_computed = set(r.hkeys(_key))
    already_computed_list = [item for item in list if item in already_computed]
    for item in already_computed_list:
        print "Results already computed for %s %s" % (_key, item)
        if redo:
            r.hdel(_key, item)
            for key in other_keys:
                r.hdel(mk_key(key, bundle), item)

    filtered_list = [item for item in list if item not in already_computed]

    if len(filtered_list) == 0 and redo is False:
        raise DuplicateBundleAttempt("No more items left to compute for %s" % _key)

    if redo:
        worklist = list
    else:
        worklist = filtered_list

    job = get_current_job_id()
    done_by = 1 if job is None else job

    yield zip(worklist, [lambda my_item=item: r.hset(_key, my_item, done_by) for item in worklist])

