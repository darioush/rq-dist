import json
import sys

from contextlib import contextmanager
from rq import get_current_job, Worker
from cStringIO import StringIO
from rq.job import NoSuchJobError

from cvgmeasure.conf import REDIS_PREFIX, DATA_PREFIX, TMP_PREFIX

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
#####

class Tee(object):
    def __init__(self, a, b):
        self.a, self.b = a, b

    def write(self, data):
        self.a.write(data)
        self.b.write(data)

    def isatty(self):
        return True

@contextmanager
def redirect_stdio():
    job = get_current_job()
    if job is None:
        yield
    else:
        oldout, olderr = sys.stdout, sys.stderr
        newout, newerr = StringIO(), StringIO()
        #sys.stdout = Tee(sys.stdout, newout)
        #sys.stderr = Tee(sys.stderr, newerr)
        try:
            yield
            job.meta['stdout'] = newout.getvalue()
            job.meta['stderr'] = newerr.getvalue()
            job.save()
        finally:
            sys.stdout, sys.stderr = oldout, olderr
            newout.close()
            newerr.close()

def job_decorator(f):
    def decorated(input, f=f, *args, **kwargs):
        with redirect_stdio():
            job = get_current_job()
            if job is None:
                f_in = input
                hostname = None
                pid = None
            else:
                f_in = json.loads(input)
                for worker in Worker.all():
                    try:
                        curr_job = worker.get_current_job()
                        if curr_job and (curr_job.id == job.id):
                            hostname, _, _pid = worker.name.partition('.')
                            pid = int(_pid)
                            break
                    except NoSuchJobError:
                        continue
                else:
                    raise Exception("Could not find worker for job: %s" % job.id)
            return f(f_in, hostname, pid, *args, **kwargs)
    return decorated


def mk_key(key, bundle, prefix=REDIS_PREFIX):
    return ':'.join([prefix, key] + map(unicode, bundle))

def mk_data_key(key, bundle, prefix=DATA_PREFIX):
    return mk_key(key, bundle, prefix=prefix)

def mk_tmp_key(key, bundle, prefix=TMP_PREFIX):
    return mk_key(key, bundle, prefix=TMP_PREFIX)

def put_list(r, key, bundle, list):
    _key = mk_key(key, bundle)
    r.delete(_key)
    return r.rpush(_key, *list)

def put_key(r, key, bundle, value):
    _key = mk_key(key, bundle)
    r.set(_key, value)

def put_into_hash(r, key, bundle, hashkey, data):
    _key = mk_key(key, bundle)
    if data is None:
        r.hdel(_key, hashkey)
    else:
        r.hset(_key, hashkey, data)


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
    job = get_current_job()
    if job:
        done_by = job.id
    else:
        done_by = 1
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

    job = get_current_job()
    if job:
        done_by = job.id
    else:
        done_by = 1

    yield zip(worklist, [lambda my_item=item: r.hset(_key, my_item, done_by) for item in worklist])

