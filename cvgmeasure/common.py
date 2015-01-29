import json
import re
import sys

from contextlib import contextmanager
from rq import get_current_job, Worker
from cStringIO import StringIO
from plumbum import local
from plumbum.cmd import rm, mkdir, ls

from cvgmeasure.conf import REDIS_PREFIX

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
        sys.stdout = Tee(sys.stdout, newout)
        sys.stderr = Tee(sys.stderr, newerr)
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
                    if worker.get_current_job() and \
                            (worker.get_current_job().id == job.id):
                        hostname, _, _pid = worker.name.partition('.')
                        pid = int(_pid)
                        break
                else:
                    raise Exception("Could not find worker for job: %s" % job.id)
            return f(f_in, hostname, pid, *args, **kwargs)
    return decorated


@contextmanager
def refresh_dir(dir, cleanup=True):
    rm('-rf', dir)
    mkdir('-p', dir)
    with local.cwd(dir):
        try:
            yield
            if cleanup:
                rm('-rf', dir)
        except:
            raise

@contextmanager
def add_to_path(l):
    for item in reversed(l):
        local.env.path.insert(0, item)
    yield
    for _ in l:
        local.env.path.pop()

def d4():
    return local['defects4j']

@contextmanager
def checkout(project, version, to):
    d4()('checkout', '-p', project, '-v', "%df" % version, '-w', to)
    with local.cwd(to):
        yield

def mk_key(key, bundle):
    return ':'.join([REDIS_PREFIX, key] + map(unicode, bundle))

def put_list(r, key, bundle, list):
    _key = mk_key(key, bundle)
    r.delete(_key)
    return r.rpush(_key, *list)

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




PROJECTS = ['Lang', 'Chart', 'Math', 'Closure', 'Time']

def get_num_bugs(project):
    if not project in PROJECTS:
        raise Exception("Bad project")
    return int(d4()('info', '-p', project, '-c').rstrip())


class CoverageCalculationException(Exception):
    pass


def get_coverage(cvg_tool, tc):
    cvg = d4()['coverage', '-T', cvg_tool, '-t']
    output = cvg(tc)
    regexps = {
            r'Lines total: (\d+)': 'lt',
            r'Lines covered: (\d+)': 'lc',
            r'Branches total: (\d+)': 'bt',
            r'Branches covered: (\d+)': 'bc',
    }
    result = {}
    def update_dict(line, result):
        for regexp, key in regexps.iteritems():
            match = re.match(regexp, line)
            if match:
                result[key] = int(match.group(1))
    for line in output.split('\n'):
        update_dict(line, result)
    if not all(val in result for val in regexps.values()):
        raise CoverageCalculationException("Could not calculate coverage for: %s, %s" % (cvg_tool, tc))

    if result['lt'] == 0:
        raise CoverageCalculationException("Lines Total reported as 0 for: %s, %s" % (cvg_tool, tc))

    return result


def get_coverage_files_to_save(cvg_tool):
    return {
            'cobertura': ['cobertura.ser', 'coverage/'],
    }[cvg_tool]


def get_tar_gz_str(files, out='output.tar.gz'):
    rm('-rf', out)
    local['tar']['cfz', out](*files)
    with open(out) as f:
        result = f.read()
    return result


