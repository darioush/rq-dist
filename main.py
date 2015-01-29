import sys
import json
import importlib

from rq import Queue
from redis import StrictRedis

from cvgmeasure.common import get_num_bugs, PROJECTS, mk_key
from cvgmeasure.conf import REDIS_URL_RQ, get_property

def get_fun(fun_dotted):
    module_name = '.'.join(fun_dotted.split('.')[:-1])
    fun_name    = fun_dotted.split('.')[-1]
    return getattr(importlib.import_module(module_name), fun_name)

def single_run(fun_dotted, json_str):
    my_function = get_fun(fun_dotted)
    my_function(json.loads(json_str))

def single_enqueue(fun_dotted, json_str, queue_name='default'):
    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    job = q.enqueue_call(
        func=fun_dotted,
        args=(json_str,),
        timeout=10000
    )

def enqueue_bundles(fun_dotted, additional_jsons, queue_name='default',
        timeout=180):
    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    for project in PROJECTS:
        for i in xrange(1, get_num_bugs(project) + 1):
            input = {'project': project, 'version': i}
            additionals = json.loads(additional_jsons)
            input.update(additionals)
            print input
            job = q.enqueue_call(
                    func=fun_dotted,
                    args=(json.dumps(input),),
                    timeout=timeout
                )

def enqueue_bundles_sliced(fun_dotted, additional_jsons, queue_name='default',
        timeout=180, bundle_size=10):
    q = Queue(queue_name, connection=StrictRedis.from_url(REDIS_URL_RQ))
    r = StrictRedis.from_url(get_property('redis_url'))
    for project in PROJECTS:
        for i in xrange(1, get_num_bugs(project) + 1):
            key = mk_key('test-classes', [project, i])
            print r.llen(key)

if __name__ == "__main__":
    if sys.argv[1] == 'single':
        single_run(sys.argv[2], sys.argv[3])
    if sys.argv[1] == 'q':
        single_enqueue(sys.argv[2], sys.argv[3], sys.argv[4])

    if sys.argv[1] == 'qb':
        enqueue_bundles(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]))

    if sys.argv[1] == 'qb-slice':
        enqueue_bundles_sliced(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]), int(sys.argv[6]))
