import sys
import json
import importlib

from rq import Queue
from redis import StrictRedis

from cvgmeasure.work import main

def get_fun(fun_dotted):
    module_name = '.'.join(fun_dotted.split('.')[:-1])
    fun_name    = fun_dotted.split('.')[-1]
    return getattr(importlib.import_module(module_name), fun_name)

def single_run(fun_dotted, json_str):
    my_function = get_fun(fun_dotted)
    my_function(json.loads(json_str))

def single_enqueue(fun_dotted, json_str, queue_name='default'):
    q = Queue(queue_name, connection=StrictRedis())
    job = q.enqueue_call(
        func=fun_dotted,
        args=(json_str,),
        timeout=10000
    )

if __name__ == "__main__":
    if sys.argv[1] == 'single':
        single_run(sys.argv[2], sys.argv[3])
    if sys.argv[1] == 'q':
        single_enqueue(sys.argv[2], sys.argv[3], sys.argv[4])
