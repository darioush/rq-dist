import json

from rq import Queue, get_current_job
from redis import StrictRedis

from cvgmeasure.common import job_decorator
from cvgmeasure.common import check_key, filter_key_list, mk_key
from cvgmeasure.common import doQ
from cvgmeasure.common import DuplicateBundleAttempt
from cvgmeasure.conf import REDIS_URL_RQ, get_property

class MonitorFailException(Exception):
    pass

@job_decorator
def monitor_job(input, hostname, pid):
    bundle = input['bundle'] # e.g., project:version:cvg_tool
    bundle_keys = bundle.split(':')
    check_key = input['check_key'] #e.g., test-methods-run
    list_key = input['list_key'] # e.g., test_methods
    monitor_queue, job_queue = input['monitor_queue'], input['job_queue']
    job_name = input['job_name']
    timeout = input['timeout']
    monitor_name = u'cvgmeasure.monitor.monitor_job'
    commit = input.get('commit', False)
    downsize = input.get('downsize', None)

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    r = StrictRedis.from_url(redis_url)

    try:
        with filter_key_list(
            r,
            bundle=[input[k] for k in bundle_keys],
            key=check_key,
            list=input[list_key],
        ) as worklist:
            items = [item for (item, _) in worklist] # ignore the callback
    except DuplicateBundleAttempt:
        items = []
    except:
        raise MonitorFailException()

    print items

    if downsize is not None:
        chunks_fun = lambda l, n:[l[x: x+n] for x in xrange(0, len(l), n)]
        chunks = chunks_fun(items, downsize)
    else:
        chunks = [items]

    for chunk in chunks:
        if len(chunk) > 0:
            r_rq = StrictRedis.from_url(REDIS_URL_RQ)
            jq = Queue(job_queue, connection=r_rq)
            monq = Queue(monitor_queue, connection=r_rq)
            job_input = {bundle_key: input[bundle_key] for bundle_key in bundle_keys}
            job_input.update({
                list_key: chunk
            })
            doQ(
                jq,
                job_name,
                json.dumps(job_input),
                timeout=timeout,
                print_only=not commit,
            )
            mon_input = {}
            mon_input.update(input)
            mon_input.update({
                list_key: chunk
                'monitor_queue': monitor_queue + '_'
            })
            doQ(
                monq,
                monitor_name,
                json.dumps(mon_input),
                timeout=get_current_job().timeout,
                print_only=not commit,
            )

    return "Success ({0} / {1})".format(len(items), len(input[list_key]))

