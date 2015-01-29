from __future__ import absolute_import

import time
import json

from rq import Queue
from redis import StrictRedis

#from cvgmeasure.common import work_function


def main():
    redis = StrictRedis()
    q = Queue('low', connection=redis, default_timeout=5)
    job = q.enqueue_call(
        func='cvgmeasure.common.work_function',
        args=(json.dumps({'info': 'yolo'}),),
        result_ttl=-1,
        timeout=10,
        description="This job"
    )

    print job.result

    time.sleep(2)
    print job.result



