#! /usr/bin/env python
import redis
import re
import json

from rq import get_failed_queue
from cvgmeasure.conf import REDIS_URL_RQ, get_property_defaults
from cvgmeasure.common import connect_to_redis, mk_key

r = redis.StrictRedis.from_url(REDIS_URL_RQ)
fq = get_failed_queue(connection=r)

with connect_to_redis(get_property_defaults('redis_url')) as r2:
    for job in fq.get_jobs():
        exc_info = '' if job.exc_info is None else job.exc_info
        reason = exc_info.split('\n')[-2:-1]
        for r in reason:
            if re.match(r"""NameError: global name 'bucket_name' is not defined""", r):
                input = json.loads(job.args[0])
                project, version, cvg_tool, suite = map(lambda x: input[x], 'project version cvg_tool suite'.split(' '))
                key = mk_key('fetch-result', [project, version, suite])
                if r2.get(key) in ['empty', 'missing']:
                    print job.id
                if project == 'Time' and version in [28, 29, 30, 31, 32]:
                    print job.id

