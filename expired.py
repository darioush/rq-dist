#! /usr/bin/env python

import sys
from redis import StrictRedis
from cvgmeasure.conf import REDIS_URL_RQ
from rq.registry import StartedJobRegistry

r = StrictRedis.from_url(REDIS_URL_RQ)
sjr = StartedJobRegistry(sys.argv[1], connection=r)
sjr.cleanup()
