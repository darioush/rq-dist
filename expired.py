#! /usr/bin/env python

import sys
from redis import StrictRedis
from cvgmeasure.conf import REDIS_URL_RQ
from rq.registry import StartedJobRegistry
from rq.utils import current_timestamp

r = StrictRedis.from_url(REDIS_URL_RQ)
sjr = StartedJobRegistry(sys.argv[1], connection=r)
#sjr.cleanup()
sjr.cleanup(timestamp=current_timestamp()+100000000000)
