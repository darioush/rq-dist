#! /usr/bin/env python
import msgpack
import sys

from redis import StrictRedis

from cvgmeasure.conf import get_property
from cvgmeasure.common import i_tn_s, mk_key

def main(r, project, v, suite):
    _key = mk_key('time', [project, v, suite])
    timings = {k: msgpack.unpackb(val) for k, val in r.hgetall(_key).iteritems()}
    the_keys = list(timings.keys())
    key_map =  dict(zip(the_keys, i_tn_s(r, the_keys, suite)))
    for key in the_keys:
        print '{tn}: [{l}'.format(tn=key_map[key], l=', '.join(map(str, timings[key])))

if __name__ == "__main__":
    r = StrictRedis.from_url(get_property('redis_url'))
    main(r, sys.argv[1], sys.argv[2], sys.argv[3])
