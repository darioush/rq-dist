import redis
import sys

from cvgmeasure.conf import get_property

def dl(key, hashkey, out):
    r = redis.StrictRedis.from_url(get_property('redis_url'))
    with open(out, 'w') as f:
        f.write(r.hget(key, hashkey))


if __name__ == "__main__":
    dl(sys.argv[1], sys.argv[2], sys.argv[3])

