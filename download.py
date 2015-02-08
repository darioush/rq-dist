import redis
import sys

from cvgmeasure.conf import get_property

i = 0
def dl(r, key, hashkey, out):
    with open(out, 'w') as f:
        f.write(r.hget(key, hashkey))


def dlkey(r, key):
    global i
    _, _, tool, project, v = key.split(':')
    DIR = '/scratch/darioush/files/'
    for k in r.hkeys(key):
        fn = "%s:%s:%s:%s.tar.gz" % (tool, project, v, k)
        print i, DIR + fn
        i += 1
        dl(r, key, k, DIR + fn)

def keys(r):
    #for key in r.keys("results:test-classes-cvg-files:*"):
    for key in r.keys("results:test-methods-run-cvg-files:*"):
        try:
            dlkey(r, key)
            r.delete(key)
        except:
            raise


if __name__ == "__main__":
    r = redis.StrictRedis.from_url(get_property('redis_url'))
    keys(r)
    #dl(sys.argv[1], sys.argv[2], sys.argv[3])

