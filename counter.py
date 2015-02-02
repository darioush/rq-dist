
import redis

from cvgmeasure.conf import get_property
from cvgmeasure.common import PROJECTS, get_num_bugs

def main():
    r = redis.StrictRedis.from_url(get_property('redis_url'))
    sum = 0
    cvr = 0
    for p in PROJECTS:
        for i in xrange(get_num_bugs(p)):
            b = i + 1
            cvred = r.hlen('results:test-classes-cvg-nonempty:jmockit:%s:%d' % (p,i))
            if cvred > 0:
                sum += r.llen('results:test-classes:%s:%d' % (p,i))
                cvr += cvred

    print cvr
    print sum

if __name__ == "__main__":
    main()

