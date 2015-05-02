#! /usr/bin/env python

from redis import StrictRedis
import json

from cvgmeasure.d4 import iter_versions, is_empty
from cvgmeasure.conf import REDIS_URL_XX
from cvgmeasure.common import i_tn_s

def main(r):
    for p, v in iter_versions():
        for tool in ['cobertura', 'codecover', 'jmockit', 'major']:
            m = r.hgetall('exec:{t}:{p}:{v}:dev'.format(p=p, v=v, t=tool))
            keys = m.keys()
            tn_dict = {ti: tn for (ti, tn) in zip(keys, i_tn_s(r, keys, 'dev'))}
            mm = {ti: 1 for ti, result in m.iteritems() if (not is_empty(tool, json.loads(result)))
                    and tn_dict[ti].find('::') != -1
                }
            r.delete('nonempty:{t}:{p}:{v}:dev'.format(p=p, v=v, t=tool), mm)
            print p, v, tool, len(mm)
            if mm:
                r.hmset('nonempty:{t}:{p}:{v}:dev'.format(p=p, v=v, t=tool), mm)

if __name__ == "__main__":
        r = StrictRedis.from_url(REDIS_URL_XX)
        main(r)



