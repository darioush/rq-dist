#! /usr/bin/env python
import sys
import json

def main(gran, qm, args):
    for bases, pools in [
           # ('0', 'B.F'), 
            ('B', 'G'), ('0', 'G'), ('0', 'B.F.G'), ('0', 'B.G'), ('B', 'F.G')]:
        print "./main.py qb cvgmeasure.select.m {args} -j '{json}'".format(args=args, json=json.dumps(
            {'qm': qm, 'bases': bases, 'pools': pools, 'granularity': gran}))

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])

