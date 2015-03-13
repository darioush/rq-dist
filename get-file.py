#! /usr/bin/env python

import socket
import boto.s3
import sys

from plumbum import LocalPath

from cvgmeasure.conf import get_property

def mkdir_p(dst):
    LocalPath(LocalPath(dst).dirname).mkdir()

def main(b_fn, dst):
    hostname, _, _ = socket.gethostname().partition('.')
    bucket, _, fn = b_fn.partition('/')
    look_dirs = get_property('s3_cache', hostname)
    cache_dir = look_dirs[-1:]

    for d in look_dirs:
        path = (LocalPath(d) / bucket / fn)
        if path.exists():
            mkdir_p(dst)
            path.copy(dst)
            break
    else:
        s3 = boto.s3.connect_to_region('us-west-2')
        b = s3.lookup(bucket)
        key = b.lookup(fn)
        if key is None:
            print "No such file on AWS"
            sys.exit(2)
        mkdir_p(dst)
        with open(dst, 'w') as out_f:
            out_f.write(key.read())

        for d in cache_dir:
            path = (LocalPath(d) / bucket / fn)
            if not path.exists():
                mkdir_p(path)
                LocalPath(dst).copy(path)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

