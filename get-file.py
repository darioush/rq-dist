#! /usr/bin/env python

import sys

from cvgmeasure.s3 import get_file_from_cache_or_s3, NoFileOnS3

def main(b_fn, dst):
    bucket, _, fn = b_fn.partition('/')
    try:
        get_file_from_cache_or_s3(bucket, fn, dst)
    except NoFileOnS3:
        print "No such file on S3"
        sys.exit(2)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

