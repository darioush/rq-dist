#! /usr/bin/env python

import sys
import json
import os

from plumbum import local
from optparse import OptionParser

from cvgmeasure.d4 import iter_versions, is_empty, get_modified_sources, refresh_dir, checkout
from cvgmeasure.tgs import JAR_PATH



def main():
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append")
    parser.add_option("-v", "--version", dest="restrict_version", action="append")

    (options, args) = parser.parse_args(sys.argv)
    java = local['java']['-cp', JAR_PATH, 'edu.washington.cs.tgs.MapBuilder']
    for p, v in iter_versions(options.restrict_project, options.restrict_version):
        print p,v 
        src_dir, f_list = get_modified_sources(p, v)
        work_dir_path = '/tmp/work.{pid}'.format(pid=os.getpid())
        with refresh_dir(work_dir_path, cleanup=True):
            with checkout(p, v, work_dir_path):
                with local.cwd(src_dir):
                    (java > '/tmp/results/{p}:{v}'.format(p=p, v=v))(*f_list)


if __name__ == "__main__":
    main()



