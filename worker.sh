#! /bin/sh

set -e

source env/bin/activate
nohup rqworker --host monarch.cs.washington.edu high default low >/dev/null 2>&1 &
