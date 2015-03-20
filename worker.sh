#! /bin/sh

set -e

if [ -z "$1" ]; then
    queues="high default low";
else
    queues="$1";
fi

source env/bin/activate
nohup rqworker --url redis://nest.cs.washington.edu:6379/0 $queues >/dev/null 2>&1 &
