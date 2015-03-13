#! /bin/sh

set -e

if [ -z "$1" ]; then
    queues="high default low";
else
    queues="$1";
fi

source env/bin/activate
nohup rqworker --host nest.cs.washington.edu $queues >/dev/null 2>&1 &
