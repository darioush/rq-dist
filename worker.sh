#! /bin/sh

set -e

source env/bin/activate
nohup rqworker >/dev/null 2>&1 &
