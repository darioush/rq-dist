#! /bin/sh

set -e

source env/bin/activate
nohup rqworker high default low >/dev/null 2>&1 &
