#! /bin/sh

set -e

source env/bin/activate
nohup rqworker &
