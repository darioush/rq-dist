#! /bin/bash

set -e

commit=$1

python requeue.py -g DuplicateBundleAttempt > jobfiles/dup
wc -l jobfiles/dup
python requeue.py -J jobfiles/dup -q dummy $commit > /dev/null
